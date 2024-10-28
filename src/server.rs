use atomic_take::AtomicTake;
use chrono::{TimeZone, Utc};
use comprehensive::health::{HealthReporter, HealthSignaller};
use comprehensive::{Resource, ResourceDependencies};
use futures::stream::{self, Stream, StreamExt};
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::Bucket;
use s3::Region;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Uri;
use tonic::{Code, Request, Response, Status};

use crate::archive::IndexRepo;
use crate::common::{archive_filenames, now_time_t};
use crate::openraildata_pb::{td_feed_server, TdQuery};
use crate::preserve;
use crate::recent::RecentDatabase;

const MAX_QUERY_TIME: i64 = 86400 * 20;

struct TDArchiveFeed {
    recent: Arc<RecentDatabase>,
    boundary_time: Arc<AtomicI64>,
}

fn map_exists<T>(r: Result<T, S3Error>) -> Result<bool, S3Error> {
    r.map_or_else(
        |e| match e {
            S3Error::HttpFailWithBody(code, _) if code == 404 => Ok(false),
            _ => Err(e),
        },
        |_| Ok(true),
    )
}

async fn day_built(bucket: &Bucket, day: i64) -> Result<bool, S3Error> {
    let (dname, iname) = archive_filenames(day);
    let (dres, ires) = tokio::join!(bucket.head_object(dname), bucket.head_object(iname));
    let dres = map_exists(dres)?;
    let ires = map_exists(ires)?;
    Ok(dres && ires)
}

impl TDArchiveFeed {
    fn new(recent: Arc<RecentDatabase>) -> Self {
        Self {
            recent,
            boundary_time: Arc::new(AtomicI64::new(0)),
        }
    }

    pub async fn scan_boundary(&self, bucket: Arc<Bucket>, ht: HealthSignaller) {
        let mut ht = Some(ht);
        let now = now_time_t();
        let today = now - (now % 86400);
        // The index should definitely not already be built for today,
        // so start with yesterday.
        let mut boundary = today - 86400;
        match day_built(&bucket, boundary).await {
            Ok(true) => {
                // Yesterday's index exists, we can move on to today.
                boundary += 86400;
                if let Some(t) = ht.take() {
                    t.set_healthy(true);
                }
            }
            Ok(false) => {
                if let Some(t) = ht.take() {
                    t.set_healthy(true);
                }
            }
            Err(e) => {
                log::error!("Error querying bucket: {}; will try again", e);
            }
        }
        self.boundary_time.store(boundary, Ordering::Release);
        self.recent.set_boundary(boundary);
        let ymd = Utc.timestamp_opt(boundary, 0).unwrap().format("%Y-%m-%d");
        log::info!(
            "Queries for data before {}T00:00:00Z will use archive, after will use recent",
            ymd
        );
        let published_boundary = Arc::clone(&self.boundary_time);
        let recent = Arc::clone(&self.recent);
        tokio::spawn(async move {
            loop {
                let ms = if ht.is_some() { 5000 } else { 300000 };
                sleep(Duration::from_millis(ms)).await;
                match day_built(&bucket, boundary).await {
                    Ok(true) => {
                        if let Some(t) = ht.take() {
                            t.set_healthy(true);
                        }
                        boundary += 86400;
                        published_boundary.store(boundary, Ordering::Release);
                        recent.set_boundary(boundary);
                        let ymd = Utc.timestamp_opt(boundary, 0).unwrap().format("%Y-%m-%d");
                        log::info!("New boundary: Queries for data before {}T00:00:00Z will use archive, after will use recent", ymd);
                    }
                    Ok(false) => {
                        if let Some(t) = ht.take() {
                            t.set_healthy(true);
                        }
                    }
                    Err(e) => {
                        log::error!("Error querying bucket: {}; will try again", e);
                    }
                }
            }
        });
    }
}

#[tonic::async_trait]
impl td_feed_server::TdFeed for TDArchiveFeedResource {
    type FeedStream = Pin<Box<dyn Stream<Item = Result<preserve::TdFrame, Status>> + Send>>;

    async fn feed(&self, req: Request<TdQuery>) -> Result<Response<Self::FeedStream>, Status> {
        let q = req.into_inner();
        let start_ts = q
            .from_timestamp
            .as_ref()
            .ok_or_else(|| Status::new(Code::InvalidArgument, "from_timestamp is required"))?
            .seconds;
        let end_ts = q
            .to_timestamp
            .as_ref()
            .ok_or_else(|| Status::new(Code::InvalidArgument, "to_timestamp is required"))?
            .seconds;
        if end_ts < start_ts {
            return Err(Status::new(
                Code::InvalidArgument,
                "to_timestamp before from_timestamp",
            ));
        }
        if end_ts - start_ts > MAX_QUERY_TIME {
            return Err(Status::new(
                Code::ResourceExhausted,
                "querying more than the allowed size of time interval",
            ));
        }

        let tdfeed = &self.tdfeed;
        let boundary = tdfeed.boundary_time.load(Ordering::Acquire);
        let mut streams = Vec::<Self::FeedStream>::new();

        if start_ts < boundary {
            let day = start_ts - (start_ts % 86400);
            let r_end_ts = if end_ts > boundary { boundary } else { end_ts };
            streams.push(Box::pin(self.repo.clone().feed(q.clone(), day, r_end_ts)));
        }
        if end_ts >= boundary {
            let mut rq = q.clone();
            if start_ts < boundary {
                let ts = rq.from_timestamp.as_mut().unwrap();
                ts.seconds = boundary;
                ts.nanos = 0;
            }
            streams.push(Box::pin(self.recent.clone().feed(rq)));
        }
        let output_stream = stream::iter(streams).flatten();
        Ok(Response::new(Box::pin(output_stream) as Self::FeedStream))
    }
}

struct HealthSignallers {
    bucket: HealthSignaller,
    live: HealthSignaller,
}

pub struct TDArchiveFeedResource {
    bucket: Arc<Bucket>,
    repo: Arc<IndexRepo>,
    recent: Arc<RecentDatabase>,
    tdfeed: TDArchiveFeed,
    live_feed_address: Uri,
    signallers: AtomicTake<HealthSignallers>,
}

#[derive(clap::Args, Debug)]
pub struct Args {
    #[arg(long)]
    s3_endpoint: String,

    #[arg(long)]
    s3_region_name: String,

    #[arg(long)]
    bucket_name: String,

    #[arg(long)]
    live_feed_address: Uri,
}

#[derive(ResourceDependencies)]
pub struct TDArchiveFeedResourceDependencies(Arc<HealthReporter>);

impl Resource for TDArchiveFeedResource {
    type Args = Args;
    type Dependencies = TDArchiveFeedResourceDependencies;
    const NAME: &str = "TDArchiveFeed";

    fn new(
        d: TDArchiveFeedResourceDependencies,
        args: Args,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let s3_cred = Credentials::default().unwrap();
        let s3_region = Region::Custom {
            region: args.s3_region_name,
            endpoint: args.s3_endpoint,
        };
        let bucket: Arc<Bucket> =
            Arc::from(Bucket::new(&args.bucket_name, s3_region, s3_cred).unwrap());

        let repo = Arc::new(IndexRepo::new(bucket.clone()));
        let recent = Arc::new(RecentDatabase::new());
        let tdfeed = TDArchiveFeed::new(recent.clone());

        Ok(Self {
            repo,
            recent,
            tdfeed,
            live_feed_address: args.live_feed_address,
            bucket,
            signallers: AtomicTake::new(HealthSignallers {
                bucket: d.0.register("bucket")?,
                live: d.0.register("live")?,
            }),
        })
    }

    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let signallers = self.signallers.take().unwrap();
        Arc::clone(&self.repo).start();
        self.tdfeed
            .scan_boundary(self.bucket.clone(), signallers.bucket)
            .await;
        Arc::clone(&self.recent).start(self.live_feed_address.clone(), signallers.live);
        Ok(())
    }
}

#[derive(comprehensive_grpc::GrpcService)]
#[implementation(TDArchiveFeedResource)]
#[service(td_feed_server::TdFeedServer)]
#[descriptor(crate::openraildata_pb::FILE_DESCRIPTOR_SET)]
pub struct TDArchiveFeedGrpcService;
