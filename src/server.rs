use chrono::{TimeZone, Utc};
use comprehensive::health::HealthReporter;
use comprehensive::v1::{AssemblyRuntime, Resource, resource};
use comprehensive::{NoArgs, ResourceDependencies};
use comprehensive_grpc::GrpcClient;
use futures::stream::{self, Stream, StreamExt};
use s3::error::S3Error;
use s3::Bucket;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tonic::{Code, Request, Response, Status};
use tracing::{error, info};

use crate::TDArchiveBucket;
use crate::archive::IndexRepo;
use crate::common::{archive_filenames, now_time_t};
use crate::openraildata_pb::{td_feed_client, td_feed_server, TdQuery};
use crate::preserve;
use crate::recent::RecentDatabase;

const MAX_QUERY_TIME: i64 = 86400 * 20;

struct TDArchiveFeed {
    recent: Arc<RecentDatabase>,
    boundary_time: AtomicI64,
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
            boundary_time: AtomicI64::new(0),
        }
    }

    pub async fn scan_boundary<T: AsRef<Bucket> + Send + Sync + 'static>(
        self: &Arc<Self>,
        bucket: Arc<T>,
    ) {
        let now = now_time_t();
        let today = now - (now % 86400);
        // The index should definitely not already be built for today,
        // so start with yesterday.
        let mut boundary = today - 86400;
        match day_built(bucket.as_ref().as_ref(), boundary).await {
            Ok(true) => {
                // Yesterday's index exists, we can move on to today.
                boundary += 86400;
            }
            Ok(false) => (),
            Err(e) => {
                error!("Error querying bucket: {}; will try again", e);
            }
        }
        self.boundary_time.store(boundary, Ordering::Release);
        self.recent.set_boundary(boundary);
        let ymd = Utc.timestamp_opt(boundary, 0).unwrap().format("%Y-%m-%d");
        info!(
            "Queries for data before {}T00:00:00Z will use archive, after will use recent",
            ymd
        );
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(300000)).await;
                match day_built(bucket.as_ref().as_ref(), boundary).await {
                    Ok(true) => {
                        boundary += 86400;
                        this.boundary_time.store(boundary, Ordering::Release);
                        this.recent.set_boundary(boundary);
                        let ymd = Utc.timestamp_opt(boundary, 0).unwrap().format("%Y-%m-%d");
                        info!("New boundary: Queries for data before {}T00:00:00Z will use archive, after will use recent", ymd);
                    }
                    Ok(false) => (),
                    Err(e) => {
                        error!("Error querying bucket: {}; will try again", e);
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
            streams.push(Box::pin(self.tdfeed.recent.clone().feed(rq)));
        }
        let output_stream = stream::iter(streams).flatten();
        Ok(Response::new(Box::pin(output_stream) as Self::FeedStream))
    }
}

#[derive(GrpcClient)]
struct LiveFeed(
    td_feed_client::TdFeedClient<comprehensive_grpc::client::Channel>,
    comprehensive_grpc::client::ClientWorker,
);

pub struct TDArchiveFeedResource {
    repo: Arc<IndexRepo>,
    tdfeed: Arc<TDArchiveFeed>,
}

#[derive(ResourceDependencies)]
pub struct TDArchiveFeedResourceDependencies {
    health_reporter: Arc<HealthReporter>,
    bucket: Arc<TDArchiveBucket>,
    repo: Arc<IndexRepo>,
    live_feed: Arc<LiveFeed>,
}

#[resource]
#[export_grpc(td_feed_server::TdFeedServer)]
#[proto_descriptor(crate::openraildata_pb::FILE_DESCRIPTOR_SET)]
impl Resource for TDArchiveFeedResource {
    fn new(
        d: TDArchiveFeedResourceDependencies,
        _: NoArgs,
        api: &mut AssemblyRuntime<'_>,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let recent = Arc::new(RecentDatabase::new());
        let tdfeed = Arc::new(TDArchiveFeed::new(recent));
        let signaller = d.health_reporter.register("live")?;
        let tdfeed2 = Arc::clone(&tdfeed);
        api.set_task(async move {
            tdfeed2.scan_boundary(d.bucket).await;
            Arc::clone(&tdfeed2.recent).start(d.live_feed.client(), signaller);
            Ok(())
        });
        Ok(Arc::new(Self {
            repo: d.repo,
            tdfeed,
        }))
    }
}
