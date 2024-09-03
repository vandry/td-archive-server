use chrono::{TimeZone, Utc};
use futures::stream::{self, Stream, StreamExt};
use s3::creds::Credentials;
use s3::Bucket;
use s3::Region;
use std::env;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tonic::{transport::Server, Code, Request, Response, Status};

use crate::archive::IndexRepo;
use crate::common::{archive_filenames, now_time_t};
use crate::openraildata_pb::{td_feed_server, TdQuery};
use crate::preserve;
use crate::recent::RecentDatabase;

const MAX_QUERY_TIME: i64 = 86400 * 20;

struct TDArchiveFeed {
    repo: Arc<IndexRepo>,
    recent: Arc<RecentDatabase>,
    boundary_time: Arc<AtomicI64>,
}

async fn day_built(bucket: &Bucket, day: i64) -> bool {
    let (dname, iname) = archive_filenames(day);
    let (dres, ires) = tokio::join!(bucket.head_object(dname), bucket.head_object(iname));
    dres.is_ok() && ires.is_ok()
}

impl TDArchiveFeed {
    fn new(repo: Arc<IndexRepo>, recent: Arc<RecentDatabase>) -> Self {
        Self {
            repo,
            recent,
            boundary_time: Arc::new(AtomicI64::new(0)),
        }
    }

    pub async fn scan_boundary(&self, bucket: Arc<Bucket>) {
        let now = now_time_t();
        let today = now - (now % 86400);
        // The index should definitely not already be built for today,
        // so start with yesterday.
        let mut boundary = today - 86400;
        if day_built(&bucket, boundary).await {
            // Yesterday's index exists, we can move on to today.
            boundary += 86400;
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
                sleep(Duration::from_millis(300000)).await;
                if day_built(&bucket, boundary).await {
                    boundary += 86400;
                    published_boundary.store(boundary, Ordering::Release);
                    recent.set_boundary(boundary);
                    let ymd = Utc.timestamp_opt(boundary, 0).unwrap().format("%Y-%m-%d");
                    log::info!("New boundary: Queries for data before {}T00:00:00Z will use archive, after will use recent", ymd);
                }
            }
        });
    }
}

#[tonic::async_trait]
impl td_feed_server::TdFeed for TDArchiveFeed {
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

        let boundary = self.boundary_time.load(Ordering::Acquire);
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

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().collect();
    if args.len() != 6 {
        eprintln!(
            "Usage: {} endpoint region bucket live-feed serving-port",
            args[0].to_string_lossy()
        );
        std::process::exit(3);
    }
    let s3_endpoint = args[1].to_string_lossy().into_owned();
    let s3_region_name = args[2].to_string_lossy().into_owned();
    let bucket_name = args[3].to_string_lossy().into_owned();
    let live_feed_address = args[4].to_string_lossy().into_owned();
    let serving_address = args[5].to_string_lossy().parse().unwrap();

    env_logger::init();

    let s3_cred = Credentials::default().unwrap();
    let s3_region = Region::Custom {
        region: s3_region_name,
        endpoint: s3_endpoint,
    };
    let bucket: Arc<Bucket> = Arc::from(Bucket::new(&bucket_name, s3_region, s3_cred).unwrap());

    let repo = Arc::new(IndexRepo::new(bucket.clone()));
    let recent = Arc::new(RecentDatabase::new());
    let tdfeed = TDArchiveFeed::new(repo.clone(), recent.clone());
    log::info!("Listening on {}", serving_address);
    repo.start();
    tdfeed.scan_boundary(bucket.clone()).await;
    recent.start(&live_feed_address);
    Server::builder()
        .add_service(td_feed_server::TdFeedServer::new(tdfeed))
        .serve(serving_address)
        .await?;
    Ok(())
}
