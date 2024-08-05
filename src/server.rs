use chrono::{TimeZone, Utc};
use dirs_sys::home_dir;
use futures::stream::{self, Stream, StreamExt};
use std::io::ErrorKind;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio::time::sleep;
use tokio_stream::wrappers::UnixListenerStream;
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

impl TDArchiveFeed {
    fn new(repo: Arc<IndexRepo>, recent: Arc<RecentDatabase>) -> Self {
        Self {
            repo,
            recent,
            boundary_time: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn scan_boundary(&self, db_path: &Path) {
        let now = now_time_t();
        let today = now - (now % 86400);
        // The index should definitely not already be built for today,
        // so start with yesterday.
        let mut boundary = today - 86400;
        let (dname, iname) = archive_filenames(db_path, boundary);
        if dname.is_file() && iname.is_file() {
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
        let db_path = db_path.to_owned();
        let published_boundary = Arc::clone(&self.boundary_time);
        let recent = Arc::clone(&self.recent);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(300000)).await;
                let (dname, iname) = archive_filenames(db_path.as_ref(), boundary);
                if dname.is_file() && iname.is_file() {
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
    env_logger::init();
    let dir = home_dir().expect("$HOME").join("var");
    let serving_socket_path = dir.join("tdfeed/sock");
    let upstream_socket_path = dir.join("collect_td_feed.sock");
    let db_path = dir.join("tdfeed");
    let repo = Arc::new(IndexRepo::new(db_path.as_ref()));
    let recent = Arc::new(RecentDatabase::new());
    let tdfeed = TDArchiveFeed::new(repo.clone(), recent.clone());
    let listener = match UnixListener::bind(serving_socket_path.clone()) {
        Ok(l) => Ok(l),
        Err(listen_err) if listen_err.kind() == ErrorKind::AddrInUse => {
            // Check if this is a stale socket.
            match tokio::net::UnixStream::connect(serving_socket_path.clone()).await {
                Err(e) if e.kind() == ErrorKind::ConnectionRefused => {
                    log::warn!(
                        "Cleaning up stale socket {}",
                        serving_socket_path.to_string_lossy()
                    );
                    std::fs::remove_file(serving_socket_path.clone())?;
                    // Try again.
                    UnixListener::bind(serving_socket_path.clone())
                }
                _ => Err(listen_err),
            }
        }
        Err(e) => Err(e),
    }?;
    log::info!("Listening on {}", serving_socket_path.to_string_lossy());
    let listener_stream = UnixListenerStream::new(listener);
    repo.start();
    tdfeed.scan_boundary(&db_path);
    recent.start(&upstream_socket_path);
    Server::builder()
        .add_service(td_feed_server::TdFeedServer::new(tdfeed))
        .serve_with_incoming(listener_stream)
        .await?;
    Ok(())
}
