use dirs_sys::home_dir;
use futures::stream::{self, Stream, StreamExt};
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Code, Request, Response, Status};

use crate::archive::IndexRepo;
use crate::common::now_time_t;
use crate::openraildata_pb::{td_feed_server, TdQuery};
use crate::preserve;
use crate::recent::RecentDatabase;

const MAX_QUERY_TIME: i64 = 86400 * 20;

struct TDArchiveFeed {
    repo: Arc<IndexRepo>,
    recent: Arc<RecentDatabase>,
}

impl TDArchiveFeed {
    fn new(repo: Arc<IndexRepo>, recent: Arc<RecentDatabase>) -> Self {
        Self { repo, recent }
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

        let now = now_time_t();
        let today = now - (now % 86400);
        let yesterday = today - 86400;
        // The archive will definitely not be already built for today, and might not be built
        // for yesterday, so query using the recent feed for those.

        let mut streams = Vec::<Self::FeedStream>::new();

        if start_ts < yesterday {
            let day = start_ts - (start_ts % 86400);
            let r_end_ts = if end_ts > yesterday {
                yesterday
            } else {
                end_ts
            };
            streams.push(Box::pin(self.repo.clone().feed(q.clone(), day, r_end_ts)));
        }
        if end_ts >= yesterday {
            let mut rq = q.clone();
            if start_ts < yesterday {
                let ts = rq.from_timestamp.as_mut().unwrap();
                ts.seconds = yesterday;
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
    let socket_path = dir.join("tdfeed/sock");
    let repo = Arc::new(IndexRepo::new(dir.join("tdfeed")));
    let recent = Arc::new(RecentDatabase::new());
    let tdfeed = TDArchiveFeed::new(repo.clone(), recent.clone());
    let listener = match UnixListener::bind(socket_path.clone()) {
        Ok(l) => Ok(l),
        Err(listen_err) if listen_err.kind() == ErrorKind::AddrInUse => {
            // Check if this is a stale socket.
            match tokio::net::UnixStream::connect(socket_path.clone()).await {
                Err(e) if e.kind() == ErrorKind::ConnectionRefused => {
                    log::warn!("Cleaning up stale socket {}", socket_path.to_string_lossy());
                    std::fs::remove_file(socket_path.clone())?;
                    // Try again.
                    UnixListener::bind(socket_path.clone())
                }
                _ => Err(listen_err),
            }
        }
        Err(e) => Err(e),
    }?;
    log::info!("Listening on {}", socket_path.to_string_lossy());
    let listener_stream = UnixListenerStream::new(listener);
    repo.start();
    recent.start(&dir);
    Server::builder()
        .add_service(td_feed_server::TdFeedServer::new(tdfeed))
        .serve_with_incoming(listener_stream)
        .await?;
    Ok(())
}
