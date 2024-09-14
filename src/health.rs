use std::sync::Arc;
use tokio::sync::Mutex;
use tonic_health::{server::HealthReporter, ServingStatus};

const LIVENESS: &str = "liveness";
const READINESS: &str = "readiness";

struct HealthTrackerInner {
    reporter: HealthReporter,
    bucket_healthy: bool,
    feed_healthy: bool,
}

impl HealthTrackerInner {
    async fn update(&mut self) {
        let status = if self.bucket_healthy && self.feed_healthy {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        self.reporter.set_service_status(READINESS, status).await;
    }
}

#[derive(Clone)]
pub struct HealthTracker {
    inner: Arc<Mutex<HealthTrackerInner>>,
}

impl HealthTracker {
    pub async fn new(mut reporter: HealthReporter) -> Self {
        reporter
            .set_service_status(LIVENESS, ServingStatus::Serving)
            .await;
        reporter
            .set_service_status(READINESS, ServingStatus::NotServing)
            .await;
        Self {
            inner: Arc::new(Mutex::new(HealthTrackerInner {
                reporter,
                bucket_healthy: false,
                feed_healthy: false,
            })),
        }
    }

    pub async fn healthy_bucket(self) {
        log::info!("Bucket is healthy.");
        let mut inner = self.inner.lock().await;
        inner.bucket_healthy = true;
        inner.update().await;
    }

    pub async fn healthy_live_feed(&self, is_healthy: bool) {
        log::info!(
            "Live feed is {}healthy.",
            if is_healthy { "" } else { "un" }
        );
        let mut inner = self.inner.lock().await;
        inner.feed_healthy = is_healthy;
        inner.update().await;
    }
}
