use async_stream::stream;
use futures::Stream;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tonic::{Request, Status};

use crate::common::{intersect, now_time_t, query_matches, union};
use crate::health::HealthTracker;
use crate::openraildata_pb::{td_feed_client, TdQuery};
use crate::preserve;

// 3 days of 10-minute (6 per hour) buckets
const NBUCKETS: usize = 6 * 24 * 3;
const ROTATE_PERIOD: Duration = Duration::new(600, 0);
const MAX_KEEP_TIME: i64 = 86400 * 2;

fn mark(m: &mut HashMap<String, Vec<u8>>, k: &str, veclen: usize, bi: usize, bit: u8) {
    let v: &mut Vec<u8> = match m.get_mut(k) {
        Some(v) => v,
        None => {
            let v = vec![0; veclen];
            m.entry(k.to_string()).or_insert(v)
        }
    };
    v[bi] |= bit;
}

struct BucketIndex {
    area_ids: HashMap<String, Vec<u8>>,
    descriptions: HashMap<String, Vec<u8>>,
}

impl BucketIndex {
    fn build(&mut self, d: &Bucket) {
        let mut veclen = d.v.len() / 8;
        if veclen * 8 < d.v.len() {
            veclen += 1;
        }
        for (i, frame) in d.v.iter().enumerate() {
            let bi = i / 8;
            let bit = 1 << (i - bi * 8);
            if let Some(ref key) = frame.area_id {
                mark(&mut self.area_ids, key, veclen, bi, bit);
            }
            if let Some(ref key) = frame.description {
                mark(&mut self.descriptions, key, veclen, bi, bit);
            }
        }
    }
}

struct BucketIterator<'a> {
    q: &'a TdQuery,
    v: &'a Vec<preserve::TdFrame>,
    matches: Vec<u8>,
    pos: usize,
}

impl<'a> BucketIterator<'a> {
    fn new(q: &'a TdQuery, v: &'a Vec<preserve::TdFrame>, matches: Vec<u8>) -> Self {
        Self {
            q,
            v,
            matches,
            pos: 0,
        }
    }
}

impl<'a> Iterator for BucketIterator<'a> {
    type Item = &'a preserve::TdFrame;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.v.len() {
            let pos = self.pos;
            let byte_i = pos >> 3;
            let bit = 1 << (pos & 7);
            self.pos += 1;
            if self.matches[byte_i] & bit == bit {
                let f = self.v.get(pos).unwrap();
                if query_matches(self.q, f) {
                    return Some(f);
                }
            }
        }
        None
    }
}

struct Bucket {
    v: Vec<preserve::TdFrame>,
    t0: i64,
    t1: i64,
    has_index: AtomicBool,
    index: UnsafeCell<BucketIndex>,
}

unsafe impl Sync for Bucket {}

impl Bucket {
    fn new() -> Self {
        Self {
            v: Vec::new(),
            t0: 0,
            t1: 0,
            has_index: AtomicBool::new(false),
            index: UnsafeCell::new(BucketIndex {
                area_ids: HashMap::new(),
                descriptions: HashMap::new(),
            }),
        }
    }

    fn within_range(&self, q: &TdQuery) -> bool {
        if let Some(ref fts) = q.from_timestamp {
            if fts.seconds > self.t1 {
                return false;
            }
        }
        if let Some(ref tts) = q.to_timestamp {
            if tts.seconds < self.t0 {
                return false;
            }
        }
        true
    }

    fn search<'a>(&'a self, q: &'a TdQuery) -> BucketIterator<'a> {
        let veclen = self.v.len() / 8 + if (self.v.len()) & 7 == 0 { 0 } else { 1 };
        let mut matches = Vec::<u8>::with_capacity(veclen);
        matches.resize(veclen, 255);

        if self.has_index.load(Ordering::Acquire) {
            let idx = unsafe {
                // Safety: has_index promises that the index is built, the index
                // is never mutated after it is built, and has_index will not turn
                // false until the bucket is deleted, which happens while holding
                // the write lock on Bucket but then we would not have &self.
                &*self.index.get()
            };

            if !q.area_id.is_empty() {
                let mut area_id_matches = vec![0_u8; veclen];
                for area_id in q.area_id.iter() {
                    if let Some(v) = idx.area_ids.get(area_id) {
                        union(&mut area_id_matches, v);
                    }
                }
                intersect(&mut matches, &area_id_matches);
            }

            if !q.description.is_empty() {
                let mut description_matches = vec![0_u8; veclen];
                for description in q.description.iter() {
                    if let Some(v) = idx.descriptions.get(description) {
                        union(&mut description_matches, v);
                    }
                }
                intersect(&mut matches, &description_matches);
            }
        }

        return BucketIterator::new(q, &self.v, matches);
    }
}

struct FreshBucketData {
    v: boxcar::Vec<preserve::TdFrame>, // append-only concurrent Vec
    t0: AtomicI64,
    t1: AtomicI64,
}

impl FreshBucketData {
    fn within_range(&self, q: &TdQuery) -> bool {
        if let Some(ref fts) = q.from_timestamp {
            if fts.seconds > self.t1.load(Ordering::Acquire) {
                return false;
            }
        }
        if let Some(ref tts) = q.to_timestamp {
            if tts.seconds < self.t0.load(Ordering::Acquire) {
                return false;
            }
        }
        true
    }

    fn search<'a>(&'a self, q: &'a TdQuery) -> impl Iterator<Item = &'a preserve::TdFrame> + 'a {
        self.v
            .iter()
            .filter_map(|(_, f)| if query_matches(q, f) { Some(f) } else { None })
    }
}

pub struct RecentDatabase {
    // The freshest data, not yet in the ring buffer
    fresh: RwLock<FreshBucketData>,

    // Ring buffer
    delete_i: AtomicUsize, // Oldest still filled bucket.
    oldest_i: AtomicUsize, // Oldest unexpired bucket. Start querying here.
    newest_i: AtomicUsize, // Newest bucket. End querying here.
    b: [RwLock<Bucket>; NBUCKETS],

    // Both fresh and the ring buffer
    count: AtomicUsize,

    // The erliest time for which we will receive queries.
    // Any earlier will go to the archive.
    start_time: AtomicI64,
}

async fn get_live(
    socket_path: String,
    feed: &RecentDatabase,
    ht: &mut HealthTracker,
    got_at_least_one: &mut bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = td_feed_client::TdFeedClient::connect(socket_path).await?;
    let mut stream = client
        .feed(Request::new(TdQuery::default()))
        .await?
        .into_inner();
    while let Some(frame) = stream.next().await {
        if !*got_at_least_one {
            *got_at_least_one = true;
            ht.healthy_live_feed(true).await;
        }
        feed.submit(frame?).await;
    }
    Ok(())
}

impl RecentDatabase {
    pub fn new() -> Self {
        Self {
            fresh: RwLock::new(FreshBucketData {
                v: boxcar::Vec::new(),
                t0: AtomicI64::new(0),
                t1: AtomicI64::new(0),
            }),
            delete_i: AtomicUsize::new(0),
            oldest_i: AtomicUsize::new(0),
            newest_i: AtomicUsize::new(0),
            b: array_init::array_init(|_| RwLock::new(Bucket::new())),
            count: AtomicUsize::new(0),
            start_time: AtomicI64::new(0),
        }
    }

    pub fn set_boundary(&self, boundary: i64) {
        self.start_time.store(boundary, Ordering::Release);
    }

    async fn submit(&self, frame: preserve::TdFrame) {
        let fresh = self.fresh.read().await;
        if let Some(ref ts) = frame.timestamp {
            let mut t1 = fresh.t1.load(Ordering::Acquire);
            while ts.seconds + 1 > t1 {
                t1 = fresh
                    .t1
                    .compare_exchange(t1, ts.seconds + 1, Ordering::SeqCst, Ordering::Acquire)
                    .unwrap_or_else(|v| v);
            }
            let mut t0 = fresh.t0.load(Ordering::Acquire);
            while ts.seconds < t0 || t0 == 0 {
                t0 = fresh
                    .t0
                    .compare_exchange(t0, ts.seconds, Ordering::SeqCst, Ordering::Acquire)
                    .unwrap_or_else(|v| v);
            }
        }
        fresh.v.push(frame);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    async fn expire(&self) {
        let mut expire_cutoff = self.start_time.load(Ordering::Acquire);
        let max_expire_cutoff: i64 = now_time_t() - MAX_KEEP_TIME;
        if expire_cutoff < max_expire_cutoff {
            log::warn!(
                "Expire cutoff {} is earlier than acceptable {}, capping to the latter",
                expire_cutoff,
                max_expire_cutoff
            );
            expire_cutoff = max_expire_cutoff;
        }
        loop {
            let i = self.oldest_i.load(Ordering::Acquire);
            if i == self.newest_i.load(Ordering::Acquire) {
                break; // underrun
            }
            let d = self.b[i].read().await;
            if d.t1 >= expire_cutoff {
                break;
            }
            // Tell everybody to stop looking at this bucket.
            self.oldest_i.store((i + 1) % NBUCKETS, Ordering::Release);
        }
    }

    async fn delete_expired(&self) {
        loop {
            let i = self.delete_i.load(Ordering::Acquire);
            if i == self.oldest_i.load(Ordering::Acquire) {
                break;
            }
            let mut b = self.b[i].write().await;
            self.count.fetch_sub(b.v.len(), Ordering::Relaxed);
            b.v = Vec::new();
            b.t0 = 0;
            b.t1 = 0;
            b.has_index.store(false, Ordering::Release);
            let idx = unsafe {
                // Safety: we have exclusive access to b.
                &mut *b.index.get()
            };
            idx.area_ids = HashMap::new();
            idx.descriptions = HashMap::new();
            self.delete_i.store((i + 1) % NBUCKETS, Ordering::Release);
        }
    }

    async fn rotate(&self) {
        // First check the head is non empty.
        if self.fresh.read().await.v.count() == 0 {
            // Skip rotating if we have nothing to rotate.
            // This is important so that build_index can sensibly populate t0 and t1.
            return;
        }
        // Get a new unused bucket.
        let new_i = (self.newest_i.load(Ordering::Acquire) + 1) % NBUCKETS;
        if self.delete_i.load(Ordering::Acquire) == new_i {
            log::error!("Ring buffer overflow: position {} not deleted yet", new_i);
            return;
        }
        let bb = &self.b[new_i];
        // Initial conversion without index.
        {
            // Pre-lock the new bucket so nobody queries it under we fill it.
            let mut b = bb.write().await;
            // Now make readers include it.
            self.newest_i.store(new_i, Ordering::Release);
            // Lock the fresh data. After this both readers and appenders are blocked!
            let mut fresh = self.fresh.write().await;
            b.v = std::mem::take(&mut fresh.v).into_iter().collect();
            b.t0 = fresh.t0.swap(0, Ordering::SeqCst);
            b.t1 = fresh.t1.swap(0, Ordering::SeqCst);
        }
        // Subsequently, while potential clients continue to query this bucket
        // without using an index, build the index.
        let b = bb.read().await;
        let idx = unsafe {
            // Safety: has_index is false and has been since b was write-locked
            // so nobody will access the index.
            &mut *b.index.get()
        };
        idx.build(&b);
        b.has_index.store(true, Ordering::Relaxed);
    }

    async fn report(&self) {
        let i0 = self.delete_i.load(Ordering::Relaxed);
        let i1 = self.newest_i.load(Ordering::Relaxed);
        let count = self.count.load(Ordering::Relaxed);
        let bucket_count = i1 + 1 + if i1 >= i0 { 0 } else { NBUCKETS } - i0;
        let fresh_count = self.fresh.read().await.v.count();
        log::info!(
            "Holding {} entries in {}/{} buckets of ring buffer plus {} fresh entries",
            count,
            bucket_count,
            NBUCKETS,
            fresh_count
        );
    }

    pub fn feed(
        self: Arc<Self>,
        q: TdQuery,
    ) -> impl Stream<Item = Result<preserve::TdFrame, Status>> {
        stream! {
            let mut i = self.oldest_i.load(Ordering::Acquire);
            let mut b = self.b[i].read().await;
            loop {
                let new_i = self.oldest_i.load(Ordering::Acquire);
                if i == new_i {
                    break;
                }
                // The oldest bucket has moved along. Try again.
                i = new_i;
                b = self.b[i].read().await;
            }
            // At this point, we hold a lock on the bucket so delete_expired will
            // not delete it from under us and we can prevent it from running ahead of
            // us by acquiring the next bucket before we drop this one.
            loop {
                if b.within_range(&q) {
                    for frame in b.search(&q) {
                        yield Ok(frame.clone());
                    }
                }
                if i == self.newest_i.load(Ordering::Acquire) {
                    // Now search the fresh data.
                    let fresh = self.fresh.read().await;
                    // It is possible that by the time we got the lock newest_i has advanced.
                    if i == self.newest_i.load(Ordering::Acquire) {
                        // We are still oky to move on to fresh data.
                        if fresh.within_range(&q) {
                            for frame in fresh.search(&q) {
                                yield Ok(frame.clone());
                            }
                        }
                        break;
                    }
                    // Otherwise keep looping.
                }
                i = (i + 1) % NBUCKETS;
                let new_b = self.b[i].read().await;  // acquire
                b = new_b;  // release
            }
        }
    }

    pub fn start(self: Arc<Self>, socket_path: &str, mut ht: HealthTracker) {
        let socket_path = socket_path.to_owned();
        let self_live_getter = self.clone();
        tokio::spawn(async move {
            loop {
                let mut got_at_least_one = false;
                if let Err(e) = get_live(
                    socket_path.clone(),
                    &self_live_getter,
                    &mut ht,
                    &mut got_at_least_one,
                )
                .await
                {
                    log::error!("Getting live feed: {}", e);
                }
                if !got_at_least_one {
                    ht.healthy_live_feed(false).await;
                }
                sleep(Duration::from_millis(10000)).await;
            }
        });
        tokio::spawn(async move {
            let mut rotated = Instant::now();
            loop {
                let elapsed = rotated.elapsed();
                if elapsed < ROTATE_PERIOD {
                    sleep(ROTATE_PERIOD - elapsed).await;
                    continue;
                }
                self.expire().await;
                self.delete_expired().await;
                self.rotate().await;
                rotated = Instant::now();
                self.report().await;
            }
        });
    }
}
