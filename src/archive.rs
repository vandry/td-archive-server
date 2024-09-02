use async_stream::stream;
use chrono::{TimeZone, Utc};
use futures::future::join_all;
use futures::Stream;
use lru::LruCache;
use memmap::{Mmap, MmapOptions};
use prost::Message;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::iter;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::OnceCell;
use tokio::time::{sleep, Duration};
use tonic::Status;
use xz::read::XzDecoder;

use crate::common::{archive_filenames, intersect, now_time_t, query_matches, union};
use crate::openraildata_pb::{TdFrame, TdIndex, TdIndexVector, TdQuery};
use crate::preserve;

const NEGATIVE_CACHE_TIME: u64 = 240;
const POSITIVE_CACHE_TIME: i64 = 3 * 3600;

fn read_index(path: &PathBuf) -> io::Result<TdIndex> {
    let indexfile = File::open(path)?;
    let mut decompressor = XzDecoder::new(indexfile);
    let mut contents = Vec::new();
    decompressor.read_to_end(&mut contents)?;
    Ok(TdIndex::decode(contents.as_slice())?)
}

fn index_vectors_to_hashmap(v: Vec<TdIndexVector>) -> HashMap<String, Vec<u8>> {
    HashMap::from_iter(v.into_iter().map(|mut v| {
        (
            std::mem::take(&mut v.key).unwrap_or_default(),
            std::mem::take(&mut v.vector).unwrap_or_default(),
        )
    }))
}

struct IndexReader {
    map: Mmap,
    vector_compression: usize,
    data_length: usize,
    veclen: usize,
    start_time: i64,
    memory_estimate: usize,
    frame_offsets: Vec<u32>,
    area_ids: HashMap<String, Vec<u8>>,
    descriptions: [HashMap<String, Vec<u8>>; 4],
    last_used: AtomicI64,
}

impl Drop for IndexReader {
    fn drop(&mut self) {
        log::info!(
            "Unloaded {}",
            Utc.timestamp_opt(self.start_time, 0)
                .unwrap()
                .format("%Y-%m-%d")
        );
    }
}

impl IndexReader {
    fn load(dir: &Path, day: i64) -> io::Result<Self> {
        let (data_name, index_name) = archive_filenames(dir, day);
        let mut index = read_index(&index_name)?;
        let datafile = File::open(data_name)?;
        let map = unsafe { MmapOptions::new().map(&datafile) }?;

        let data_length: usize = index.data_length.unwrap_or_default().try_into().unwrap();
        if map.len() < data_length {
            return Err(io::Error::new(io::ErrorKind::Other, "data file too short"));
        }
        let num_frames = index.frame_offset.len();
        let vector_compression: usize = index
            .vector_compression
            .unwrap_or_default()
            .try_into()
            .unwrap();
        let mut veclen: usize = num_frames / vector_compression / 8;
        if veclen * 8 * vector_compression < num_frames {
            veclen += 1;
        }

        let mut ret = Self {
            map,
            vector_compression,
            data_length,
            veclen,
            start_time: day,
            memory_estimate: 0,
            frame_offsets: std::mem::take(&mut index.frame_offset),
            area_ids: index_vectors_to_hashmap(std::mem::take(&mut index.area_ids)),
            descriptions: [
                index_vectors_to_hashmap(std::mem::take(&mut index.description0)),
                index_vectors_to_hashmap(std::mem::take(&mut index.description1)),
                index_vectors_to_hashmap(std::mem::take(&mut index.description2)),
                index_vectors_to_hashmap(std::mem::take(&mut index.description3)),
            ],
            last_used: AtomicI64::new(0),
        };
        let nvectors: usize =
            ret.area_ids.len() + ret.descriptions.iter().map(|h| h.len()).sum::<usize>();
        ret.memory_estimate = 4 * ret.frame_offsets.len() + veclen * nvectors;
        log::info!(
            "Loaded {}",
            Utc.timestamp_opt(day, 0).unwrap().format("%Y-%m-%d")
        );
        Ok(ret)
    }

    fn find_timestamp(&self, ts: i64, p0: usize, p1: usize) -> usize {
        let pmid = (p0 + p1) / 2;
        if pmid == p0 {
            return p0;
        }
        match self.get::<TdFrame>(pmid) {
            None => p0,
            Some(frame) => {
                if ts < frame.timestamp.unwrap_or_default().seconds {
                    self.find_timestamp(ts, p0, pmid)
                } else {
                    self.find_timestamp(ts, pmid, p1)
                }
            }
        }
    }

    fn search<'a>(&'a self, q: &TdQuery) -> IndexIterator<'a> {
        let start_pos = match q.from_timestamp {
            None => 0,
            Some(ref from_timestamp) => {
                if from_timestamp.seconds < self.start_time {
                    0
                } else {
                    self.find_timestamp(from_timestamp.seconds, 0, self.frame_offsets.len())
                }
            }
        };
        let mut matches = Vec::with_capacity(self.veclen);
        matches.resize(self.veclen, 255);

        if !q.area_id.is_empty() {
            let mut area_id_matches = vec![0_u8; self.veclen];
            for area_id in q.area_id.iter() {
                if let Some(v) = self.area_ids.get(area_id) {
                    union(&mut area_id_matches, v);
                }
            }
            intersect(&mut matches, &area_id_matches);
        }

        if !q.description.is_empty() {
            let mut description_matches = vec![0_u8; self.veclen];
            for description in q.description.iter() {
                let mut one_desc_matches = Vec::with_capacity(self.veclen);
                one_desc_matches.resize(self.veclen, 255);
                let mut possible = true;
                for (i, charindex) in self.descriptions.iter().enumerate() {
                    let key = if description.len() >= i {
                        &description[i..i + 1]
                    } else {
                        "\0"
                    };
                    if let Some(v) = charindex.get(key) {
                        intersect(&mut one_desc_matches, v);
                    } else {
                        possible = false;
                        break;
                    }
                }
                if possible {
                    union(&mut description_matches, &one_desc_matches);
                }
            }
            intersect(&mut matches, &description_matches);
        }

        IndexIterator::new(self, matches, start_pos)
    }

    fn get<T: prost::Message + Default>(&self, i: usize) -> Option<T> {
        if i >= self.frame_offsets.len() {
            return None;
        }
        let start: usize = self.frame_offsets[i].try_into().unwrap();
        let end: usize = if (i + 1) == self.frame_offsets.len() {
            self.data_length
        } else {
            (self.frame_offsets[i + 1] - 4).try_into().unwrap()
        };
        if start > end || start > self.data_length || end > self.data_length {
            return None;
        }
        T::decode(&self.map[start..end]).ok()
    }
}

struct IndexIterator<'a> {
    r: &'a IndexReader,
    matches: Vec<u8>,
    pos: usize,
}

impl<'a> IndexIterator<'a> {
    fn new(r: &'a IndexReader, matches: Vec<u8>, start_pos: usize) -> IndexIterator<'a> {
        IndexIterator {
            r,
            matches,
            pos: start_pos,
        }
    }
}

impl<'a> Iterator for IndexIterator<'a> {
    type Item = preserve::TdFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let vector_compression = self.r.vector_compression;
        while self.pos < self.r.frame_offsets.len() {
            let compression_offset = self.pos % vector_compression;
            if compression_offset != 0 {
                // We must have already matched against this bit.
                let ret = self.r.get(self.pos);
                self.pos += 1;
                if ret.is_none() {
                    continue;
                }
                return ret;
            }
            let vi = self.pos / vector_compression;
            let byte_i = vi >> 3;
            let bit = 1 << (vi & 7);
            if self.matches[byte_i] & bit == bit {
                let ret = self.r.get(self.pos);
                self.pos += 1;
                if ret.is_none() {
                    continue;
                }
                return ret;
            }
            self.pos += vector_compression;
        }
        None
    }
}

enum IndexRepoEntry {
    Entry(Arc<IndexReader>),
    Missing(Instant),
}

pub struct IndexRepo {
    dir: PathBuf,
    cache: Mutex<LruCache<i64, Arc<OnceCell<IndexRepoEntry>>>>,
}

impl IndexRepo {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_owned(),
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap())),
        }
    }

    async fn get(&self, day: i64) -> Option<Arc<IndexReader>> {
        let entry = self
            .cache
            .lock()
            .unwrap()
            .get_or_insert(day, || Arc::new(OnceCell::new()))
            .clone();
        match entry
            .get_or_init(|| async {
                match IndexReader::load(&self.dir, day) {
                    Ok(r) => IndexRepoEntry::Entry(Arc::new(r)),
                    Err(e) => {
                        log::error!(
                            "Unable to load data for {}: {}",
                            Utc.timestamp_opt(day, 0).unwrap().format("%Y-%m-%d"),
                            e
                        );
                        IndexRepoEntry::Missing(Instant::now())
                    }
                }
            })
            .await
        {
            IndexRepoEntry::Entry(r) => {
                r.last_used.store(now_time_t(), Ordering::Release);
                Some(r.clone())
            }
            IndexRepoEntry::Missing(_) => None,
        }
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut infolog = Instant::now();
            loop {
                let (count, memory_estimate) = self.get_stats();
                if infolog.elapsed().as_secs() >= 600 {
                    log::info!(
                        "Caching {} days using at least {} bytes of memory",
                        count,
                        memory_estimate
                    );
                    infolog = Instant::now();
                }
                sleep(Duration::from_millis(10000)).await;
            }
        });
    }

    fn get_stats(&self) -> (usize, usize) {
        let now = now_time_t();
        let mut cache = self.cache.lock().unwrap();
        let mut todelete = Vec::new();
        let mut count = 0;
        let mut memory_estimate = 0;
        for (k, v) in cache.iter() {
            match v.get() {
                Some(IndexRepoEntry::Entry(r)) => {
                    if now - r.last_used.load(Ordering::Acquire) > POSITIVE_CACHE_TIME {
                        todelete.push(*k);
                    } else {
                        count += 1;
                        memory_estimate += r.memory_estimate;
                    }
                }
                Some(IndexRepoEntry::Missing(vintage)) => {
                    if vintage.elapsed().as_secs() > NEGATIVE_CACHE_TIME {
                        todelete.push(*k);
                    }
                }
                None => {}
            }
        }
        for k in todelete.into_iter() {
            cache.pop(&k);
        }
        (count, memory_estimate)
    }

    pub fn feed(
        self: Arc<Self>,
        q: TdQuery,
        day: i64,
        end_ts: i64,
    ) -> impl Stream<Item = Result<preserve::TdFrame, Status>> {
        stream! {
            let indices = iter::repeat(day)
                .enumerate()
                .map(|(i, v)| v + (i as i64) * 86400)
                .take_while(|day| *day < end_ts)
                .map(|day| self.get(day));
            for index in join_all(indices)
                .await
                .into_iter()
                .flatten()
            {
                let results = index
                    .search(&q)
                    .take_while(|f| match q.to_timestamp {
                        None => true,
                        Some(ref to_ts) => {
                            if let Some(ref ts) = f.timestamp {
                                (ts.seconds, ts.nanos) < (to_ts.seconds, to_ts.nanos)
                            } else {
                                true
                            }
                        }
                    })
                    .filter(|f| query_matches(&q, f));
                for frame in results {
                    yield Ok(frame);
                }
            }
        }
    }
}
