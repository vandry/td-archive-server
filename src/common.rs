use chrono::{TimeZone, Utc};
use std::time::SystemTime;

use crate::openraildata_pb::{TdFrame, TdQuery};

pub fn query_matches(q: &TdQuery, f: &TdFrame) -> bool {
    if let Some(ref ts) = f.timestamp {
        if let Some(ref fts) = q.from_timestamp {
            if (ts.seconds, ts.nanos) < (fts.seconds, fts.nanos) {
                return false;
            }
        }
        if let Some(ref tts) = q.to_timestamp {
            if (ts.seconds, ts.nanos) >= (tts.seconds, tts.nanos) {
                return false;
            }
        }
    }
    if !q.area_id.is_empty() {
        match f.area_id {
            None => {
                return false;
            }
            Some(ref area_id) => {
                if !q.area_id.iter().any(|candidate| candidate == area_id) {
                    return false;
                }
            }
        };
    }
    if !q.description.is_empty() {
        match f.description {
            None => {
                return false;
            }
            Some(ref description) => {
                if !q
                    .description
                    .iter()
                    .any(|candidate| candidate == description)
                {
                    return false;
                }
            }
        };
    }
    true
}

pub fn intersect(acc: &mut [u8], v: &[u8]) {
    if v.len() < acc.len() {
        return;
    }
    acc.iter_mut()
        .zip(v.iter())
        .for_each(|(acccell, cell)| *acccell &= cell);
}

pub fn union(acc: &mut [u8], v: &[u8]) {
    if v.len() < acc.len() {
        return;
    }
    acc.iter_mut()
        .zip(v.iter())
        .for_each(|(acccell, cell)| *acccell |= cell);
}

pub fn now_time_t() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .try_into()
        .unwrap()
}

pub fn archive_filenames(day: i64) -> (String, String) {
    let base = Utc.timestamp_opt(day, 0).unwrap().format("%Y%m%d");
    let mut data_name = base.to_string();
    data_name.push_str(".TDFrames");
    let mut index_name = base.to_string();
    index_name.push_str(".TDIndex.xz");
    (data_name, index_name)
}
