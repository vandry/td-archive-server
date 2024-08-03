use chrono::{DateTime, TimeZone, Utc};
use dirs_sys::home_dir;
use memmap::{Mmap, MmapOptions};
use prost::{DecodeError, Message};
use regex::Regex;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use xz::write::XzEncoder;

mod openraildata_pb {
    tonic::include_proto!("openraildata");
}

mod preserve;

use crate::openraildata_pb::{TdFrame, TdIndex, TdIndexVector};

const VECTOR_COMPRESSION: usize = 10;

fn find_batches(dir: &Path) -> io::Result<HashMap<DateTime<Utc>, Vec<PathBuf>>> {
    let batchdir = dir.join("batches");
    let batch_re = Regex::new(r"^batch\.(\d\d\d\d)(\d\d)(\d\d)\.").unwrap();
    let mut available_batches = HashMap::new();
    for entry in fs::read_dir(batchdir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(base) = path.file_name().expect("path file_name").to_str() {
            if let Some(caps) = batch_re.captures(base) {
                let (_, [y_s, m_s, d_s]) = caps.extract();
                let y = y_s.parse::<i32>().unwrap();
                let m = m_s.parse::<u32>().unwrap();
                let d = d_s.parse::<u32>().unwrap();
                let when = Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap();
                available_batches
                    .entry(when)
                    .or_insert_with(Vec::new)
                    .push(path);
            }
        }
    }
    Ok(available_batches)
}

struct IndexVector {
    m: TdIndexVector,
    vector_compression: usize,
}

impl IndexVector {
    fn new(key: &str, vector_compression: usize, vector_len: usize) -> Self {
        let mut veclen: usize = vector_len / vector_compression / 8;
        if veclen * 8 * vector_compression < vector_len {
            veclen += 1;
        }
        let raw_vec = vec![0; veclen];
        Self {
            m: TdIndexVector {
                key: Some(key.to_string()),
                vector: Some(raw_vec),
            },
            vector_compression,
        }
    }

    fn mark(&mut self, i: usize) {
        let ci = i / self.vector_compression;
        let bi = ci / 8;
        let bit = ci - bi * 8;
        self.m.vector.as_mut().unwrap()[bi] |= 1 << bit;
    }

    fn into_td_index_vector(self) -> TdIndexVector {
        self.m
    }
}

struct LenBlobFileIterator<'a> {
    map: &'a Mmap,
    pos: usize,
}

impl<'a> Iterator for LenBlobFileIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 4 > self.map.len() {
            return None;
        }
        let item_size: usize = usize::from(self.map[self.pos])
            | (usize::from(self.map[self.pos + 1]) << 8)
            | (usize::from(self.map[self.pos + 2]) << 16)
            | (usize::from(self.map[self.pos + 3]) << 24);
        self.pos += 4;
        if self.pos + item_size > self.map.len() {
            return None;
        }
        let item_pos = self.pos;
        self.pos += item_size;
        Some(&self.map[item_pos..item_pos + item_size])
    }
}

fn iter_len_blob_file(map: &Mmap) -> LenBlobFileIterator {
    LenBlobFileIterator { map, pos: 0 }
}

fn build(
    dir: &PathBuf,
    when: &DateTime<Utc>,
    file_list: &[PathBuf],
    vector_compression: usize,
) -> io::Result<()> {
    let maps: Vec<Mmap> = file_list
        .iter()
        .map(|path| {
            let file = File::open(path)?;
            unsafe { MmapOptions::new().map(&file) }
        })
        .collect::<Result<Vec<Mmap>, _>>()?;

    let mut frames = maps
        .iter()
        .flat_map(iter_len_blob_file)
        .map(|blob| {
            let m = TdFrame::decode(blob)?;
            Ok((blob, m))
        })
        .collect::<Result<Vec<(&[u8], TdFrame)>, DecodeError>>()?;

    frames.sort_by_key(|(_, m)| match m.timestamp {
        Some(ref ts) => (ts.seconds, ts.nanos),
        None => (0, 0),
    });

    let mut index = TdIndex {
        vector_compression: Some(vector_compression.try_into().unwrap()),
        ..Default::default()
    };
    let mut area_ids = HashMap::new();
    let mut desc = [(); 4].map(|_| HashMap::new());

    let datafile = NamedTempFile::new_in(dir)?;
    let mut pos: usize = 0;
    let mut stream = BufWriter::new(datafile.as_file());
    for (i, (ser, frame)) in frames.iter().enumerate() {
        let serlen = ser.len();
        let header: [u8; 4] = [
            (serlen & 255).try_into().unwrap(),
            ((serlen >> 8) & 255).try_into().unwrap(),
            ((serlen >> 16) & 255).try_into().unwrap(),
            ((serlen >> 24) & 255).try_into().unwrap(),
        ];
        let n = stream.write(&header)?;
        if n != 4 {
            return Err(io::Error::new(io::ErrorKind::Other, "short write"));
        }
        pos += 4;
        index.frame_offset.push(pos.try_into().unwrap());
        let n = stream.write(ser)?;
        if n != serlen {
            return Err(io::Error::new(io::ErrorKind::Other, "short write"));
        }
        pos += serlen;

        if let Some(ref area_id) = frame.area_id {
            area_ids
                .entry(area_id)
                .or_insert_with(|| IndexVector::new(area_id, vector_compression, frames.len()))
                .mark(i);
        }
        if let Some(ref description) = frame.description {
            for di in [0, 1, 2, 3] {
                let key = if description.len() >= di {
                    &description[di..di + 1]
                } else {
                    "\0"
                };
                desc[di]
                    .entry(key)
                    .or_insert_with(|| IndexVector::new(key, vector_compression, frames.len()))
                    .mark(i);
            }
        }
    }
    stream.flush()?;
    index.data_length = Some(pos.try_into().unwrap());
    drop(stream);

    index
        .area_ids
        .extend(area_ids.into_values().map(|v| v.into_td_index_vector()));
    let mut desc_values = desc
        .into_iter()
        .map(|d| d.into_values().map(|v| v.into_td_index_vector()));
    index.description0.extend(desc_values.next().unwrap());
    index.description1.extend(desc_values.next().unwrap());
    index.description2.extend(desc_values.next().unwrap());
    index.description3.extend(desc_values.next().unwrap());

    let indexfile = NamedTempFile::new_in(dir)?;
    let mut encoder = XzEncoder::new(indexfile.as_file(), 7);
    encoder.write_all(&index.encode_to_vec())?;
    encoder.finish()?;

    let base = when.format("%Y%m%d");
    let mut data_name = base.to_string();
    data_name.push_str(".TDFrames");
    let mut index_name = base.to_string();
    index_name.push_str(".TDIndex.xz");
    datafile.persist_noclobber(dir.join(data_name))?;
    indexfile.persist_noclobber(dir.join(index_name))?;

    for spool_file in file_list.iter() {
        std::fs::remove_file(spool_file)?;
    }

    Ok(())
}

fn main() -> io::Result<()> {
    let dir = home_dir().expect("$HOME").join("var/tdfeed");
    let batches = find_batches(&dir)?;
    let now = Utc::now();
    let mut success = true;
    for (when, file_list) in batches.iter() {
        let age = (now - when).num_hours();
        if age < 36 {
            // 12 hours after the end of that day
            continue; // too soon: we might still write batches
        }
        if let Err(e) = build(&dir, when, file_list, VECTOR_COMPRESSION) {
            success = false;
            eprintln!("Building {}: {}", when.format("%Y-%m-%d"), e);
        }
    }
    if !success {
        std::process::exit(1);
    }
    Ok(())
}
