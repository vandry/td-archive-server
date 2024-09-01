use chrono::{DateTime, TimeZone, Utc};
use futures::future::join_all;
use prost::{DecodeError, Message};
use regex::Regex;
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::request::ResponseData;
use s3::Bucket;
use s3::Region;
use std::collections::HashMap;
use std::env;
use std::io::Write;
use xz::write::XzEncoder;

mod openraildata_pb {
    tonic::include_proto!("openraildata");
}

mod preserve;

use crate::openraildata_pb::{TdFrame, TdIndex, TdIndexVector};

const VECTOR_COMPRESSION: usize = 10;

async fn find_batches(bucket: &Bucket) -> Result<HashMap<DateTime<Utc>, Vec<String>>, S3Error> {
    let batch_re = Regex::new(r"^.*/batch\.(\d\d\d\d)(\d\d)(\d\d)\.").unwrap();
    let mut available_batches = HashMap::new();
    for entry in bucket
        .list(String::from(""), Some(String::from("")))
        .await?
    {
        for obj in entry.contents {
            let name = obj.key;
            if let Some(caps) = batch_re.captures(&name) {
                let (_, [y_s, m_s, d_s]) = caps.extract();
                let y = y_s.parse::<i32>().unwrap();
                let m = m_s.parse::<u32>().unwrap();
                let d = d_s.parse::<u32>().unwrap();
                let when = Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap();
                available_batches
                    .entry(when)
                    .or_insert_with(Vec::new)
                    .push(name);
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
    contents: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for LenBlobFileIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 4 > self.contents.len() {
            return None;
        }
        let item_size: usize = usize::from(self.contents[self.pos])
            | (usize::from(self.contents[self.pos + 1]) << 8)
            | (usize::from(self.contents[self.pos + 2]) << 16)
            | (usize::from(self.contents[self.pos + 3]) << 24);
        self.pos += 4;
        if self.pos + item_size > self.contents.len() {
            return None;
        }
        let item_pos = self.pos;
        self.pos += item_size;
        Some(&self.contents[item_pos..item_pos + item_size])
    }
}

fn iter_len_blob_file(obj: &ResponseData) -> LenBlobFileIterator {
    LenBlobFileIterator {
        contents: obj.as_slice(),
        pos: 0,
    }
}

#[derive(Debug)]
struct AlreadyBuiltError;

impl std::error::Error for AlreadyBuiltError {}

impl std::fmt::Display for AlreadyBuiltError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Archive objects are already built for this date")
    }
}

async fn build(
    src_bucket: &Bucket,
    dst_bucket: &Bucket,
    when: &DateTime<Utc>,
    file_list: &[String],
    vector_compression: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let fetches = file_list.iter().map(|name| src_bucket.get_object(name));
    let maps = join_all(fetches)
        .await
        .into_iter()
        .collect::<Result<Vec<ResponseData>, _>>()?;

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

    let mut datafile: Vec<u8> = Vec::new();
    let mut pos: usize = 0;
    for (i, (ser, frame)) in frames.iter().enumerate() {
        let serlen = ser.len();
        let header: [u8; 4] = [
            (serlen & 255).try_into().unwrap(),
            ((serlen >> 8) & 255).try_into().unwrap(),
            ((serlen >> 16) & 255).try_into().unwrap(),
            ((serlen >> 24) & 255).try_into().unwrap(),
        ];
        datafile.extend_from_slice(&header);
        pos += 4;
        index.frame_offset.push(pos.try_into().unwrap());
        datafile.extend_from_slice(ser);
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
    index.data_length = Some(pos.try_into().unwrap());

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

    let mut indexfile: Vec<u8> = Vec::new();
    let mut encoder = XzEncoder::new(&mut indexfile, 7);
    encoder.write_all(&index.encode_to_vec())?;
    encoder.finish()?;

    let base = when.format("%Y%m%d");
    let mut data_name = base.to_string();
    data_name.push_str(".TDFrames");
    let mut index_name = base.to_string();
    index_name.push_str(".TDIndex.xz");

    // BUG: racy
    if dst_bucket.head_object(data_name.clone()).await.is_ok() {
        return Err(Box::new(AlreadyBuiltError {}));
    }

    dst_bucket.put_object(data_name, &datafile).await?;
    dst_bucket.put_object(index_name, &indexfile).await?;

    let deletes = file_list.iter().map(|name| src_bucket.delete_object(name));
    if let Some(err) = join_all(deletes)
        .await
        .into_iter()
        .filter_map(|r| r.err())
        .next()
    {
        return Err(Box::new(err));
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().collect();
    if args.len() != 5 {
        eprintln!(
            "Usage: {} endpoint region src-bucket dst-bucket",
            args[0].to_string_lossy()
        );
        std::process::exit(3);
    }
    let s3_endpoint = args[1].to_string_lossy().into_owned();
    let s3_region_name = args[2].to_string_lossy().into_owned();
    let src_bucket_name = args[3].to_string_lossy().into_owned();
    let dst_bucket_name = args[4].to_string_lossy().into_owned();

    let s3_cred = Credentials::default().unwrap();
    let s3_region = Region::Custom {
        region: s3_region_name,
        endpoint: s3_endpoint,
    };

    let src_bucket = Bucket::new(&src_bucket_name, s3_region.clone(), s3_cred.clone()).unwrap();
    let dst_bucket = Bucket::new(&dst_bucket_name, s3_region, s3_cred).unwrap();

    let batches = find_batches(&src_bucket).await?;
    let now = Utc::now();
    let mut success = true;
    for (when, file_list) in batches.iter() {
        let age = (now - when).num_hours();
        if age < 36 {
            // 12 hours after the end of that day
            continue; // too soon: we might still write batches
        }
        if let Err(e) = build(
            &src_bucket,
            &dst_bucket,
            when,
            file_list,
            VECTOR_COMPRESSION,
        )
        .await
        {
            success = false;
            eprintln!("Building {}: {}", when.format("%Y-%m-%d"), e);
        }
    }
    if !success {
        std::process::exit(1);
    }
    Ok(())
}
