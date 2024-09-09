// Backs up an S3 bucket to local files.
// We prepend the etag to the filename so that if the contents change we
// have a backup of both versions, although we never expect that to happen.
// We also deliberately never delete files, for basically the same reason.

use s3::creds::Credentials;
use s3::serde_types::Object;
use s3::Bucket;
use s3::Region;
use std::env;
use std::path::{Path, PathBuf};

async fn list_bucket(bucket: &Bucket) -> Result<Vec<Object>, Box<dyn std::error::Error>> {
    let entries = bucket
        .list(String::from(""), Some(String::from("")))
        .await?;
    if entries.len() != 1 {
        return Err(Box::from(format!(
            "Listing bucket {}, expected 1 result, got {}",
            bucket.name(),
            entries.len()
        )));
    }
    let entry0 = entries.into_iter().next().unwrap();
    if entry0.name != bucket.name() {
        return Err(Box::from(format!(
            "Listing bucket {}, expected 1 result with name {}, got name {}",
            bucket.name(),
            bucket.name(),
            entry0.name
        )));
    }
    if entry0.is_truncated {
        return Err(Box::from("Got unsupported truncated result"));
    }
    if entry0.contents.is_empty() {
        return Err(Box::from("Suspicious: empty bucket"));
    }
    Ok(entry0.contents)
}

fn safe_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    if name.starts_with(".") {
        return false;
    }
    for c in name.chars() {
        if c == '/' || c.is_control() {
            return false;
        }
    }
    true
}

async fn maybe_copy(
    bucket: &Bucket,
    obj: &Object,
    dst_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let etag = obj.e_tag.as_ref().unwrap();
    let local_name = dst_path.join(format!("{}.{}", etag, &obj.key));
    if local_name.try_exists()? {
        return Ok(());
    }
    let d = bucket.get_object(&obj.key).await?;
    match d.headers().get("etag") {
        None => {
            return Err(Box::from(format!(
                "Downloaded object {} without etag",
                &obj.key
            )));
        }
        Some(got_etag) => {
            if got_etag != etag {
                return Err(Box::from(format!(
                    "Etag of object {} changed between list and download, was {} now {}",
                    &obj.key, etag, got_etag
                )));
            }
        }
    }
    Ok(std::fs::write(local_name, d.as_slice())?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().collect();
    if args.len() != 5 {
        eprintln!(
            "Usage: {} endpoint region src-bucket dst-path",
            args[0].to_string_lossy()
        );
        std::process::exit(3);
    }
    let s3_endpoint = args[1].to_string_lossy().into_owned();
    let s3_region_name = args[2].to_string_lossy().into_owned();
    let bucket_name = args[3].to_string_lossy().into_owned();
    let dst_path = PathBuf::from(args[4].clone());

    let s3_cred = Credentials::default().unwrap();
    let s3_region = Region::Custom {
        region: s3_region_name,
        endpoint: s3_endpoint,
    };

    let bucket = Bucket::new(&bucket_name, s3_region, s3_cred).unwrap();
    let mut success = true;
    for obj in list_bucket(&bucket).await? {
        if obj.e_tag.is_none() {
            success = false;
            eprintln!("Got object without e_tag {:?}", obj);
            continue;
        }
        if !safe_name(&obj.key) || !safe_name(obj.e_tag.as_ref().unwrap()) {
            success = false;
            eprintln!("Got object with unsafe name {:?}", obj);
            continue;
        }
        if let Err(e) = maybe_copy(&bucket, &obj, &dst_path).await {
            success = false;
            eprintln!("Copying {}: {}", obj.key, e);
        }
    }
    if !success {
        std::process::exit(1);
    }
    Ok(())
}
