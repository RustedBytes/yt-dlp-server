use std::path::Path;

use anyhow::Context;
use hmac::{Hmac, Mac};
use log::{debug, trace};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::{fs, io::AsyncReadExt};

type HmacSha256 = Hmac<Sha256>;

pub async fn append_jsonl(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    trace!(
        "appending JSONL path={} bytes={}",
        path.display(),
        line.len()
    );

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    use tokio::io::AsyncWriteExt;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("failed to open JSONL file {}", path.display()))?;
    file.write_all(&line)
        .await
        .with_context(|| format!("failed to append JSONL file {}", path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("failed to flush JSONL file {}", path.display()))?;
    debug!(
        "JSONL append flushed path={} bytes={}",
        path.display(),
        line.len()
    );
    Ok(())
}

pub fn hmac_sha256_hex(secret: &str, bytes: &[u8]) -> String {
    hmac_sha256_bytes(secret.as_bytes(), bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn hmac_sha256_bytes(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts secrets of any length");
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

pub async fn sha256_file_hex(path: &Path) -> std::io::Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn signs_bytes_as_lowercase_hmac_sha256_hex() {
        let signature = hmac_sha256_hex("secret", b"payload");

        assert_eq!(
            signature,
            "b82fcb791acec57859b989b430a826488ce2e479fdf92326bd0a2e8375a42ba4"
        );
    }

    #[tokio::test]
    async fn hashes_file_as_lowercase_sha256_hex() {
        let path = temp_path("sha256.bin");
        fs::write(&path, b"video").await.unwrap();

        let digest = sha256_file_hex(&path).await.unwrap();

        assert_eq!(
            digest,
            "0cab1c9617404faf2b24e221e189ca5945813e14d3f766345b09ca13bbe28ffc"
        );
        fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn appends_json_lines() {
        let path = temp_path("append-jsonl.jsonl");

        append_jsonl(&path, &json!({ "a": 1 })).await.unwrap();
        append_jsonl(&path, &json!({ "b": 2 })).await.unwrap();

        let contents = fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents, "{\"a\":1}\n{\"b\":2}\n");

        fs::remove_file(path).await.unwrap();
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("yt-dlp-server-{nanos}-{name}"))
    }
}
