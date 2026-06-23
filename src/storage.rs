use std::path::Path;

use anyhow::{Context, anyhow};
use log::debug;
use reqwest::{
    Url,
    header::{HOST, HeaderMap, HeaderName, HeaderValue},
};
use time::OffsetDateTime;
use tokio::fs;
use uuid::Uuid;

use crate::{
    config::{Config, ObjectStorageBackend, ObjectStorageConfig},
    types::{DownloadMetadata, StoredArtifacts, StoredObject},
    util::{hex_lower, hmac_sha256_bytes, sha256_file_hex, sha256_hex},
};

pub async fn store_download_artifacts(
    config: &Config,
    job_id: Uuid,
    metadata: &DownloadMetadata,
) -> anyhow::Result<Option<StoredArtifacts>> {
    match config.object_storage.backend {
        ObjectStorageBackend::Local => Ok(None),
        ObjectStorageBackend::S3 => {
            let media = upload_file(config, job_id, &metadata.media_path, "media").await?;
            let info_json =
                upload_file(config, job_id, &metadata.info_json_path, "info.json").await?;
            Ok(Some(StoredArtifacts {
                backend: config.object_storage.backend.as_str().to_string(),
                bucket: config.object_storage.bucket.clone(),
                media,
                info_json,
            }))
        }
    }
}

async fn upload_file(
    config: &Config,
    job_id: Uuid,
    path: &Path,
    role: &str,
) -> anyhow::Result<StoredObject> {
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("failed to read artifact {}", path.display()))?;
    let size = bytes.len() as u64;
    let sha256 = sha256_file_hex(path)
        .await
        .with_context(|| format!("failed to hash artifact {}", path.display()))?;
    let key = object_key(&config.object_storage, job_id, path, role);
    put_s3_object(&config.object_storage, &key, bytes).await?;
    let url = public_object_url(&config.object_storage, &key);

    debug!(
        "stored artifact in object storage backend={} key={} bytes={}",
        config.object_storage.backend.as_str(),
        key,
        size
    );

    Ok(StoredObject {
        key,
        url,
        bytes: size,
        sha256,
    })
}

fn object_key(config: &ObjectStorageConfig, job_id: Uuid, path: &Path, role: &str) -> String {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.is_empty());
    let file_name = match extension {
        Some(extension) if role == "media" => format!("{job_id}.{extension}"),
        _ => format!("{job_id}.{role}"),
    };
    if config.prefix.is_empty() {
        format!("{job_id}/{file_name}")
    } else {
        format!("{}/{job_id}/{file_name}", config.prefix)
    }
}

async fn put_s3_object(
    config: &ObjectStorageConfig,
    key: &str,
    body: Vec<u8>,
) -> anyhow::Result<()> {
    let request = signed_put_request(config, key, &body)?;
    let response = reqwest::Client::new()
        .put(request.url)
        .headers(request.headers)
        .body(body)
        .send()
        .await
        .context("failed to send object storage PUT request")?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "object storage PUT failed with HTTP {status}: {}",
            text.chars().take(2_000).collect::<String>()
        ));
    }
    Ok(())
}

struct SignedPutRequest {
    url: Url,
    headers: HeaderMap,
}

fn signed_put_request(
    config: &ObjectStorageConfig,
    key: &str,
    body: &[u8],
) -> anyhow::Result<SignedPutRequest> {
    let url = object_url(config, key)?;
    let now = OffsetDateTime::now_utc();
    let amz_date = now
        .format(&time::macros::format_description!(
            "[year][month][day]T[hour][minute][second]Z"
        ))
        .context("failed to format x-amz-date")?;
    let date = now
        .format(&time::macros::format_description!("[year][month][day]"))
        .context("failed to format SigV4 date scope")?;
    let payload_hash = sha256_hex(body);
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("object storage endpoint URL must include a host"))?;
    let host = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    let session_token = config.session_token.as_deref();
    let canonical_uri = url.path();
    let signed_headers = if session_token.is_some() {
        "host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
    } else {
        "host;x-amz-content-sha256;x-amz-date"
    };
    let canonical_headers = match session_token {
        Some(token) => format!(
            "host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\nx-amz-security-token:{token}\n"
        ),
        None => {
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n")
        }
    };
    let canonical_request =
        format!("PUT\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/{}/s3/aws4_request", config.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let access_key = config
        .access_key_id
        .as_deref()
        .ok_or_else(|| anyhow!("object storage access key is not configured"))?;
    let secret_key = config
        .secret_access_key
        .as_deref()
        .ok_or_else(|| anyhow!("object storage secret key is not configured"))?;
    let signing_key = signing_key(secret_key, &date, &config.region);
    let signature = hex_hmac(&signing_key, string_to_sign.as_bytes());
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut headers = HeaderMap::new();
    headers.insert(HOST, header_value(&host)?);
    headers.insert(
        HeaderName::from_static("x-amz-content-sha256"),
        header_value(&payload_hash)?,
    );
    headers.insert(
        HeaderName::from_static("x-amz-date"),
        header_value(&amz_date)?,
    );
    headers.insert(
        HeaderName::from_static("authorization"),
        header_value(&authorization)?,
    );
    if let Some(token) = session_token {
        headers.insert(
            HeaderName::from_static("x-amz-security-token"),
            header_value(token)?,
        );
    }

    Ok(SignedPutRequest { url, headers })
}

fn object_url(config: &ObjectStorageConfig, key: &str) -> anyhow::Result<Url> {
    let endpoint = config
        .endpoint_url
        .as_deref()
        .ok_or_else(|| anyhow!("object storage endpoint URL is not configured"))?;
    let bucket = config
        .bucket
        .as_deref()
        .ok_or_else(|| anyhow!("object storage bucket is not configured"))?;
    let endpoint = endpoint.trim_end_matches('/');
    let encoded_key = encode_path(key);
    let url = if config.force_path_style {
        format!("{endpoint}/{}/{}", encode_path(bucket), encoded_key)
    } else {
        let endpoint =
            Url::parse(endpoint).context("failed to parse object storage endpoint URL")?;
        let scheme = endpoint.scheme();
        let host = endpoint
            .host_str()
            .ok_or_else(|| anyhow!("object storage endpoint URL must include a host"))?;
        let port = endpoint
            .port()
            .map(|port| format!(":{port}"))
            .unwrap_or_default();
        format!("{scheme}://{bucket}.{host}{port}/{encoded_key}")
    };
    Url::parse(&url).context("failed to build object storage URL")
}

fn public_object_url(config: &ObjectStorageConfig, key: &str) -> Option<String> {
    config.public_base_url.as_ref().map(|base| {
        format!(
            "{}/{}",
            base.trim_end_matches('/'),
            encode_path(key).trim_start_matches('/')
        )
    })
}

fn encode_path(value: &str) -> String {
    value
        .split('/')
        .map(percent_encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn percent_encode_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn signing_key(secret_key: &str, date: &str, region: &str) -> Vec<u8> {
    let date_key = hmac_sha256_bytes(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let date_region_key = hmac_sha256_bytes(&date_key, region.as_bytes());
    let date_region_service_key = hmac_sha256_bytes(&date_region_key, b"s3");
    hmac_sha256_bytes(&date_region_service_key, b"aws4_request")
}

fn hex_hmac(key: &[u8], bytes: &[u8]) -> String {
    hex_lower(&hmac_sha256_bytes(key, bytes))
}

fn header_value(value: &str) -> anyhow::Result<HeaderValue> {
    HeaderValue::from_str(value).context("failed to build object storage request header")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_keys_include_prefix_job_and_stable_file_names() {
        let config = ObjectStorageConfig {
            backend: ObjectStorageBackend::S3,
            endpoint_url: Some("http://localhost:9000".to_string()),
            bucket: Some("downloads".to_string()),
            region: "us-east-1".to_string(),
            access_key_id: Some("access".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: None,
            prefix: "prod/videos".to_string(),
            force_path_style: true,
            public_base_url: None,
        };
        let job_id = Uuid::parse_str("10af7128-4b98-4e19-a494-17a3d5597e2c").unwrap();

        assert_eq!(
            object_key(&config, job_id, Path::new("download.mp4"), "media"),
            "prod/videos/10af7128-4b98-4e19-a494-17a3d5597e2c/10af7128-4b98-4e19-a494-17a3d5597e2c.mp4"
        );
        assert_eq!(
            object_key(
                &config,
                job_id,
                Path::new("download.info.json"),
                "info.json"
            ),
            "prod/videos/10af7128-4b98-4e19-a494-17a3d5597e2c/10af7128-4b98-4e19-a494-17a3d5597e2c.info.json"
        );
    }

    #[test]
    fn builds_path_style_object_url_with_encoded_key() {
        let config = ObjectStorageConfig {
            backend: ObjectStorageBackend::S3,
            endpoint_url: Some("http://localhost:9000".to_string()),
            bucket: Some("my bucket".to_string()),
            region: "us-east-1".to_string(),
            access_key_id: Some("access".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: None,
            prefix: String::new(),
            force_path_style: true,
            public_base_url: None,
        };

        let url = object_url(&config, "job id/video file.mp4").unwrap();

        assert_eq!(
            url.as_str(),
            "http://localhost:9000/my%20bucket/job%20id/video%20file.mp4"
        );
    }

    #[test]
    fn signed_put_request_sets_sigv4_headers() {
        let config = ObjectStorageConfig {
            backend: ObjectStorageBackend::S3,
            endpoint_url: Some("http://localhost:9000".to_string()),
            bucket: Some("downloads".to_string()),
            region: "us-east-1".to_string(),
            access_key_id: Some("access".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: Some("token".to_string()),
            prefix: String::new(),
            force_path_style: true,
            public_base_url: None,
        };

        let request = signed_put_request(&config, "job/video.mp4", b"video").unwrap();

        assert_eq!(
            request.url.as_str(),
            "http://localhost:9000/downloads/job/video.mp4"
        );
        assert!(request.headers.contains_key("authorization"));
        assert!(request.headers.contains_key("x-amz-date"));
        assert!(request.headers.contains_key("x-amz-content-sha256"));
        assert_eq!(
            request.headers.get("x-amz-security-token").unwrap(),
            "token"
        );
    }
}
