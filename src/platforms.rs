use reqwest::Url;

#[derive(Debug, Clone, Copy)]
pub struct Platform {
    pub id: &'static str,
    pub hosts: &'static [&'static str],
    pub path_examples: &'static [&'static str],
}

const PLATFORMS: &[Platform] = &[
    Platform {
        id: "tiktok",
        hosts: &["tiktok.com"],
        path_examples: &["/@user/video/123", "/v/123", "vm.tiktok.com/<id>"],
    },
    Platform {
        id: "instagram",
        hosts: &["instagram.com"],
        path_examples: &["/reel/<id>/", "/reels/<id>/"],
    },
    Platform {
        id: "youtube",
        hosts: &["youtube.com", "youtu.be"],
        path_examples: &["/shorts/<id>", "youtu.be/<id>"],
    },
    Platform {
        id: "facebook",
        hosts: &["facebook.com", "fb.watch"],
        path_examples: &[
            "/reel/<id>",
            "/watch/<id>",
            "/share/r/<id>",
            "fb.watch/<id>/",
        ],
    },
    Platform {
        id: "snapchat",
        hosts: &["snapchat.com"],
        path_examples: &["/spotlight/<id>"],
    },
    Platform {
        id: "rutube",
        hosts: &["rutube.ru"],
        path_examples: &["/shorts/<id>/"],
    },
    Platform {
        id: "douyin",
        hosts: &["douyin.com"],
        path_examples: &["/video/<id>"],
    },
    Platform {
        id: "likee",
        hosts: &["likee.video"],
        path_examples: &["/v/<id>", "/@user/video/<id>"],
    },
    Platform {
        id: "vk",
        hosts: &["vk.com", "vkvideo.ru"],
        path_examples: &["/clip-123_456", "/video-123_456"],
    },
    Platform {
        id: "yappy",
        hosts: &["yappy.media"],
        path_examples: &["/video/<id>"],
    },
];

pub fn default_enabled_platforms() -> Vec<String> {
    PLATFORMS
        .iter()
        .map(|platform| platform.id.to_string())
        .collect()
}

pub fn validate_enabled_platforms(values: Vec<String>) -> anyhow::Result<Vec<String>> {
    let mut platforms = Vec::with_capacity(values.len());
    for value in values {
        let platform = value.trim().to_ascii_lowercase();
        if platform.is_empty() || platforms.contains(&platform) {
            continue;
        }
        if !is_known_platform(&platform) {
            return Err(anyhow::anyhow!(
                "unsupported platform `{platform}`; supported values are {}",
                known_platforms().join(", ")
            ));
        }
        platforms.push(platform);
    }
    Ok(platforms)
}

pub fn known_platforms() -> Vec<&'static str> {
    PLATFORMS.iter().map(|platform| platform.id).collect()
}

pub fn platform_definitions() -> &'static [Platform] {
    PLATFORMS
}

pub fn is_platform_enabled(platform: &str, enabled_platforms: &[String]) -> bool {
    enabled_platforms.iter().any(|enabled| enabled == platform)
}

pub fn platform_for_host(host: &str) -> Option<&'static str> {
    PLATFORMS
        .iter()
        .find(|platform| platform.hosts.iter().any(|known| host_matches(host, known)))
        .map(|platform| platform.id)
}

pub fn is_supported_video_url(platform: &str, url: &Url, host: &str) -> bool {
    let path = url.path();
    match platform {
        "tiktok" => is_tiktok_video_path(path, host),
        "instagram" => is_instagram_video_path(path),
        "youtube" => is_youtube_video_path(path, host),
        "facebook" => is_facebook_video_path(path, host),
        "snapchat" => path_starts_with_segment(path, "spotlight"),
        "douyin" | "yappy" => path_starts_with_segment(path, "video"),
        "rutube" => path_starts_with_segment(path, "shorts"),
        "likee" => is_likee_video_path(path),
        "vk" => is_vk_video_path(path),
        _ => false,
    }
}

fn is_tiktok_video_path(path: &str, host: &str) -> bool {
    path_contains_segment(path, "video")
        || !matches!(host, "tiktok.com" | "www.tiktok.com" | "m.tiktok.com")
}

fn is_instagram_video_path(path: &str) -> bool {
    path_starts_with_segment(path, "reel") || path_starts_with_segment(path, "reels")
}

fn is_youtube_video_path(path: &str, host: &str) -> bool {
    if host_matches(host, "youtu.be") {
        return path.trim_matches('/').len() >= 6;
    }
    path_starts_with_segment(path, "shorts")
}

fn is_facebook_video_path(path: &str, host: &str) -> bool {
    if host_matches(host, "fb.watch") {
        return !path.trim_matches('/').is_empty();
    }
    path_starts_with_segment(path, "reel")
        || path_starts_with_segment(path, "watch")
        || path.starts_with("/share/r/")
}

fn is_likee_video_path(path: &str) -> bool {
    path_starts_with_segment(path, "v") || path_contains_segment(path, "video")
}

fn is_vk_video_path(path: &str) -> bool {
    path_starts_with_segment(path, "clip") || path_starts_with_segment(path, "video")
}

fn is_known_platform(value: &str) -> bool {
    PLATFORMS.iter().any(|platform| platform.id == value)
}

fn host_matches(host: &str, supported: &str) -> bool {
    host == supported || host.ends_with(&format!(".{supported}"))
}

fn path_starts_with_segment(path: &str, segment: &str) -> bool {
    path.trim_start_matches('/')
        .split('/')
        .next()
        .is_some_and(|first| first == segment || first.starts_with(&format!("{segment}-")))
}

fn path_contains_segment(path: &str, segment: &str) -> bool {
    path.trim_matches('/')
        .split('/')
        .any(|part| part == segment)
}
