# URL Input

Requests accept short-form social video URLs from supported platforms.

Validation rules:

- URLs must use `http` or `https`.
- URLs must not include credentials.
- Empty lines in the browser form are ignored.
- Exact normalized duplicates in one request are queued once.
- The whole request is rejected if any URL is invalid.
- The whole request is rejected if the number of non-empty URLs exceeds `download.max_urls_per_request`.

Supported hosts are:

- TikTok: `tiktok.com` and subdomains such as `www.tiktok.com`, `vm.tiktok.com`, `vt.tiktok.com`, and `m.tiktok.com`
- Instagram: `instagram.com` and subdomains such as `www.instagram.com`
- YouTube Shorts: `youtube.com`, `youtu.be`, and subdomains such as `www.youtube.com` and `m.youtube.com`
- Facebook Reels/watch links: `facebook.com`, `fb.watch`, and subdomains such as `www.facebook.com`
- Snapchat Spotlight: `snapchat.com` and subdomains such as `www.snapchat.com`
- RUTUBE Shorts: `rutube.ru` and subdomains
- Douyin short videos: `douyin.com` and subdomains such as `www.douyin.com`
- Likee short videos: `likee.video` and subdomains such as `www.likee.video`
- VK Clips: `vk.com`, `vkvideo.ru`, and subdomains such as `m.vk.com`
- Yappy vertical videos: `yappy.media` and subdomains
- XiaoHongShu / RedNote short-video posts: `xiaohongshu.com` and subdomains
- X/Twitter video links: `x.com`, `twitter.com`, and subdomains such as `mobile.twitter.com`

The allowlist is intentionally host-based. A supported host means the request may be queued; the actual download still depends on the current `yt-dlp` extractor behavior for that specific URL.
