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

Validation is platform-aware. A supported host is not enough by itself; the URL path must look like the platform's short-form video shape, such as YouTube `/shorts/...`, Instagram `/reel/...`, TikTok `/@user/video/...`, Snapchat `/spotlight/...`, RUTUBE `/shorts/...`, Douyin `/video/...`, Likee `/v/...` or `/.../video/...`, VK `/clip...` or `/video...`, and Yappy `/video/...`.

Broad or edge video platforms such as XiaoHongShu / RedNote, Bilibili, Ixigua / Xigua Video, Pinterest, and X/Twitter are not accepted by default because they are not dedicated Shorts/Reels-style products in the same sense as the supported list above.

Deployments can narrow the default set with `download.enabled_platforms` or `DOWNLOAD_ENABLED_PLATFORMS`. Supported platform IDs are `tiktok`, `instagram`, `youtube`, `facebook`, `snapchat`, `rutube`, `douyin`, `likee`, `vk`, and `yappy`.

Clients can discover this deployment's current platform list with:

```bash
curl http://127.0.0.1:3000/v1/platforms
```

Clients can preflight a pasted list without enqueueing jobs with:

```bash
curl -s -X POST http://127.0.0.1:3000/v1/downloads/validate \
  -H 'content-type: application/json' \
  -d '{"urls":["https://www.youtube.com/shorts/abc","https://example.com/video"]}'
```
