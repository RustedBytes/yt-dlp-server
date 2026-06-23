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
- X/Twitter video links: `x.com`, `twitter.com`, and subdomains such as `mobile.twitter.com`
