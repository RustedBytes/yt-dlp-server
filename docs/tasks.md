# URL Input

Requests accept TikTok and Instagram video URLs.

Validation rules:

- URLs must use `http` or `https`.
- URLs must not include credentials.
- Empty lines in the browser form are ignored.
- Exact normalized duplicates in one request are queued once.
- The whole request is rejected if any URL is invalid.
- The whole request is rejected if the number of non-empty URLs exceeds `download.max_urls_per_request`.

Supported hosts are `tiktok.com`, `instagram.com`, and their subdomains such as `www.tiktok.com`, `vm.tiktok.com`, `vt.tiktok.com`, `m.tiktok.com`, and `www.instagram.com`.
