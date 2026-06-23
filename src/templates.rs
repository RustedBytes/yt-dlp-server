use askama::Template;

use crate::types::QueueResponse;

#[derive(Template)]
#[template(
    source = r###"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Social Video Downloader</title>
  <style>
    :root {
      color-scheme: light;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f6f7f9;
      color: #1f2933;
    }
    body {
      margin: 0;
      padding: 32px;
    }
    main {
      max-width: 760px;
      margin: 0 auto;
      background: #ffffff;
      border: 1px solid #d9dee7;
      border-radius: 8px;
      padding: 28px;
      box-shadow: 0 10px 30px rgba(31, 41, 51, 0.08);
    }
    h1 {
      margin: 0 0 6px;
      font-size: 24px;
      line-height: 1.2;
    }
    p {
      margin: 0 0 24px;
      color: #52606d;
    }
    form {
      display: grid;
      gap: 18px;
    }
    label {
      display: block;
      margin-bottom: 8px;
      font-weight: 650;
      color: #323f4b;
    }
    input[type="url"],
    textarea {
      box-sizing: border-box;
      width: 100%;
      border: 1px solid #cbd2d9;
      border-radius: 6px;
      padding: 10px 12px;
      font: inherit;
      background: #ffffff;
    }
    textarea {
      min-height: 180px;
      resize: vertical;
    }
    button {
      justify-self: start;
      border: 0;
      border-radius: 6px;
      padding: 10px 16px;
      font: inherit;
      font-weight: 700;
      color: #ffffff;
      background: #2563eb;
      cursor: pointer;
    }
    button:hover {
      background: #1d4ed8;
    }
    .notice,
    .error {
      margin-top: 22px;
      padding: 14px 16px;
      border-radius: 6px;
    }
    .notice {
      border: 1px solid #b7d7c0;
      background: #edf8f0;
      color: #1f5130;
    }
    .error {
      border: 1px solid #f3b5b5;
      background: #fff1f1;
      color: #8a1f1f;
    }
    ul {
      margin: 10px 0 0;
      padding-left: 20px;
    }
    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.95em;
    }
  </style>
</head>
<body>
  <main>
    <h1>Social Video Downloader</h1>
    <p>Paste short-form social video URLs and queue them for download.</p>

    <form method="post" action="/downloads-form">
      <div>
        <label for="urls">URLs</label>
        <textarea id="urls" name="urls" placeholder="https://www.tiktok.com/@user/video/123&#10;https://www.instagram.com/reel/ABC/&#10;https://www.youtube.com/shorts/XYZ" required></textarea>
      </div>

      <div>
        <label for="webhook_url">Webhook URL (optional)</label>
        <input id="webhook_url" name="webhook_url" type="url" placeholder="https://example.com/download-webhook">
      </div>

      <button type="submit">Queue downloads</button>
    </form>

    {% if queued_jobs.len() > 0 %}
    <div class="notice">
      Queued {{ queued_jobs.len() }} job{% if queued_jobs.len() != 1 %}s{% endif %}:
      <ul>
      {% for job in queued_jobs %}
        <li><code>{{ job.id }}</code> - <a href="{{ job.status_url }}">{{ job.status_url }}</a></li>
      {% endfor %}
      </ul>
    </div>
    {% endif %}

    {% if error != "" %}
    <div class="error">{{ error }}</div>
    {% endif %}
  </main>
</body>
</html>
"###,
    ext = "html"
)]
pub struct IndexTemplate {
    pub queued_jobs: Vec<QueueResponse>,
    pub error: String,
}
