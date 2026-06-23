use askama::Template;

use crate::types::{JobRecord, QueueResponse};

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
    .action-form {
      display: inline;
    }
    label {
      display: block;
      margin-bottom: 8px;
      font-weight: 650;
      color: #323f4b;
    }
    input[type="url"],
    input[type="text"],
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
    .action-button {
      margin-left: 8px;
      border: 1px solid #cbd2d9;
      padding: 4px 8px;
      font-size: 12px;
      font-weight: 650;
      color: #1f2933;
      background: #ffffff;
    }
    .action-button:hover {
      background: #f5f7fa;
    }
    .action-button.danger {
      border-color: #f3b5b5;
      color: #8a1f1f;
    }
    .action-button.danger:hover {
      background: #fff1f1;
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
    table {
      width: 100%;
      margin-top: 26px;
      border-collapse: collapse;
      font-size: 14px;
    }
    th,
    td {
      border-top: 1px solid #e4e7eb;
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      color: #52606d;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .url-cell {
      max-width: 360px;
      overflow-wrap: anywhere;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 10px;
      margin: 18px 0;
    }
    .summary-item {
      border: 1px solid #d9e2ec;
      background: #fff;
      border-radius: 6px;
      padding: 10px 12px;
    }
    .summary-item strong {
      display: block;
      font-size: 20px;
      line-height: 1.2;
    }
    .summary-item span {
      color: #52606d;
      font-size: 12px;
      text-transform: uppercase;
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
    <p><a href="/jobs">Job history</a> · <a href="/v1/workers">Workers</a> · <a href="/webhooks/dead-letters">Webhook dead letters</a></p>

    <div class="summary-grid" aria-label="Current server summary">
      <div class="summary-item"><strong>{{ total_jobs }}</strong><span>Total</span></div>
      <div class="summary-item"><strong>{{ queued_count }}</strong><span>Queued</span></div>
      <div class="summary-item"><strong>{{ running_count }}</strong><span>Running</span></div>
      <div class="summary-item"><strong>{{ succeeded_count }}</strong><span>Succeeded</span></div>
      <div class="summary-item"><strong>{{ failed_count }}</strong><span>Failed</span></div>
      <div class="summary-item"><strong>{{ canceled_count }}</strong><span>Canceled</span></div>
      <div class="summary-item"><strong>{{ deleted_count }}</strong><span>Deleted</span></div>
      <div class="summary-item"><strong>{{ queue_available_slots }}/{{ queue_capacity }}</strong><span>Queue slots</span></div>
      <div class="summary-item"><strong>{{ workers_ready }}/{{ workers_expected }}</strong><span>Workers</span></div>
      <div class="summary-item"><strong>{{ active_workers }}</strong><span>Active</span></div>
    </div>

    <form method="post" action="/downloads-form">
      <div>
        <label for="urls">URLs</label>
        <textarea id="urls" name="urls" placeholder="https://www.tiktok.com/@user/video/123&#10;https://www.instagram.com/reel/ABC/&#10;https://rutube.ru/shorts/XYZ/" required></textarea>
      </div>

      <div>
        <label for="webhook_url">Webhook URL (optional)</label>
        <input id="webhook_url" name="webhook_url" type="url" placeholder="https://example.com/download-webhook">
      </div>

      <div>
        <label for="format">yt-dlp format (optional)</label>
        <input id="format" name="format" type="text" placeholder="bv*+ba/b or mp4/best">
      </div>

      <div>
        <label for="cookie_profile">Cookie profile (optional)</label>
        <input id="cookie_profile" name="cookie_profile" type="text" placeholder="account_a">
      </div>

      <div>
        <label><input name="force" type="checkbox" value="true"> Redownload existing successful URLs</label>
      </div>

      <button type="submit">Queue downloads</button>
    </form>

    {% if queued_jobs.len() > 0 %}
    <div class="notice">
      Queued {{ queued_jobs.len() }} job{% if queued_jobs.len() != 1 %}s{% endif %}:
      <ul>
      {% for job in queued_jobs %}
        <li><code>{{ job.id }}</code> - <a href="{{ job.status_url }}">{{ job.status_url }}</a>{% if job.existing %} (existing){% endif %}</li>
      {% endfor %}
      </ul>
    </div>
    {% endif %}

    {% if notice != "" %}
    <div class="notice">{{ notice }}</div>
    {% endif %}

    {% if error != "" %}
    <div class="error">{{ error }}</div>
    {% endif %}

    {% if recent_jobs.len() > 0 %}
    <table>
      <thead>
        <tr>
          <th>Job</th>
          <th>Status</th>
          <th>URL</th>
          <th>Links</th>
        </tr>
      </thead>
      <tbody>
      {% for job in recent_jobs %}
        <tr>
          <td><code>{{ job.id }}</code></td>
          <td>{{ job.status }}</td>
          <td class="url-cell">{{ job.url }}</td>
          <td>
            <a href="/jobs/{{ job.id }}">details</a>
            · <a href="/v1/jobs/{{ job.id }}">json</a>
            {% if job.result.is_some() %}
            · <a href="/v1/jobs/{{ job.id }}/media">media</a>
            · <a href="/v1/jobs/{{ job.id }}/info-json">info</a>
            · <a href="/v1/jobs/{{ job.id }}/archive">archive</a>
            {% endif %}
            {% if job.status.is_cancelable() %}
            <form class="action-form" method="post" action="/jobs-form/{{ job.id }}/cancel">
              <button class="action-button danger" type="submit">Cancel</button>
            </form>
            {% endif %}
            {% if job.status.is_retryable() %}
            <form class="action-form" method="post" action="/jobs-form/{{ job.id }}/retry">
              <button class="action-button" type="submit">Retry</button>
            </form>
            {% endif %}
            {% if job.status.is_deletable() %}
            <form class="action-form" method="post" action="/jobs-form/{{ job.id }}/delete">
              <button class="action-button danger" type="submit">Delete</button>
            </form>
            {% endif %}
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% endif %}
  </main>
  {% if has_active_jobs %}
  <script>
    window.setTimeout(() => window.location.reload(), 5000);
  </script>
  {% endif %}
</body>
</html>
"###,
    ext = "html"
)]
pub struct IndexTemplate {
    pub queued_jobs: Vec<QueueResponse>,
    pub notice: String,
    pub has_active_jobs: bool,
    pub recent_jobs: Vec<JobRecord>,
    pub error: String,
    pub total_jobs: usize,
    pub queued_count: usize,
    pub running_count: usize,
    pub succeeded_count: usize,
    pub failed_count: usize,
    pub canceled_count: usize,
    pub deleted_count: usize,
    pub queue_capacity: usize,
    pub queue_available_slots: usize,
    pub workers_ready: usize,
    pub workers_expected: usize,
    pub active_workers: usize,
}

#[derive(Template)]
#[template(
    source = r###"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Job History - Social Video Downloader</title>
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
      max-width: 980px;
      margin: 0 auto;
      background: #ffffff;
      border: 1px solid #d9dee7;
      border-radius: 8px;
      padding: 28px;
      box-shadow: 0 10px 30px rgba(31, 41, 51, 0.08);
    }
    header {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0;
      font-size: 24px;
      line-height: 1.2;
    }
    a {
      color: #1d4ed8;
    }
    form.filters {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: end;
      margin: 18px 0;
    }
    label {
      display: block;
      margin-bottom: 6px;
      color: #52606d;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    select,
    input[type="number"] {
      border: 1px solid #cbd2d9;
      border-radius: 6px;
      padding: 8px 10px;
      font: inherit;
      background: #ffffff;
    }
    button {
      border: 1px solid #cbd2d9;
      border-radius: 6px;
      padding: 8px 12px;
      font: inherit;
      font-weight: 650;
      color: #1f2933;
      background: #ffffff;
      cursor: pointer;
    }
    button:hover {
      background: #f5f7fa;
    }
    table {
      width: 100%;
      margin-top: 18px;
      border-collapse: collapse;
      font-size: 14px;
    }
    th,
    td {
      border-top: 1px solid #e4e7eb;
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      color: #52606d;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .url-cell {
      max-width: 360px;
      overflow-wrap: anywhere;
    }
    .pager {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      margin-top: 18px;
    }
    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.95em;
    }
    @media (max-width: 720px) {
      body {
        padding: 16px;
      }
      main {
        padding: 20px;
      }
      table,
      thead,
      tbody,
      tr,
      th,
      td {
        display: block;
      }
      th {
        display: none;
      }
      td {
        border-top: 0;
        padding: 6px 0;
      }
      tr {
        border-top: 1px solid #e4e7eb;
        padding: 10px 0;
      }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Job History</h1>
      <nav><a href="/">Queue</a> · <a href="/v1/jobs">JSON</a></nav>
    </header>

    <form class="filters" method="get" action="/jobs">
      <div>
        <label for="status">Status</label>
        <select id="status" name="status">
          <option value=""{% if status == "" %} selected{% endif %}>All</option>
          <option value="queued"{% if status == "queued" %} selected{% endif %}>Queued</option>
          <option value="running"{% if status == "running" %} selected{% endif %}>Running</option>
          <option value="succeeded"{% if status == "succeeded" %} selected{% endif %}>Succeeded</option>
          <option value="failed"{% if status == "failed" %} selected{% endif %}>Failed</option>
          <option value="canceled"{% if status == "canceled" %} selected{% endif %}>Canceled</option>
          <option value="deleted"{% if status == "deleted" %} selected{% endif %}>Deleted</option>
        </select>
      </div>
      <div>
        <label for="platform">Platform</label>
        <select id="platform" name="platform">
          <option value=""{% if platform == "" %} selected{% endif %}>All</option>
          {% for option in platform_options %}
          <option value="{{ option.id }}"{% if option.selected %} selected{% endif %}>{{ option.id }}</option>
          {% endfor %}
        </select>
      </div>
      <div>
        <label for="q">Search</label>
        <input id="q" name="q" type="search" value="{{ query }}" placeholder="title, uploader, URL, error">
      </div>
      <div>
        <label for="limit">Limit</label>
        <input id="limit" name="limit" type="number" min="1" max="500" value="{{ limit }}">
      </div>
      <input name="offset" type="hidden" value="0">
      <button type="submit">Apply</button>
    </form>

    <p>Showing {{ shown_start }}-{{ shown_end }} of {{ total }} job{% if total != 1 %}s{% endif %}.</p>

    {% if jobs.len() > 0 %}
    <table>
      <thead>
        <tr>
          <th>Job</th>
          <th>Status</th>
          <th>Platform</th>
          <th>URL</th>
          <th>Updated</th>
          <th>Links</th>
        </tr>
      </thead>
      <tbody>
      {% for job in jobs %}
        <tr>
          <td><code>{{ job.id }}</code></td>
          <td>{{ job.status }}</td>
          <td>{{ job.platform }}</td>
          <td class="url-cell">{{ job.url }}</td>
          <td>{{ job.updated_at }}</td>
          <td>
            <a href="/jobs/{{ job.id }}">details</a>
            · <a href="/v1/jobs/{{ job.id }}">json</a>
            {% if job.has_media %}
            · <a href="/v1/jobs/{{ job.id }}/media">media</a>
            · <a href="/v1/jobs/{{ job.id }}/info-json">info</a>
            · <a href="/v1/jobs/{{ job.id }}/archive">archive</a>
            {% endif %}
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}
    <p>No jobs match this filter.</p>
    {% endif %}

    <div class="pager">
      {% if has_previous %}
      <a href="{{ previous_url }}">Previous</a>
      {% endif %}
      {% if has_next %}
      <a href="{{ next_url }}">Next</a>
      {% endif %}
    </div>
  </main>
</body>
</html>
"###,
    ext = "html"
)]
pub struct JobListTemplate {
    pub jobs: Vec<JobListItemView>,
    pub status: String,
    pub platform: String,
    pub platform_options: Vec<PlatformFilterOption>,
    pub query: String,
    pub total: usize,
    pub limit: usize,
    pub shown_start: usize,
    pub shown_end: usize,
    pub has_previous: bool,
    pub previous_url: String,
    pub has_next: bool,
    pub next_url: String,
}

pub struct PlatformFilterOption {
    pub id: &'static str,
    pub selected: bool,
}

pub struct JobListItemView {
    pub id: String,
    pub status: String,
    pub platform: String,
    pub url: String,
    pub updated_at: String,
    pub has_media: bool,
}

#[derive(Template)]
#[template(
    source = r###"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Job {{ job.id }} - Social Video Downloader</title>
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
      max-width: 920px;
      margin: 0 auto;
      background: #ffffff;
      border: 1px solid #d9dee7;
      border-radius: 8px;
      padding: 28px;
      box-shadow: 0 10px 30px rgba(31, 41, 51, 0.08);
    }
    header {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 22px;
    }
    h1 {
      margin: 0;
      font-size: 22px;
      line-height: 1.25;
      overflow-wrap: anywhere;
    }
    h2 {
      margin: 28px 0 10px;
      font-size: 16px;
      line-height: 1.3;
    }
    a {
      color: #1d4ed8;
    }
    .nav {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      font-size: 14px;
    }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin: 18px 0 4px;
    }
    .action-form {
      display: inline;
    }
    button {
      border: 1px solid #cbd2d9;
      border-radius: 6px;
      padding: 6px 10px;
      font: inherit;
      font-size: 13px;
      font-weight: 650;
      color: #1f2933;
      background: #ffffff;
      cursor: pointer;
    }
    button:hover {
      background: #f5f7fa;
    }
    button.danger {
      border-color: #f3b5b5;
      color: #8a1f1f;
    }
    button.danger:hover {
      background: #fff1f1;
    }
    dl {
      display: grid;
      grid-template-columns: minmax(130px, 190px) minmax(0, 1fr);
      gap: 10px 18px;
      margin: 0;
    }
    dt {
      color: #52606d;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    dd {
      margin: 0;
      overflow-wrap: anywhere;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th,
    td {
      border-top: 1px solid #e4e7eb;
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      color: #52606d;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .status {
      display: inline-block;
      border: 1px solid #cbd2d9;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 13px;
      font-weight: 700;
      background: #f8fafc;
    }
    .notice {
      border: 1px solid #b7d7c0;
      border-radius: 6px;
      padding: 12px 14px;
      background: #edf8f0;
      color: #1f5130;
      overflow-wrap: anywhere;
    }
    .error {
      border: 1px solid #f3b5b5;
      border-radius: 6px;
      padding: 12px 14px;
      background: #fff1f1;
      color: #8a1f1f;
      overflow-wrap: anywhere;
    }
    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.95em;
    }
    @media (max-width: 640px) {
      body {
        padding: 16px;
      }
      main {
        padding: 20px;
      }
      dl {
        grid-template-columns: 1fr;
        gap: 4px 0;
      }
      dt {
        margin-top: 10px;
      }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1><code>{{ job.id }}</code></h1>
      <nav class="nav">
        <a href="/">Queue</a>
        <a href="/v1/jobs/{{ job.id }}">JSON</a>
        {% if job.has_result %}
        <a href="/v1/jobs/{{ job.id }}/media">Media</a>
        <a href="/v1/jobs/{{ job.id }}/info-json">Info JSON</a>
        <a href="/v1/jobs/{{ job.id }}/archive">Archive</a>
        {% endif %}
      </nav>
    </header>

    {% if notice != "" %}
    <div class="notice">{{ notice }}</div>
    {% endif %}

    {% if error != "" %}
    <div class="error">{{ error }}</div>
    {% endif %}

    <dl>
      <dt>Status</dt>
      <dd><span class="status">{{ job.status }}</span></dd>
      <dt>URL</dt>
      <dd><a href="{{ job.url }}" rel="noreferrer">{{ job.url }}</a></dd>
      {% if job.has_format %}
      <dt>Format</dt>
      <dd><code>{{ job.format }}</code></dd>
      {% endif %}
      {% if job.has_cookie_profile %}
      <dt>Cookie profile</dt>
      <dd><code>{{ job.cookie_profile }}</code></dd>
      {% endif %}
      {% if job.has_webhook %}
      <dt>Webhook</dt>
      <dd>{{ job.webhook_url }}</dd>
      {% endif %}
      <dt>Created</dt>
      <dd>{{ job.created_at }}</dd>
      <dt>Updated</dt>
      <dd>{{ job.updated_at }}</dd>
      <dt>Attempts</dt>
      <dd>{{ job.attempts }}</dd>
    </dl>

    <div class="actions">
      {% if job.can_cancel %}
      <form class="action-form" method="post" action="/jobs-form/{{ job.id }}/cancel">
        <button class="danger" type="submit">Cancel</button>
      </form>
      {% endif %}
      {% if job.can_retry %}
      <form class="action-form" method="post" action="/jobs-form/{{ job.id }}/retry">
        <button type="submit">Retry</button>
      </form>
      {% endif %}
      {% if job.can_delete %}
      <form class="action-form" method="post" action="/jobs-form/{{ job.id }}/delete">
        <button class="danger" type="submit">Delete</button>
      </form>
      {% endif %}
      {% if job.can_redeliver_webhook %}
      <form class="action-form" method="post" action="/jobs-form/{{ job.id }}/webhook">
        <button type="submit">Redeliver webhook</button>
      </form>
      {% endif %}
    </div>

    {% if job.has_error %}
    <h2>Error</h2>
    <div class="error">
      {% if job.has_error_kind %}<strong>{{ job.error_kind }}</strong>: {% endif %}{{ job.error }}
    </div>
    {% endif %}

    {% if job.has_result %}
    {% if job.result.can_preview %}
    <h2>Preview</h2>
    <video controls preload="metadata" style="width: 100%; max-height: 560px; background: #111827; border-radius: 6px;">
      <source src="{{ job.result.preview_url }}" type="{{ job.result.media_content_type }}">
    </video>
    {% endif %}

    <h2>Result</h2>
    <dl>
      <dt>Original URL</dt>
      <dd>{{ job.result.original_url }}</dd>
      {% if job.result.has_webpage_url %}
      <dt>Final URL</dt>
      <dd>{{ job.result.webpage_url }}</dd>
      {% endif %}
      {% if job.result.has_title %}
      <dt>Title</dt>
      <dd>{{ job.result.title }}</dd>
      {% endif %}
      {% if job.result.has_uploader %}
      <dt>Uploader</dt>
      <dd>{{ job.result.uploader }}</dd>
      {% endif %}
      {% if job.result.has_extractor %}
      <dt>Extractor</dt>
      <dd>{{ job.result.extractor }}</dd>
      {% endif %}
      {% if job.result.has_duration %}
      <dt>Duration</dt>
      <dd>{{ job.result.duration }}</dd>
      {% endif %}
      {% if job.result.has_extension %}
      <dt>Extension</dt>
      <dd>{{ job.result.extension }}</dd>
      {% endif %}
      <dt>Preview URL</dt>
      <dd><a href="{{ job.result.preview_url }}">{{ job.result.preview_url }}</a></dd>
      <dt>Media bytes</dt>
      <dd>{{ job.result.media_bytes }}</dd>
      {% if job.result.has_media_sha256 %}
      <dt>Media SHA-256</dt>
      <dd><code>{{ job.result.media_sha256 }}</code></dd>
      {% endif %}
      <dt>Media path</dt>
      <dd><code>{{ job.result.media_path }}</code></dd>
      {% if job.result.has_info_json_sha256 %}
      <dt>Info JSON SHA-256</dt>
      <dd><code>{{ job.result.info_json_sha256 }}</code></dd>
      {% endif %}
      <dt>Info JSON path</dt>
      <dd><code>{{ job.result.info_json_path }}</code></dd>
      {% if job.result.has_archive_metadata %}
      <dt>Archive bytes</dt>
      <dd>{{ job.result.archive_bytes }}</dd>
      <dt>Archive SHA-256</dt>
      <dd><code>{{ job.result.archive_sha256 }}</code></dd>
      {% endif %}
      <dt>yt-dlp version</dt>
      <dd>{{ job.result.yt_dlp_version }}</dd>
      <dt>Elapsed</dt>
      <dd>{{ job.result.elapsed_ms }} ms</dd>
    </dl>
    {% endif %}

    {% if job.attempt_errors.len() > 0 %}
    <h2>Attempt Errors</h2>
    <table>
      <thead>
        <tr>
          <th>Attempt</th>
          <th>Error</th>
          <th>Elapsed</th>
          <th>Backoff</th>
        </tr>
      </thead>
      <tbody>
      {% for attempt in job.attempt_errors %}
        <tr>
          <td>{{ attempt.attempt }}</td>
          <td>{{ attempt.error }}</td>
          <td>{{ attempt.elapsed_ms }} ms</td>
          <td>{{ attempt.retry_backoff_ms }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% endif %}
  </main>
  {% if job.is_active %}
  <script>
    window.setTimeout(() => window.location.reload(), 5000);
  </script>
  {% endif %}
</body>
</html>
"###,
    ext = "html"
)]
pub struct JobDetailTemplate {
    pub job: JobDetailView,
    pub notice: String,
    pub error: String,
}

pub struct JobDetailView {
    pub id: String,
    pub status: String,
    pub url: String,
    pub has_format: bool,
    pub format: String,
    pub has_cookie_profile: bool,
    pub cookie_profile: String,
    pub has_webhook: bool,
    pub webhook_url: String,
    pub created_at: String,
    pub updated_at: String,
    pub attempts: usize,
    pub has_error_kind: bool,
    pub error_kind: String,
    pub has_error: bool,
    pub error: String,
    pub has_result: bool,
    pub result: JobResultView,
    pub attempt_errors: Vec<JobAttemptView>,
    pub can_cancel: bool,
    pub can_retry: bool,
    pub can_delete: bool,
    pub can_redeliver_webhook: bool,
    pub is_active: bool,
}

#[derive(Default)]
pub struct JobResultView {
    pub original_url: String,
    pub has_webpage_url: bool,
    pub webpage_url: String,
    pub has_extractor: bool,
    pub extractor: String,
    pub has_title: bool,
    pub title: String,
    pub has_uploader: bool,
    pub uploader: String,
    pub has_duration: bool,
    pub duration: String,
    pub has_extension: bool,
    pub extension: String,
    pub can_preview: bool,
    pub preview_url: String,
    pub media_content_type: String,
    pub media_path: String,
    pub media_bytes: u64,
    pub has_media_sha256: bool,
    pub media_sha256: String,
    pub info_json_path: String,
    pub has_info_json_sha256: bool,
    pub info_json_sha256: String,
    pub has_archive_metadata: bool,
    pub archive_bytes: u64,
    pub archive_sha256: String,
    pub yt_dlp_version: String,
    pub elapsed_ms: u128,
}

pub struct JobAttemptView {
    pub attempt: usize,
    pub error: String,
    pub elapsed_ms: u128,
    pub retry_backoff_ms: String,
}

#[derive(Template)]
#[template(
    source = r###"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Webhook Dead Letters - Social Video Downloader</title>
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
      max-width: 980px;
      margin: 0 auto;
      background: #ffffff;
      border: 1px solid #d9dee7;
      border-radius: 8px;
      padding: 28px;
      box-shadow: 0 10px 30px rgba(31, 41, 51, 0.08);
    }
    header {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 20px;
    }
    h1 {
      margin: 0;
      font-size: 24px;
      line-height: 1.2;
    }
    a {
      color: #1d4ed8;
    }
    table {
      width: 100%;
      margin-top: 22px;
      border-collapse: collapse;
      font-size: 14px;
    }
    th,
    td {
      border-top: 1px solid #e4e7eb;
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      color: #52606d;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .url-cell,
    .error-cell {
      max-width: 300px;
      overflow-wrap: anywhere;
    }
    .action-form {
      display: inline;
    }
    button {
      border: 1px solid #cbd2d9;
      border-radius: 6px;
      padding: 6px 10px;
      font: inherit;
      font-size: 13px;
      font-weight: 650;
      color: #1f2933;
      background: #ffffff;
      cursor: pointer;
    }
    button:hover {
      background: #f5f7fa;
    }
    .notice,
    .error {
      margin-top: 18px;
      padding: 12px 14px;
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
    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.95em;
    }
    @media (max-width: 720px) {
      body {
        padding: 16px;
      }
      main {
        padding: 20px;
      }
      table,
      thead,
      tbody,
      tr,
      th,
      td {
        display: block;
      }
      th {
        display: none;
      }
      td {
        border-top: 0;
        padding: 6px 0;
      }
      tr {
        border-top: 1px solid #e4e7eb;
        padding: 10px 0;
      }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Webhook Dead Letters</h1>
      <nav><a href="/">Queue</a> · <a href="/v1/webhooks/dead-letters">JSON</a></nav>
    </header>

    {% if notice != "" %}
    <div class="notice">{{ notice }}</div>
    {% endif %}

    {% if error != "" %}
    <div class="error">{{ error }}</div>
    {% endif %}

    {% if dead_letters.len() > 0 %}
    <table>
      <thead>
        <tr>
          <th>Event</th>
          <th>Job</th>
          <th>Webhook</th>
          <th>Failed</th>
          <th>Error</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody>
      {% for dead_letter in dead_letters %}
        <tr>
          <td><code>{{ dead_letter.event_id }}</code><br>{{ dead_letter.event_type }}</td>
          <td><a href="/jobs/{{ dead_letter.job_id }}"><code>{{ dead_letter.job_id }}</code></a><br>{{ dead_letter.job_status }}</td>
          <td class="url-cell">{{ dead_letter.webhook_url }}</td>
          <td>{{ dead_letter.failed_at }}<br>{{ dead_letter.attempts }} attempt{% if dead_letter.attempts != 1 %}s{% endif %}</td>
          <td class="error-cell">{{ dead_letter.error }}</td>
          <td>
            <form class="action-form" method="post" action="/webhooks/dead-letters/{{ dead_letter.event_id }}/replay">
              <button type="submit">Replay</button>
            </form>
            <form class="action-form" method="post" action="/webhooks/dead-letters/{{ dead_letter.event_id }}/dismiss">
              <button type="submit">Dismiss</button>
            </form>
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}
    <p>No webhook dead letters.</p>
    {% endif %}
  </main>
</body>
</html>
"###,
    ext = "html"
)]
pub struct DeadLettersTemplate {
    pub dead_letters: Vec<DeadLetterView>,
    pub notice: String,
    pub error: String,
}

pub struct DeadLetterView {
    pub event_id: String,
    pub event_type: String,
    pub job_id: String,
    pub job_status: String,
    pub webhook_url: String,
    pub failed_at: String,
    pub attempts: usize,
    pub error: String,
}
