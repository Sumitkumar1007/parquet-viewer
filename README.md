# Parquet Viewer

Read-only parquet exploration app with:

- secure login page
- folder-based parquet selection
- schema inspection
- SQL query console
- browser-based result preview
- strict read-only query enforcement

This project is meant for safe parquet inspection. App blocks write-style SQL and only allows read-focused statements.

## Features

- Separate `/login` page with cookie-based session auth
- Main app at `/`
- Root-path selector for browsing parquet files in chosen folder
- Direct-folder scan only
  - app shows parquet files inside selected folder
  - app does not auto-scan nested subfolders
- Schema viewer for selected parquet file
- Query console against table alias `current_parquet`
- Preview button for quick row preview
- Read-only SQL guardrails

## Allowed SQL

Only these query starters are allowed:

- `SELECT`
- `WITH`
- `SHOW`
- `DESCRIBE`
- `EXPLAIN`

Blocked keywords include:

- `INSERT`
- `UPDATE`
- `DELETE`
- `DROP`
- `ALTER`
- `CREATE`
- `REPLACE`
- `TRUNCATE`
- `ATTACH`
- `DETACH`
- `COPY`
- `EXPORT`
- `INSTALL`
- `LOAD`
- `CALL`

Multiple statements are also blocked.

## Tech Stack

- FastAPI
- DuckDB
- Jinja2 templates
- Vanilla JavaScript
- Plain CSS

## Project Structure

```text
parquet_viewer/
├── app/
│   ├── core/
│   │   └── auth.py
│   ├── routes/
│   │   ├── api.py
│   │   └── web.py
│   ├── services/
│   │   └── query_engine.py
│   ├── static/
│   │   ├── app.js
│   │   ├── login.js
│   │   └── styles.css
│   ├── templates/
│   │   ├── app.html
│   │   └── login.html
│   ├── config.py
│   └── main.py
├── data/
├── scripts/
│   ├── generate_more_sample_parquets.py
│   └── generate_sample_parquet.py
├── .env.example
├── .gitignore
├── Dockerfile
├── README.md
└── requirements.txt
```

## Requirements

Python `3.12+` recommended.

Install packages from [requirements.txt](/home/ubuntu/aiml/parquet_viewer/requirements.txt:1):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment Variables

Copy env template:

```bash
cp .env.example .env
```

Available settings:

- `APP_NAME`
- `APP_ENV`
- `SECRET_KEY`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD_HASH`
- `PARQUET_PATH`
- `PARQUET_ROOT`
- `SESSION_TTL_MINUTES`
- `MAX_PREVIEW_ROWS`

Example file: [.env.example](/home/ubuntu/aiml/parquet_viewer/.env.example:1)

## Create Admin Password Hash

Generate password hash:

```bash
python3 -c "from app.core.auth import hash_password; print(hash_password('ChangeMe123!'))"
```

Paste output into `ADMIN_PASSWORD_HASH` inside `.env`.

## Generate Sample Parquet Files

Single sample:

```bash
.venv/bin/python scripts/generate_sample_parquet.py
```

Multiple samples:

```bash
.venv/bin/python scripts/generate_more_sample_parquets.py
```

Current sample files created in `data/`:

- `sample.parquet`
- `sales_q1.parquet`
- `inventory_snapshot.parquet`
- `customer_support.parquet`

## Run Locally

Start app:

```bash
.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8010
```

Open:

- `http://127.0.0.1:8010/login`

After login, app redirects to:

- `http://127.0.0.1:8010/`

## Login

Default local example:

- username: `admin`
- password: value used to generate your configured hash

Do not keep demo credentials in shared environments.

## How Root Path Works

Root path selection behavior:

- app scans only selected folder
- app lists only `.parquet` files directly inside selected folder
- if selected folder has no parquet files, UI shows none
- app clears previous file/schema/result state when folder is empty

Example:

- `/data/parquet` -> files in that exact folder only
- not recursive into deeper subfolders

## How Queries Work

Selected parquet file is exposed as:

```sql
current_parquet
```

Example queries:

```sql
SELECT * FROM current_parquet LIMIT 25;
```

```sql
SELECT region, SUM(revenue) AS total_revenue
FROM current_parquet
GROUP BY region
ORDER BY total_revenue DESC;
```

```sql
DESCRIBE current_parquet;
```

## API Overview

Important routes:

- `GET /login`
- `GET /`
- `GET /api/health`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`
- `GET /api/files`
- `GET /api/schema`
- `GET /api/preview`
- `POST /api/query`

## Security Notes

- session cookie is `HttpOnly`
- session token is signed
- password stored as PBKDF2 hash
- CSP and security headers added
- only read-style queries allowed

This is safer than open query execution, but still should sit behind proper network controls in real deployments.

## Docker

Build:

```bash
docker build -t parquet-viewer .
```

Run:

```bash
docker run --rm -p 8000:8000 --env-file .env parquet-viewer
```

## Troubleshooting

### No parquet files shown

Check:

- selected root path is correct
- folder contains `.parquet` files directly inside it
- browser hard refresh if old UI cached

### Old files still visible

Use hard refresh:

```text
Ctrl+Shift+R
```

Then reselect root path.

### Login fails

Check:

- `ADMIN_USERNAME`
- `ADMIN_PASSWORD_HASH`
- `.env` loaded correctly

### Query blocked

Expected if query contains write-style SQL or blocked keywords.

## Future Improvements

- real multi-user auth
- password change flow
- pagination for large results
- recursive scan toggle
- file upload with safety checks
- audit logging
- CSV export for query results
