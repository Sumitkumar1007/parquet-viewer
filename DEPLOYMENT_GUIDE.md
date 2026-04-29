# Deployment Guide

This guide explains how to deploy `parquet-viewer` on another Ubuntu server without Docker or Kubernetes.

Recommended stack:

- GitHub repo as source
- Python virtual environment
- `systemd` service for app process
- optional `nginx` reverse proxy

## 1. Install System Packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git nginx
```

## 2. Clone Repository

Use SSH if GitHub SSH key is configured:

```bash
cd /opt
sudo git clone git@github.com:Sumitkumar1007/parquet-viewer.git
sudo chown -R $USER:$USER /opt/parquet-viewer
cd /opt/parquet-viewer
```

Or use HTTPS:

```bash
cd /opt
sudo git clone https://github.com/Sumitkumar1007/parquet-viewer.git
sudo chown -R $USER:$USER /opt/parquet-viewer
cd /opt/parquet-viewer
```

## 3. Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 4. Create Runtime Folders

```bash
mkdir -p runtime data
```

If parquet files live elsewhere, point `PARQUET_ROOT` and `PARQUET_PATH` to that location in `.env`.

If you plan to read from HDFS instead of local disk:

- `data/` is not required for HDFS-only usage
- you can set `PARQUET_ROOT` to an HDFS URI such as `hdfs://namenode:8020/user/data/parquet`
- file selection in the UI can also use an HDFS root path directly

## 5. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and set:

- `SECRET_KEY`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD_HASH`
- `PARQUET_ROOT`
- `PARQUET_PATH`
- optional pagination/upload settings

For HDFS deployments:

- `PARQUET_ROOT` can be an HDFS URI like `hdfs://namenode:8020/user/data/parquet`
- `PARQUET_PATH` can be a specific HDFS parquet file like `hdfs://namenode:8020/user/data/parquet/sample.parquet`

Generate password hash:

```bash
source .venv/bin/activate
python3 -c "from app.core.auth import hash_password; print(hash_password('YourStrongPassword123'))"
```

Paste output into:

```text
ADMIN_PASSWORD_HASH=...
```

## 5A. HDFS Requirements

If you will query parquet files from HDFS, make sure this server also has:

- `pyarrow` installed from [requirements.txt](/home/ubuntu/aiml/parquet_viewer/requirements.txt:1)
- network access to the HDFS namenode
- working HDFS client/native libraries required by PyArrow in your environment
- permission to read the target HDFS directories

Example HDFS values in `.env`:

```text
PARQUET_ROOT=hdfs://namenode:8020/user/warehouse/parquet
PARQUET_PATH=hdfs://namenode:8020/user/warehouse/parquet/sample.parquet
```

If HDFS access is not configured correctly, file listing or query execution will fail even though the app itself is running.

## 6. Create Additional Users

App supports file-based multi-user auth.

Create or update users:

```bash
source .venv/bin/activate
python3 scripts/create_user.py alice StrongPass123 --role viewer
python3 scripts/create_user.py bob StrongPass456 --role admin
```

This writes:

```text
runtime/users.json
```

## 7. Test Manually

```bash
source .venv/bin/activate
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8010
```

Open:

- `http://SERVER_IP:8010/login`

Stop after test with `Ctrl+C`.

## 8. Create systemd Service

Create service file:

```bash
sudo nano /etc/systemd/system/parquet-viewer.service
```

Paste:

```ini
[Unit]
Description=Parquet Viewer
After=network.target

[Service]
User=ubuntu
Group=ubuntu
WorkingDirectory=/opt/parquet-viewer
Environment="PATH=/opt/parquet-viewer/.venv/bin"
ExecStart=/opt/parquet-viewer/.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8010
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Load and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable parquet-viewer
sudo systemctl start parquet-viewer
sudo systemctl status parquet-viewer
```

## 9. View Logs

```bash
sudo journalctl -u parquet-viewer -f
```

App audit log file:

```text
runtime/audit.log
```

## 10. Optional Nginx Reverse Proxy

Create config:

```bash
sudo nano /etc/nginx/sites-available/parquet-viewer
```

Paste:

```nginx
server {
    listen 80;
    server_name your-domain-or-server-ip;

    location / {
        proxy_pass http://127.0.0.1:8010;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable:

```bash
sudo ln -s /etc/nginx/sites-available/parquet-viewer /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 11. Continuous Development Flow

You said you will keep developing on `main`.

Each time you push new changes to GitHub, run this on server:

```bash
cd /opt/parquet-viewer
git pull origin main
source .venv/bin/activate
pip install -r requirements.txt
python3 -m compileall app scripts
sudo systemctl restart parquet-viewer
sudo systemctl status parquet-viewer --no-pager
```

## 12. Recommended Update Script

Create deploy helper:

```bash
nano /opt/parquet-viewer/deploy.sh
```

Paste:

```bash
#!/bin/bash
set -e

cd /opt/parquet-viewer
git pull origin main
source .venv/bin/activate
pip install -r requirements.txt
python3 -m compileall app scripts
sudo systemctl restart parquet-viewer
sudo systemctl status parquet-viewer --no-pager
```

Make executable:

```bash
chmod +x /opt/parquet-viewer/deploy.sh
```

Run updates later with:

```bash
/opt/parquet-viewer/deploy.sh
```

## 13. Important Files Not To Replace

Do not overwrite these on server:

- `.env`
- `runtime/users.json`
- `runtime/audit.log`
- private parquet datasets

## 14. Security Notes

- keep `.env` only on server
- use strong `SECRET_KEY`
- use strong passwords
- prefer SSH clone/pull over HTTPS password flow
- use `nginx` + TLS in production
- keep private parquet files outside repo

## 15. Troubleshooting

### App not starting

```bash
sudo systemctl status parquet-viewer
sudo journalctl -u parquet-viewer -n 100 --no-pager
```

### Dependency issue after pull

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### Login fails

Check:

- `.env`
- `runtime/users.json`
- password hash

### No parquet files shown

Check:

- `PARQUET_ROOT`
- `PARQUET_PATH`
- selected folder in UI
- folder really contains `.parquet` files

### Port already busy

Find process:

```bash
ss -ltnp | grep 8010
```

Then stop conflicting service or change port in `systemd` config.
