# Current Site Build

This directory builds the application from this repository and runs the same New API service configuration used by the current site. PostgreSQL and Redis versions, task-artifact storage, health checks, loopback-only application port, and reverse-proxy trust settings follow the production deployment.

## Start a fresh instance

From the repository root:

```bash
cp deploy/current-site/.env.example deploy/current-site/.env
```

Replace the four secret placeholders with independent values from `openssl rand -hex 32`. Keep `SITE_ORIGIN`, `TASK_PUBLIC_ADDRESS`, and `TRUSTED_PROXIES` aligned with the domain and reverse proxy. Protect the environment file:

```bash
chmod 600 deploy/current-site/.env
docker compose --project-directory deploy/current-site --env-file deploy/current-site/.env -f deploy/current-site/compose.yaml config -q
docker compose --project-directory deploy/current-site --env-file deploy/current-site/.env -f deploy/current-site/compose.yaml up -d --build
```

The app listens only on `127.0.0.1:3000`; put an HTTPS reverse proxy in front of it. The current Nginx template is [`nginx/zmw.bon2.space.conf`](nginx/zmw.bon2.space.conf). Install the systemd backup units from [`systemd/`](systemd/) after reviewing the host paths. A proxy certificate and DNS are host-level resources and are not part of this repository.

For the documented `/opt/new-api` layout, install the backup timer with:

```bash
chmod 700 deploy/current-site/scripts/backup-postgres.sh
sudo cp deploy/current-site/systemd/*.service deploy/current-site/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now new-api-image-relay-backup.timer
```

## What is not in GitHub

This repository contains the app source and reproducible deployment configuration, not the live site state. User accounts, channels, API keys, system settings, usage records, PostgreSQL/Redis contents, generated images under `/data/task-artifacts`, logs, backups, and production secrets remain on the server and must not be committed. A new instance starts with an empty database and requires its own setup. To reproduce the live site state, transfer a verified database backup and required persistent files through a private, access-controlled channel, then configure matching secrets separately.
