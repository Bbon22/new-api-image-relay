# Current Production Build Manifest

- Application image: `bon2/new-api:image-relay-speedfix-20261106`
- Image digest at deployment: `sha256:8a06f9c0479a7eecd1728e029a4700fd7e3c6be08f413a616d2cd867055fca2d`
- Source base commit: `5883a2b88bb4fd77122fc2d86c1b820d676c9140`
- Runtime source: this repository after the production source patch was applied
- Application: loopback-only `127.0.0.1:3000`
- Public reverse proxy: Nginx with TLS for `zmw.bon2.space`
- Database: PostgreSQL 15 Alpine
- Cache: Redis 7 Alpine

This records the reproducible build inputs. The live database, Redis state, user uploads, generated artifacts, logs, TLS certificates, and `.env` secrets are intentionally excluded.
