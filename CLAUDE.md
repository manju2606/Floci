# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Services

This project runs two Docker services via `docker-compose.yml`:

| Service | Image | Port |
|---------|-------|------|
| `floci` | `floci/floci:latest` | 4566 |
| `s3ui` | `cloudlena/s3manager` | 8888 |

`floci` is a local cloud storage emulator (similar to LocalStack). `s3manager` provides a web UI for browsing S3-compatible buckets at `http://localhost:8080`.

## Running the stack

```powershell
docker compose up -d       # start all services
docker compose down        # stop and remove containers
docker compose logs -f     # follow logs
```
