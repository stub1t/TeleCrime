# Telecrime — Stealer Log Pipeline

A pipeline for processing stealer logs from Telegram channels. It ingests conversations, discovers archive files, downloads them with crash-safe resume, extracts credential files with password inference, parses credentials into searchable structured records, and stores everything in PostgreSQL.

> **⚠️ Important**: This tool processes credential data of potentially criminal origin. Before using it, ensure you have legal authority and a legitimate purpose (incident response, law enforcement, security research). The authors are not responsible for misuse. **Never publish credential data.** The `channels.txt` / `channels.md` lists (Telegram links only, no credentials) are the sole exception — they are deliberately public and refreshed by the host cron.

## Features

- **Conversation Inventory**: Enumerate all accessible Telegram conversations and messages
- **Archive Discovery**: Automatically detect archive files (ZIP, RAR, 7z, split archives)
- **Sequential Downloads**: One file at a time with crash-safe resume
- **Multi-part Archives**: Detect and group split archives (`.part1.rar`, `.7z.001`, etc.)
- **Password Inference**: Extract passwords from message captions and nearby context
- **Credential Parsing**: Parse stealer log formats into searchable structured records
- **Duplicate Detection**: Two-stage deduplication via a compact hash-prefix index + exact unique index
- **Full Idempotency**: Database-backed state ensures no duplicate work
- **Parallel Parsing**: Large credential files are parsed across worker processes (configurable via `TELECRIME_PARSE_WORKERS`)
- **Active Channel Lists**: Exported lists contain only active, accessible channels — deleted/private ones are filtered out automatically

## Installation

### Docker Compose (recommended for production)

```bash
git clone https://github.com/stub1t/TeleCrime.git
cd TeleCrime

# Configure secrets
cp .env.example .env   # fill in Telegram API credentials + Postgres password

# Start database, web dashboard, and worker
docker compose up -d --build
```

The stack consists of three containers:

| Service | Purpose |
|---|---|
| `db` | PostgreSQL 16 (data lives in the `postgres_data` docker volume) |
| `web` | FastAPI dashboard with full-text search on port 8000 |
| `worker` | Scheduler + pipeline (supervised subprocess, watchdog-restarted) |

See [DEPLOYMENT.md](DEPLOYMENT.md) for moving the stack (including the encrypted
database SSD) to new hardware.

### Local development

```bash
# Install with uv (recommended)
uv sync

# Or install with pip
pip install -e .
```

## Configuration

Set environment variables or create `~/.config/telecrime/config.toml`:

```bash
export TELECRIME_TELEGRAM_API_ID=your_api_id
export TELECRIME_TELEGRAM_API_HASH=your_api_hash
export TELECRIME_DATABASE_URL=postgresql://telecrime:password@localhost:5432/telecrime
```

Get your Telegram API credentials at https://my.telegram.org/apps

### Performance knobs

| Variable | Default | Purpose |
|---|---|---|
| `TELECRIME_PARSE_WORKERS` | min(4, cpus-1) | Parallel parse worker processes |
| `TELECRIME_READY_GROUP_CONCURRENCY` | 3 | Parallel extraction/parse groups |
| `TELECRIME_PIPELINE_STALE_SECONDS` | 3600 | Watchdog stale threshold |
| `TELECRIME_CHANNEL_CHECK_BATCH` | 20 | Channels verified per `channel_join` run |
| `TELECRIME_REPO_URL` | — | Public repo URL used in the channel-list footer |

## Usage

```bash
# Initialize database
telecrime init

# Run the pipeline
telecrime run

# Run in batch mode (download all, then extract all, then parse all)
telecrime run --batch

# Check status
telecrime status

# Build or rebuild the search index
telecrime fts rebuild

# Retry failed jobs
telecrime retry

# Clean up downloads
telecrime clean --downloads --force

# Regenerate + push the public channel lists (see scripts/update-channels.sh)
telecrime channels-export --output-dir . --commit --push
```

## Pipeline Stages

1. **Ingest** - Enumerate conversations and messages from Telegram
2. **Channel Discover** - Discover and track new Telegram channels from local data
3. **Discover** - Identify archive candidates by extension/MIME type
4. **Plan** - Create download jobs and group multi-part archives
5. **Acquire** - Download files sequentially with verification
6. **Enrich** - Resolve forwarded message origins
7. **Extract** - Run 7z with inferred password candidates
8. **Parse** - Parse credential files into structured records with deduplication
9. **Finalize** - Record provenance, detect duplicates, cleanup

## Requirements

- Python 3.11+
- 7-Zip (`7z` command) for archive extraction
- Telegram API credentials
- PostgreSQL 16 (production; SQLite is accepted for test fixtures only)

## Development

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=telecrime

# Type checking
uv run mypy telecrime/

# Linting
uv run ruff check telecrime/

# Secret scan (full history)
gitleaks detect --source . --log-opts="--all" --config .gitleaks.toml
```

## Acknowledgements

This project was written almost entirely with the help of AI assistants
(LLMs such as Claude and OpenAI models), used for implementation,
refactoring, testing, and debugging. Human review and operations guidance
were provided throughout.

## License

AGPL-3.0 - See [LICENSE](LICENSE) for details.
