# Migrating to New Hardware

This document describes how to move the Telecrime stack (code + data) to a new
machine. The architecture is designed for this: **all production data lives on
the encrypted external SSD**, and the machine itself only holds the code
repository, configuration, and Docker.

## Data layout

| What | Where | Movable? |
|---|---|---|
| PostgreSQL database (all credentials, ~222GB) | External SSD, `/mnt/telecrime/postgres_data` | ✅ Whole SSD moves |
| Pipeline data (Telegram session, logs, exports) | External SSD, `/mnt/telecrime/data` | ✅ Whole SSD moves |
| Telegram API credentials | Laptop, `.env` | Copy to new machine |
| Code + Docker config | Laptop, `/home/user/TeleCrime` | ✅ Git clone |

Everything critical is on the external SSD. The laptop only holds code and
secrets.

## Migration checklist

### 1. On the new machine — prepare

```bash
# Install: docker, docker compose plugin, git, uv (or pip)
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git

# Verify the encrypted SSD is detected
lsblk -o NAME,SIZE,MODEL,TRAN
```

### 2. Mount the external SSD

The SSD is LUKS-encrypted (Crucial CT1000BX500SSD1). Unlock and mount it:

```bash
# Determine the LUKS device (UUID from the old machine):
#   lsblk -o NAME,UUID | grep sdb
sudo cryptsetup luksOpen /dev/sdb1 telecrime-data
sudo mount /dev/mapper/telecrime-data /mnt/telecrime
df -h /mnt/telecrime
```

> The mount script lives at `scripts/mount-encrypted-data.sh`. Set the
> `TELECRIME_LUKS_UUID` variable to your device UUID (run as root).

### 3. Get the code

```bash
git clone https://github.com/stub1t/TeleCrime.git
cd TeleCrime
```

### 4. Configure secrets

```bash
cp .env.example .env
# Fill in:
#   TELECRIME_TELEGRAM_API_ID / API_HASH   (from my.telegram.org)
#   TELECRIME_POSTGRES_PASSWORD             (the same one used on the old machine!)
#   TELECRIME_DATABASE_URL
```

**Critical**: The PostgreSQL data directory already exists on the SSD with the
old database. The Postgres password **must match** the one used previously, or
the database will reject connections. If you do not remember it, reset it on
the OLD machine first:

```bash
# On the old machine, inside the db container:
docker compose exec db psql -U telecrime -c "ALTER USER telecrime WITH PASSWORD 'new-password';"
```

### 5. Start the stack

```bash
docker compose up -d --build
```

- `db` boots and recovers from the SSD volume (may take a few minutes for a
  large database)
- `web` runs alembic migrations, then serves the dashboard on port 8000
- `worker` starts the scheduler, which spawns the pipeline subprocess

Verify:

```bash
docker compose ps                  # all three containers healthy
curl http://localhost:8000/        # dashboard responds
docker compose logs -f worker      # pipeline starts ingesting
```

## Restoring from scratch (no SSD)

If you only have the data directory (e.g. a Postgres dump) instead of the SSD:

```bash
# Load a pg_dump into a fresh database
docker compose up -d db
docker compose exec -T db psql -U telecrime -d telecrime < telecrime.dump
```

## Before you switch off the old machine

- [ ] `docker compose ps` — confirm all healthy on the NEW machine
- [ ] Dashboard loads at `http://new-host:8000`
- [ ] Pipeline is running (worker logs show ingest/parse activity)
- [ ] Old machine: `sudo umount /mnt/telecrime` before disconnecting the SSD
- [ ] Keep the old machine's `.env` as backup (contains Telegram credentials)

## Hardware recommendations

The current bottleneck is database I/O. For best performance:

- **32GB+ RAM** — the credential dedup indexes (~49GB) then fit in the OS
  page cache, making dedup 10-50× faster than disk-bound
- **NVMe SSD** — eliminates the I/O wait that dominates bulk inserts
- **8+ cores** — the parallel parse scales with cores

With 64GB RAM and NVMe, the same pipeline run that takes hours here completes
in minutes.
