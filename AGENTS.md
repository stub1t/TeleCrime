# Operating Rules (User Preferences)

## Critical fixes: act immediately
For critical/logic bugs (data loss, hangs, wrong results): implement the fix,
restart the affected service (docker compose up -d worker, etc.) and commit the
change to git **immediately** — do not wait for confirmation.

See also `scripts/unattended-watchdog.sh` (host health-check, restart loop) and
`docker-compose.yml` (worker/web/db).

## Git workflow
After making changes, commit them and push the commit to the configured remote
without waiting for separate confirmation.
