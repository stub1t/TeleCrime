# Security Policy

## What this project handles

Telecrime processes credential data sourced from stealer-log Telegram channels.
The repository itself contains **no runtime data**: no credentials, no channel
lists, no Telegram sessions, no database dumps. Runtime data belongs in the
operator-configured data directory and never enters git.

## Reporting a vulnerability

Please report security issues privately — do not open a public issue that
could reveal operational details.

- Open a GitHub Security Advisory (preferred)
- Or email the maintainers (address in the repository profile)

Include:

- A description of the issue and its impact
- Steps to reproduce (without including real credential data)
- Affected version(s)

## Data handling rules for contributors

These are enforced by `.gitignore` and by the codebase (the channel export
command never commits):

- **Never commit** `.env`, `*.session`, `*.db`, `data/`, dumps, or credential
  exports. Exception: `channels.txt` / `channels.md` are deliberately public —
  they list only active, accessible stealer-log channels and are refreshed by
  the host cron.
- If you accidentally commit a secret, **rotate it** (it is compromised)
  and then remove it from history with `git-filter-repo`
- Do not add real channel names or Telegram links to issues, PRs, or docs

## Supported versions

Security fixes are applied to the current `main` branch and released as
tagged versions. There is no long-term-support channel.

## Deployment notes

- The dashboard (port 8000) has **no authentication** by design — it is meant
  for a trusted LAN. Expose it to the internet only behind a reverse proxy
  with auth (e.g. Basic Auth, mTLS, or a VPN).
- The Telegram session (`telecrime.session`) grants full access to the linked
  Telegram account. Protect it like a password.
