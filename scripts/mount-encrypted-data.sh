#!/usr/bin/env bash
# Optional helper for unlocking and mounting a LUKS-backed data directory.
# Run after each reboot before `docker compose up` when external storage is used.
#
#   sudo ./scripts/mount-encrypted-data.sh
#
# Idempotent: skips steps that are already done.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Replace with your device's UUID — find it with: lsblk -o NAME,UUID
LUKS_UUID="${TELECRIME_LUKS_UUID:-YOUR-LUKS-UUID-HERE}"
MAPPER_NAME="${TELECRIME_MAPPER_NAME:-telecrime-data}"
MOUNT_POINT="${TELECRIME_MOUNT_POINT:-${TELECRIME_DATA_DIR:-$REPO_DIR/data}}"

if [[ $EUID -ne 0 ]]; then
    echo "must be run as root (use sudo)" >&2
    exit 1
fi

DEV="/dev/disk/by-uuid/${LUKS_UUID}"
if [[ ! -e "$DEV" ]]; then
    echo "LUKS device $DEV not found — is the USB SSD connected?" >&2
    exit 1
fi

if [[ ! -e "/dev/mapper/${MAPPER_NAME}" ]]; then
    echo "Unlocking LUKS volume..."
    cryptsetup luksOpen "$DEV" "$MAPPER_NAME"
else
    echo "LUKS mapper /dev/mapper/${MAPPER_NAME} already open."
fi

if ! mountpoint -q "$MOUNT_POINT"; then
    mkdir -p "$MOUNT_POINT"
    mount "/dev/mapper/${MAPPER_NAME}" "$MOUNT_POINT"
    echo "Mounted /dev/mapper/${MAPPER_NAME} at $MOUNT_POINT"
else
    echo "$MOUNT_POINT already mounted."
fi

df -h "$MOUNT_POINT"
