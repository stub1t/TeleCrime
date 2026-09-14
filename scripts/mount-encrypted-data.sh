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

# UUID of the LUKS partition on the external drive (find it with: lsblk -o NAME,UUID)
LUKS_UUID="${TELECRIME_LUKS_UUID:-2d87de69-8f80-4ac9-bfc1-56f4e304a572}"
MAPPER_NAME="${TELECRIME_MAPPER_NAME:-telecrime-data}"
# Drive filesystem root — must be the parent of TELECRIME_DATA_DIR (data/ and
# postgres_data/ live directly on this filesystem).
MOUNT_POINT="${TELECRIME_MOUNT_POINT:-/mnt/telecrime}"

if [[ $EUID -ne 0 ]]; then
    echo "must be run as root (use sudo)" >&2
    exit 1
fi

# Already mounted: nothing to do. Check this BEFORE the device lookup — after
# a successful boot mount the by-uuid symlink can disappear while the mapper
# stays mounted, and this also keeps re-runs from trying to re-open a volume
# that is already in use.
if mountpoint -q "$MOUNT_POINT"; then
    echo "$MOUNT_POINT already mounted."
    df -h "$MOUNT_POINT"
    exit 0
fi

# The volume may already be open under the systemd/udisks2-generated mapper
# name (luks-<uuid>) instead of the crypttab MAPPER_NAME. Re-running
# `cryptsetup luksOpen` then fails with "already in use". Find the mapper
# BEFORE looking for the by-uuid symlink: an open mapper is mountable even if
# udev has not (re)created the device symlink yet.
ACTUAL_MAPPER=""
for name in "$MAPPER_NAME" "luks-${LUKS_UUID}"; do
    if [[ -e "/dev/mapper/$name" ]]; then
        ACTUAL_MAPPER="$name"
        break
    fi
done

if [[ -z "$ACTUAL_MAPPER" ]]; then
    DEV="/dev/disk/by-uuid/${LUKS_UUID}"
    if [[ ! -e "$DEV" ]]; then
        echo "LUKS device $DEV not found — is the USB SSD connected?" >&2
        exit 1
    fi
    echo "Unlocking LUKS volume..."
    cryptsetup luksOpen "$DEV" "$MAPPER_NAME"
    ACTUAL_MAPPER="$MAPPER_NAME"
else
    echo "LUKS mapper /dev/mapper/${ACTUAL_MAPPER} already open."
fi

mkdir -p "$MOUNT_POINT"
# Honor the fstab options for this mount point (noatime/nosuid/nodev/
# errors=remount-ro here) instead of silently mounting with bare defaults.
# `mount -o nofail` is accepted by util-linux; findmnt is best-effort.
FSTAB_OPTS="$(findmnt --fstab --target "$MOUNT_POINT" --noheadings --output OPTIONS 2>/dev/null | tr -d ' ' || true)"
if [[ -n "$FSTAB_OPTS" ]]; then
    mount -o "$FSTAB_OPTS" "/dev/mapper/${ACTUAL_MAPPER}" "$MOUNT_POINT"
else
    mount "/dev/mapper/${ACTUAL_MAPPER}" "$MOUNT_POINT"
fi
echo "Mounted /dev/mapper/${ACTUAL_MAPPER} at $MOUNT_POINT"

df -h "$MOUNT_POINT"
