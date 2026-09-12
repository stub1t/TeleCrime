"""Shared constants used across pipeline stages."""

# Maximum extraction attempts per group before startup recovery gives up and
# marks the group FAILED_TERMINAL. Bounds both the FAILED→READY retry loop and
# the PASSWORD_NEEDED→PENDING reset (each run counts one attempt).
EXTRACTION_MAX_ATTEMPTS = 3
