"""Shared constants used across pipeline stages."""

# Maximum extraction attempts per group before startup recovery gives up and
# marks the group FAILED_TERMINAL. Bounds both the FAILED→READY retry loop and
# the PASSWORD_NEEDED→PENDING reset (each run counts one attempt).
EXTRACTION_MAX_ATTEMPTS = 3

# Maximum total download attempts for session-file locks / connection timeouts
# before an artifact is marked FAILED_TERMINAL. Retry counts persist across
# runs, so a transient lock that keeps recurring for ~3 runs eventually stops.
DOWNLOAD_TRANSIENT_MAX_ATTEMPTS = 9
