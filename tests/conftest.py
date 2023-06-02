import logging
import os

# Enable debug logging.
logging.getLogger().setLevel(logging.DEBUG)
os.putenv("DATAHUB_DEBUG", "1")

# Disable telemetry
os.putenv("DATAHUB_TELEMETRY_ENABLED", "false")
