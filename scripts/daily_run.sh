#!/bin/bash

# SQES Daily Processing Script
# Runs sqes_cli.py for "yesterday" (UTC)

# Ensure we are in the correct directory
PROJECT_DIR="/home/geo2sqes/dev/sqes_backend"
cd "$PROJECT_DIR" || exit 1

# Calculate yesterday's date in YYYYMMDD format (UTC)
# -u ensures UTC, -d "yesterday" gets the previous day
DATE_TO_PROCESS=$(date -u -d "yesterday" +%Y%m%d)

echo "--- Starting Daily Cron Job for Date: $DATE_TO_PROCESS ---"

# Path to the Python executable in the 'sqes_backend' conda environment
PYTHON_EXEC="/opt/miniconda3/envs/sqes_backend/bin/python"

# Run the CLI
# -v for INFO logging
# --sensor-update to keep metadata fresh (optional but recommended)
# stdout is discarded (Python logs to logs/log/ automatically), stderr goes to logs/error/
$PYTHON_EXEC sqes_cli.py --date "$DATE_TO_PROCESS" --sensor-update -v > /dev/null 2>> logs/error/cron_$DATE_TO_PROCESS.err

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "--- Success ---"
else
    echo "--- Failed with exit code $EXIT_CODE ---"
fi

exit $EXIT_CODE
