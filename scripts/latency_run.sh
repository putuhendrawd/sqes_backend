#!/bin/bash

# SQES Latency Collection Script
# Runs sqes_cli.py with --latency-collector

# Automatically determine project directory from script location
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR" || exit 1

TODAY=$(date -u +%Y%m%d_%H%M)

echo "--- Starting Latency Collection for Date: $TODAY ---"

# Automatically detect Python executable
# First try: Check if we're in a conda environment
if [ -n "$CONDA_PREFIX" ]; then
    PYTHON_EXEC="$CONDA_PREFIX/bin/python"
# Second try: Find sqes_backend environment in default conda location
elif [ -f "/opt/miniconda3/envs/sqes_backend/bin/python" ]; then
    PYTHON_EXEC="/opt/miniconda3/envs/sqes_backend/bin/python"
# Third try: Use system python3
else
    PYTHON_EXEC="$(which python3)"
fi

echo "Using Python: $PYTHON_EXEC"

# Run the CLI
# -v for INFO logging
# stdout is discarded (Python logs to logs/log/ automatically), stderr goes to logs/error/
$PYTHON_EXEC sqes_cli.py --latency-collector -v > /dev/null 2>> logs/error/latency_cron_$TODAY.err

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "--- Success ---"
else
    echo "--- Failed with exit code $EXIT_CODE ---"
fi

exit $EXIT_CODE
