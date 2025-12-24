#!/bin/bash

# force_daterange_flush_run.sh
# A wrapper script to enforce flushing data over a date range by calling sqes_cli.py for single days iteratively.
# This bypasses the safety restriction in sqes_cli.py that prevents --flush with --date-range.

# Automatically determine project directory from script location
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR" || exit 1

# Ensure error log directory exists
mkdir -p "logs/error"

# Function to print usage
usage() {
    echo "Usage: $0 -r <START_DATE> <END_DATE> [OPTIONS]"
    echo ""
    echo "Required:"
    echo "  -r, --date-range START END   Date range to process and flush (YYYYMMDD)"
    echo ""
    echo "Options (passed to sqes_cli.py):"
    echo "  -s, --station STA [STA ...]  Stations to process"
    echo "  -n, --network NET [NET ...]  Networks to process"
    echo "  -v, --verbose                Increase verbosity"
    echo "  --ppsd                       Save PPSD matrices"
    echo "  --mseed                      Save MiniSEED files"
    echo ""
    echo "Warning: This script will FLUSH (DELETE) existing data for every day in the range!"
    exit 1
}

EXTRA_ARGS=()
START_DATE=""
END_DATE=""

if [ $# -eq 0 ]; then
    usage
fi

# Argument Parsing
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -r|--date-range)
      if [[ -z "$2" || -z "$3" ]]; then
          echo "Error: --date-range requires two arguments (START END)."
          exit 1
      fi
      START_DATE="$2"
      END_DATE="$3"
      shift # past argument
      shift # past value
      shift # past value
      ;;
    -s|--station|--stations)
      EXTRA_ARGS+=("$1")
      shift # past argument
      # Collect all arguments until next flag
      while [[ $# -gt 0 ]] && ! [[ "$1" =~ ^- ]]; do
        EXTRA_ARGS+=("$1")
        shift
      done
      ;;
    -n|--network)
      EXTRA_ARGS+=("$1")
      shift # past argument
      while [[ $# -gt 0 ]] && ! [[ "$1" =~ ^- ]]; do
        EXTRA_ARGS+=("$1")
        shift
      done
      ;;
    -v|--verbose)
       EXTRA_ARGS+=("$1")
       shift
       ;;
    -vv) # Handle combined verbose flags commonly used
       EXTRA_ARGS+=("-vv")
       shift
       ;;
    --ppsd)
       EXTRA_ARGS+=("--ppsd")
       shift
       ;;
    --mseed)
       EXTRA_ARGS+=("--mseed")
       shift
       ;;
    -h|--help)
      usage
      ;;
    *)
      # Attempt to pass unknown args through? Or fail?
      # For safety, strictly matching known args is better, but maybe user uses -vvv?
      if [[ "$1" =~ ^- ]]; then
          EXTRA_ARGS+=("$1")
          shift
      else
          echo "Unknown positional argument: $1"
          usage
      fi
      ;;
  esac
done

if [[ -z "$START_DATE" ]] || [[ -z "$END_DATE" ]]; then
    echo "Error: Date range is required (-r YYYYMMDD YYYYMMDD)."
    usage
fi

# Verify CLI exists
if [[ ! -f "sqes_cli.py" ]]; then
    echo "Error: Could not find sqes_cli.py in $PROJECT_DIR"
    exit 1
fi

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
echo "=================================================================="
echo "⚠️  FORCE FLUSH MODE ACTIVATED ⚠️"
echo "Date Range: $START_DATE to $END_DATE"
echo "Extra Args: ${EXTRA_ARGS[@]}"
echo "Error Logs: logs/error/force_flush_<DATE>.err (one per date)"
echo "This will DELETE and REPROCESS data for each day in the range."
echo "=================================================================="
echo "Starting in 10 seconds... (Ctrl+C to cancel)"
sleep 10

# Loop through dates
CURRENT_DATE="$START_DATE"

while [[ "$CURRENT_DATE" -le "$END_DATE" ]]; do
    # Create unique error log for this specific date
    DATE_ERROR_LOG="logs/error/force_flush_$CURRENT_DATE.err"
    
    echo ""
    echo ">>> Processing Date: $CURRENT_DATE with --flush <<<"
    echo "    Error log: $DATE_ERROR_LOG"
    
    # Construct command
    # We pass --date $CURRENT_DATE and --flush
    # parse_args handles the rest
    
    $PYTHON_EXEC sqes_cli.py --date "$CURRENT_DATE" --flush "${EXTRA_ARGS[@]}" 2>> "$DATE_ERROR_LOG"
    
    # Check return code
    RET_CODE=$?
    if [ $RET_CODE -ne 0 ]; then
        echo "❌ Error processing $CURRENT_DATE. Aborting loop."
        exit $RET_CODE
    fi
    
    # Increment date using GNU date
    NEXT_DATE=$(date -d "$CURRENT_DATE + 1 day" +%Y%m%d)
    CURRENT_DATE="$NEXT_DATE"
done

echo ""
echo "✅ Range processing complete."
