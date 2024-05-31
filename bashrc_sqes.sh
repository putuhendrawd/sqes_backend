## Path
export SQES_PATH="$(cd $(dirname ${BASH_SOURCE:-$0}); pwd)"
# export PATH="$SQES_PATH/sqes_backend:$PATH"
export PYTHONPATH="$SQES_PATH/sqes_backend:$PYTHONPATH"

