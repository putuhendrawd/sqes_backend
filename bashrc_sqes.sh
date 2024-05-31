## Path
export SQES_PATH="$(cd $(dirname ${BASH_SOURCE:-$0}); pwd)"
export PYTHONPATH="$SQES_PATH/sqes_backend:$PYTHONPATH"
# export PATH="$SQES_PATH/sqes_backend:$PATH"
