#!/bin/bash

echo "- program : batch "
echo "- pwd : ${PWD}/${0}"
echo "- job : start the program"


DIR="$( cd "$( dirname "$0" )" && pwd )"
VENV_DIR="${DIR}/.venv"
MAIN_DIR="${DIR}/app/main.py"

if [ -e "$MAIN_DIR" ]; then
    echo "start the program!"
    . $VENV_DIR/bin/activate
    #pip list
    python $MAIN_DIR
else
    echo "$MAIN_DIR does not exist."
fi