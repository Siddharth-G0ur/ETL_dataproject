#!/bin/bash

# Use the SCRIPT environment variable if set, otherwise use the first argument
SCRIPT_TO_RUN=${SCRIPT:-$1}

# Check if a script name was provided
if [ -z "$SCRIPT_TO_RUN" ]; then
    echo "No script specified. Please provide a script name."
    echo "Available scripts:"
    ls -1 *.py
    exit 1
fi

# Check if the specified script exists
if [ ! -f "$SCRIPT_TO_RUN" ]; then
    echo "Script $SCRIPT_TO_RUN not found."
    echo "Available scripts:"
    ls -1 *.py
    exit 1
fi

# Run the specified script
python "$SCRIPT_TO_RUN"