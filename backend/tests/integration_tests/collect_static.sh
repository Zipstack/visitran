#!/bin/bash

# Save the current directory
start_dir=$(pwd)

# Do some work...
cd ../../../visitran_ui
npm install
REACT_APP_PROJECT_NAME=duckdbuitest npm run relocate
cd ../visitran_backend
uv run manage.py collectstatic --noinput

# Return to the starting directory
cd "$start_dir"
