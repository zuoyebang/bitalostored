#!/bin/bash

directory="demo"
if [ ! -d "$directory" ]; then
  echo "Directory $directory not found."
  exit 1
fi

for pidfile in "$directory"/*.pid; do
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    echo "Killing process $pid"
    kill -9 "$pid"
    rm "$pidfile"
  fi
done
