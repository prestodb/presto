#!/usr/bin/env bash
set -e

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 [image]"
  exit 1
fi

docker build -t "$1" -f "$PWD/Dockerfile" ..
