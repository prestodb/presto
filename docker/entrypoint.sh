#!/bin/sh

set -e
trap 'kill -TERM $app 2>/dev/null' TERM

$PRESTO_HOME/bin/launcher run &
app=$!
wait $app
