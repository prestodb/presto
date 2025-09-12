#!/bin/sh

set -e
trap 'kill -TERM $app 2>/dev/null' TERM

java -Dconfig=/opt/function-server/etc/config.properties -jar /opt/presto-remote-server

$PRESTO_HOME/bin/launcher run &
app=$!
wait $app

