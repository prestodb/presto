#!/bin/sh

set -e

java -Dconfig=/opt/function-server/etc/config.properties -jar /opt/presto-remote-server  >> log1.txt 2>&1

$PRESTO_HOME/bin/launcher run
