#!/bin/sh

set -e

java -Dconfig=/opt/function-server/etc/config.properties -jar /opt/presto-remote-server

$PRESTO_HOME/bin/launcher run
