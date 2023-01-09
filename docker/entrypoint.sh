#!/bin/sh

set -e
echo "node.id=$HOSTNAME" >> $PRESTO_HOME/etc/node.properties

$PRESTO_HOME/bin/launcher run
