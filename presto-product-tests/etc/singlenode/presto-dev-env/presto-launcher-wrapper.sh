#!/bin/bash -xe

presto-server/target/presto-server-$PRESTO_VERSION/bin/launcher \
  -Dnode.id=$HOSTNAME \
  --config=/etc/master/config.properties \
  --jvm-config=/etc/master/jvm.config \
  --log-levels-file=/etc/master/log.properties \
  --data-dir=/var/presto \
  $* 
