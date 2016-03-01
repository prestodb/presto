#!/bin/bash -xe

CONFIG=$1

if [[ "$CONFIG" != "master" && "$CONFIG" != "worker" ]]; then
   echo "Usage: launcher-wrapper <master|worker> <launcher args>"
   exit 1
fi

shift 1
presto-server/target/presto-server-$PRESTO_VERSION/bin/launcher \
  -Dnode.id=$HOSTNAME \
  --config=/etc/$CONFIG/config.properties \
  --jvm-config=/etc/$CONFIG/jvm.config \
  --log-levels-file=/etc/$CONFIG/log.properties \
  --data-dir=/var/presto \
  $* 
