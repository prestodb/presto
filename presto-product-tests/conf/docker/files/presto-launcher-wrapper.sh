#!/bin/bash -xe

CONFIG=$1

if [[ "$CONFIG" != "singlenode" && "$CONFIG" != "distributed-master" && "$CONFIG" != "distributed-worker" ]]; then
   echo "Usage: launcher-wrapper <singlenode|distributed-master|distributed-worker> <launcher args>"
   exit 1
fi

shift 1
/presto-server/target/presto-server-${PRESTO_VERSION}/bin/launcher \
  -Dnode.id=${HOSTNAME} \
  -Dcatalog.config-dir=/etc/presto/catalog \
  --config=/etc/presto/${CONFIG}.properties \
  --jvm-config=/etc/presto/jvm.config \
  --log-levels-file=/etc/presto/log.properties \
  --data-dir=/var/presto \
  $* 
