#!/bin/bash

set -e

if [ $# -lt 2 ]; then
  echo "usage: publish-okera.sh [presto-server] [okera-suffix]"
  echo "example: publish-okera.sh 0.234.2 5"
  exit 1
fi

PRESTO_VERSION=$1
OKERA_SUFFIX=$2
BUCKET=s3://cerebrodata-public

./mvnw clean install -DskipTests
aws s3 cp presto-server/target/presto-server-${PRESTO_VERSION}.tar.gz \
    $BUCKET/okera-presto-server-${PRESTO_VERSION}-${OKERA_SUFFIX}.tar.gz --acl public-read
