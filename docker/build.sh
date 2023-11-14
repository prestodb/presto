#!/bin/bash

if [ "$#" != "1" ]; then
    echo "usage: build.sh <version>"
    exit 1
fi

VERSION=$1
TAG="${TAG:-latest}"

wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${VERSION}/presto-server-${VERSION}.tar.gz
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${VERSION}/presto-cli-${VERSION}-executable.jar

docker build -t prestodb/presto:${TAG} --build-arg="PRESTO_VERSION=${VERSION}" -f Dockerfile .

# docker push?
