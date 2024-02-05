#!/bin/bash

# Prerequisites:
#   - the presto-server-${VERSION}.tar.gz and presto-cli-${VERSION}-executable.jar shall exist
#     in the same directory.
#   - docker build driver is setup and ready to build multiple platform images. If not
#     check the documentation here: https://docs.docker.com/build/building/multi-platform/
#   - login to the container registry where images will be published to
set -e

if [ "$#" != "1" ]; then
    echo "usage: build.sh <version>"
    exit 1
fi

VERSION=$1; shift
TAG="${TAG:-latest}"
IMAGE_NAME="${IMAGE_NAME:-presto}"
REG_ORG="${REG_ORG:-docker.io/prestodb}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64,linux/ppc64le}"
TMP_IIDFILE=$(mktemp)
IIDFILE="${IIDFILE:-$TMP_IIDFILE}"
PUBLISH="${PUBLISH:-false}"
BUILDER="${BUILDER:-container}"

export BUILDX_NO_DEFAULT_ATTESTATIONS=""
# If PUBLISH=false, images only stores in local cache, otherwise they are pushed to th container registry
docker buildx build --builder="${BUILDER}" --iidfile "${IIDFILE}" \
    --build-arg="PRESTO_VERSION=${VERSION}" \
    --build-arg="JMX_PROMETHEUS_JAVAAGENT_VERSION=0.20.0" \
    --output "type=image,name=${REG_ORG}/${IMAGE_NAME},push-by-digest=true,name-canonical=true,push=${PUBLISH}" \
    --platform "${PLATFORMS}" -f Dockerfile .

if [[ "$PUBLISH" = "true" ]]; then
    # This only happens when push=true, since push-by-digest=true in the above build step, need to tag the images explicitly
    docker buildx imagetools create --builder="${BUILDER}" \
    -t "${REG_ORG}/${IMAGE_NAME}:${TAG}" "${REG_ORG}/${IMAGE_NAME}@$(cat "$IIDFILE")"
fi
