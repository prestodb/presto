#!/bin/bash -ex

if [[ "$#" -ne 2 ]]; then
    echo "Usage: $0 <github_user> <github_access_token>"
    exit 1
fi

RELEASE_TOOLS_VERSION=${RELEASE_TOOLS_VERSION:-"0.14"}
REPO_OWNER=${REPO_OWNER:-"prestodb"}

curl -L -o /tmp/presto_release "https://github.com/${REPO_OWNER}/presto-release-tools/releases/download/${RELEASE_TOOLS_VERSION}/presto-release-tools-${RELEASE_TOOLS_VERSION}-executable.jar"
chmod 755 /tmp/presto_release
java ${JVM_OPTS} -jar /tmp/presto_release release-notes --github-user $1 --github-access-token $2