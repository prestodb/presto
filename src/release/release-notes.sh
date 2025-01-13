#!/bin/bash

if [[ "$#" -ne 2 ]]; then
    echo "Usage: $0 <github_user> <github_access_token>"
    exit 1
fi

curl -L -o /tmp/presto_release "https://jitpack.io/com/github/prestodb/presto-release-tools/presto-release-tools/master-SNAPSHOT/presto-release-tools-master-SNAPSHOT-executable.jar"
chmod 755 /tmp/presto_release
/tmp/presto_release release-notes --github-user $1 --github-access-token $2
