#!/bin/bash

if [[ "$#" -ne 2 ]]; then
    echo "Usage: $0 <github_user> <github_access_token>"
    exit 1
fi

curl -L -o /tmp/presto_release "https://oss.sonatype.org/service/local/artifact/maven/redirect?g=com.facebook.presto&a=presto-release-tools&v=RELEASE&r=releases&c=executable&e=jar"
chmod 755 /tmp/presto_release
/tmp/presto_release release-notes --github-user $1 --github-access-token $2
