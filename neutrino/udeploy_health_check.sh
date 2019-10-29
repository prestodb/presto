#!/bin/bash

set -ex

HTTP_PORT_TO_USE=8080
if [[ -n "$UBER_PORT_HTTP" ]]; then
    HTTP_PORT_TO_USE=$UBER_PORT_HTTP
fi
exec /usr/lib/nagios/plugins/check_http -I 127.0.0.1 -p "$HTTP_PORT_TO_USE" -u /v1/info -w 2 -c 10 -e '200 OK'
