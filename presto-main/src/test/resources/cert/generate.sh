#!/bin/sh

set -eux

openssl req -new -x509 -newkey rsa:4096 -sha256 -nodes -keyout localhost.key -days 3560 -out localhost.crt -config localhost.conf
cat localhost.crt localhost.key > localhost.pem

