#!/bin/bash

set -euo pipefail

java \
  "-Djava.util.logging.config.file=/docker/volumes/conf/tempto/logging.properties" \
  -Duser.timezone=Asia/Kathmandu \
  -cp "/docker/volumes/jdbc/driver.jar:/docker/volumes/presto-product-tests/presto-product-tests-executable.jar" \
  com.facebook.presto.tests.TemptoProductTestRunner \
  --report-dir "/docker/volumes/test-reports" \
  --config "tempto-configuration.yaml,/docker/volumes/tempto/tempto-configuration-local.yaml" \
  "$@"
