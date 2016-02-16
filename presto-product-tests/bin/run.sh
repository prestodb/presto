#!/bin/bash -e

function wait_for() {
  echo "waiting for - $*"
  for i in $(seq 10); do
    if $* > /dev/null; then
      return
    fi
    echo ...
    sleep 5
  done
  echo "$* - did not succeeded within a time limit"
  exit 1
}

cd $(dirname $(readlink -f $0))

VERSION=$(cd ..; mvn -B org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[' )

# set up hadoop cluster
devenv hadoop build-image
devenv hadoop start
devenv hadoop set-ip

# set up presto cluster
devenv presto build-image --presto-source ../.. --master-config ../etc/master/ --worker-config ../etc/worker/  --no-build
devenv presto start
devenv presto set-ip

# wait for cluster to be up and running
wait_for devenv status

# run product tests
echo Running product tests, you can attach debugger at port 5005
java \
  -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 \
  -jar ../target/presto-product-tests-${VERSION}-executable.jar \
  --report-dir ../target/tests-report \
  --config-local file://`pwd`/../etc/tempto-configuration-local.yaml \
  $*
PRODUCT_TESTS_RETURN_STATUS=$?

# stop clusters
read -n 1 -p "Should stop docker clusters (y/n)? " ANSWER
echo
if [ x$ANSWER == "xy" ]; then
  devenv presto stop
  devenv hadoop stop

  # clean clusters
  read -n 1 -p "Should clean docker clusters (y/n)? " ANSWER
  echo
  if [ x$ANSWER == "xy" ]; then
    devenv presto clean
    devenv hadoop clean
  fi
fi

exit $PRODUCT_TESTS_RETURN_STATUS
