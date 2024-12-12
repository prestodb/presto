mkdir build && cd build
../scripts/setup-ubuntu.sh
../velox/scripts/setup-adapters.sh
../scripts/setup-adapters.sh
rm -rf build
rm -rf /usr/local/hadoop
rm -rf /usr/local/bin/fizz*
rm -rf /usr/local/bin/minio-2022-05-26
rm -rf /usr/local/bin/proxygen*
rm -rf /usr/local/bin/thrift*
rm -rf /usr/local/bin/grpc*
rm -rf /usr/local/bin/hq
