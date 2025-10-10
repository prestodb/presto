#!/bin/bash
# Before executing this script, you should compile presto_server
# Copy shared libs to the directory /runtime-libraries
mkdir /runtime-libraries && \
    bash -c '!(LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64 ldd ../cmake-build-debug/presto_cpp/main/presto_server | grep "not found")' && \
    LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64 ldd ../cmake-build-debug/presto_cpp/main/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }'