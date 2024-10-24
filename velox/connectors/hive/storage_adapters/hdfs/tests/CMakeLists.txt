# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(velox_hdfs_file_test HdfsFileSystemTest.cpp HdfsMiniCluster.cpp
                                    HdfsUtilTest.cpp)

add_test(velox_hdfs_file_test velox_hdfs_file_test)
target_link_libraries(
  velox_hdfs_file_test
  velox_file
  velox_hdfs
  velox_core
  velox_exec_test_lib
  velox_hive_connector
  velox_dwio_common_exception
  velox_exec
  GTest::gtest
  GTest::gtest_main
  GTest::gmock)

target_compile_options(velox_hdfs_file_test
                       PRIVATE -Wno-deprecated-declarations)

add_executable(velox_hdfs_insert_test InsertIntoHdfsTest.cpp
                                      HdfsMiniCluster.cpp HdfsUtilTest.cpp)

add_test(velox_hdfs_insert_test velox_hdfs_insert_test)

target_link_libraries(
  velox_hdfs_insert_test
  velox_exec_test_lib
  velox_exec
  GTest::gtest
  GTest::gtest_main
  GTest::gmock)

target_compile_options(velox_hdfs_insert_test
                       PRIVATE -Wno-deprecated-declarations)

# velox_hdfs_insert_test and velox_hdfs_file_test two tests can't run in
# parallel due to the port conflict in Hadoop NameNode and DataNode. The
# namenode port conflict can be resolved using the -nnport configuration in
# hadoop-mapreduce-client-jobclient-3.3.0-tests.jar. However the data node port
# cannot be configured. Therefore, we need to make sure that
# velox_hdfs_file_test runs only after velox_hdfs_insert_test has finished.
set_tests_properties(velox_hdfs_insert_test PROPERTIES DEPENDS
                                                       velox_hdfs_file_test)
