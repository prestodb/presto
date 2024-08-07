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

cmake_policy(SET CMP0079 NEW)

add_library(presto_tpcds_connector OBJECT tpcds/TpcdsConnector.cpp)
target_link_libraries(presto_tpcds_connector velox_connector tpcds_gen fmt::fmt)

# Without this hack, there are multiple link errors similar to the one below
# only on GCC. "undefined reference to `vtable for
# velox::connector::tpcds::TpcdsTableHandle`. TODO: Fix this hack.
target_link_libraries(velox_exec_test_lib presto_tpcds_connector)

add_subdirectory(tpcds)
