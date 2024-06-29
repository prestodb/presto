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
add_executable(presto_expr_eval_test PrestoExprEvalTest.cpp)

add_test(presto_expr_eval_test presto_expr_eval_test)

target_link_libraries(
  presto_expr_eval_test
  presto_expr_eval
  presto_http
  velox_exec_test_lib
  velox_presto_serializer
  gtest
  gtest_main
  ${PROXYGEN_LIBRARIES})

set_property(TARGET presto_expr_eval_test PROPERTY JOB_POOL_LINK
                                                   presto_link_job_pool)
