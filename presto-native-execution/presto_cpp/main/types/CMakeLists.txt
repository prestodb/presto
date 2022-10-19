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
add_library(
  presto_type_converter OBJECT
  TypeSignatureTypeConverter.cpp antlr/TypeSignatureLexer.cpp
  antlr/TypeSignatureParser.cpp)

target_link_libraries(presto_type_converter velox_type)

add_library(presto_types PrestoToVeloxQueryPlan.cpp PrestoToVeloxExpr.cpp
                         PrestoToVeloxSplit.cpp)
add_dependencies(presto_types presto_type_converter velox_type
                 velox_hive_connector velox_dwio_dwrf_proto)

target_link_libraries(presto_types presto_type_converter velox_hive_connector
                      velox_hive_partition_function velox_tpch_gen)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
