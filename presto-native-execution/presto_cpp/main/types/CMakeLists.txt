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

add_library(presto_type_converter OBJECT TypeParser.cpp)
target_link_libraries(presto_type_converter velox_presto_type_parser)

add_library(presto_velox_expr_conversion OBJECT PrestoToVeloxExpr.cpp)
target_link_libraries(presto_velox_expr_conversion velox_presto_types
                      velox_vector velox_exception)

add_library(presto_types PrestoToVeloxQueryPlan.cpp VeloxPlanValidator.cpp
                         PrestoToVeloxSplit.cpp)
target_compile_definitions(presto_types PUBLIC VELOX_ENABLE_BACKWARD_COMPATIBILITY)
target_link_libraries(
  presto_types
  presto_velox_expr_conversion
  presto_connectors
  presto_operators
  presto_type_converter
  velox_type
  velox_type_fbhive)

set_property(TARGET presto_types PROPERTY JOB_POOL_LINK presto_link_job_pool)

add_library(presto_function_metadata OBJECT FunctionMetadata.cpp)
target_link_libraries(presto_function_metadata velox_function_registry)

add_library(presto_velox_plan_conversion OBJECT VeloxPlanConversion.cpp)
target_link_libraries(presto_velox_plan_conversion velox_type)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
