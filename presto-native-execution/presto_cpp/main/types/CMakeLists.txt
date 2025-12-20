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
target_link_libraries(presto_velox_expr_conversion velox_presto_types velox_vector velox_exception)

if(PRESTO_ENABLE_REMOTE_FUNCTIONS)
  target_link_libraries(presto_velox_expr_conversion presto_to_velox_remote_functions)
endif()

add_library(presto_types PrestoToVeloxQueryPlan.cpp VeloxPlanValidator.cpp PrestoToVeloxSplit.cpp)
target_link_libraries(
  presto_types
  presto_tvf_core
  presto_velox_expr_conversion
  presto_connectors
  presto_operators
  presto_type_converter
  presto_session_properties
  velox_type
  velox_type_fbhive
)

set_property(TARGET presto_types PROPERTY JOB_POOL_LINK presto_link_job_pool)

add_library(presto_velox_plan_conversion OBJECT VeloxPlanConversion.cpp)
target_link_libraries(presto_velox_plan_conversion velox_type)

add_library(presto_velox_to_presto_expr VeloxToPrestoExpr.cpp)

target_link_libraries(
  presto_velox_to_presto_expr
  presto_exception
  presto_type_converter
  presto_types
  presto_protocol
)

add_library(presto_expression_optimizer ExpressionOptimizer.cpp)

target_link_libraries(presto_expression_optimizer presto_types presto_velox_to_presto_expr)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
