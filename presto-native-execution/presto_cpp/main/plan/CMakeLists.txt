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

add_library(presto_plan PrestoToVeloxQueryPlan.cpp VeloxPlanChecker.cpp)
target_link_libraries(
  presto_plan
  presto_velox_expr_conversion
  presto_connectors
  presto_operators
  presto_type_converter
  presto_session_properties
  presto_exception
  velox_type
  velox_type_fbhive
  velox_core
)

set_property(TARGET presto_plan PROPERTY JOB_POOL_LINK presto_link_job_pool)
