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

add_library(
  velox_cudf_exec
  CudfConversion.cpp
  CudfFilterProject.cpp
  CudfHashAggregation.cpp
  CudfHashJoin.cpp
  CudfLimit.cpp
  CudfLocalPartition.cpp
  CudfOrderBy.cpp
  DebugUtil.cpp
  ExpressionEvaluator.cpp
  ToCudf.cpp
  Utilities.cpp
  VeloxCudfInterop.cpp
)

target_link_libraries(
  velox_cudf_exec
  PUBLIC cudf::cudf
  PRIVATE arrow velox_arrow_bridge velox_exception velox_common_base velox_cudf_vector velox_exec
)

target_compile_options(velox_cudf_exec PRIVATE -Wno-missing-field-initializers)
