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
  velox_expression_functions FunctionSignature.cpp SignatureBinder.cpp
                             ReverseSignatureBinder.cpp)

target_link_libraries(velox_expression_functions velox_common_base
                      velox_type_calculation)

add_library(
  velox_expression
  BooleanMix.cpp
  CastExpr.cpp
  CoalesceExpr.cpp
  ConjunctExpr.cpp
  ConstantExpr.cpp
  EvalCtx.cpp
  Expr.cpp
  ExprCompiler.cpp
  ExprToSubfieldFilter.cpp
  FieldReference.cpp
  LambdaExpr.cpp
  VectorFunction.cpp
  SimpleFunctionRegistry.cpp
  SwitchExpr.cpp
  TryExpr.cpp)

target_link_libraries(
  velox_expression velox_core velox_vector velox_common_base
  velox_expression_functions velox_functions_util)

add_subdirectory(type_calculation)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
