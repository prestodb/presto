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

velox_add_library(
  velox_functions_prestosql
  ArrayConcatRegistration.cpp
  ArrayFunctionsRegistration.cpp
  ArrayNGramsRegistration.cpp
  BinaryFunctionsRegistration.cpp
  BitwiseFunctionsRegistration.cpp
  CheckedArithmeticRegistration.cpp
  ComparisonFunctionsRegistration.cpp
  DateTimeFunctionsRegistration.cpp
  GeneralFunctionsRegistration.cpp
  HyperLogFunctionsRegistration.cpp
  JsonFunctionsRegistration.cpp
  MapFunctionsRegistration.cpp
  MathematicalFunctionsRegistration.cpp
  MathematicalOperatorsRegistration.cpp
  ProbabilityTrigonometricFunctionsRegistration.cpp
  RegistrationFunctions.cpp
  StringFunctionsRegistration.cpp
  URLFunctionsRegistration.cpp)

# GCC 12 has a bug where it does not respect "pragma ignore" directives and ends
# up failing compilation in an openssl header included by a hash-related
# function.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND NOT VELOX_MONO_LIBRARY)
  target_compile_options(velox_functions_prestosql
                         PRIVATE -Wno-deprecated-declarations)
endif()

velox_link_libraries(velox_functions_prestosql velox_functions_prestosql_impl
                     velox_is_null_functions simdjson::simdjson)

if(NOT VELOX_MONO_LIBRARY)
  set_property(TARGET velox_functions_prestosql PROPERTY JOB_POOL_COMPILE
                                                         high_memory_pool)
endif()
