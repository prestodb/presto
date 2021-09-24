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
add_subdirectory(aggregates)
add_subdirectory(buffer)
add_subdirectory(common)
add_subdirectory(duckdb)
add_subdirectory(connectors)
add_subdirectory(core)
add_subdirectory(dwio)
add_subdirectory(examples)
add_subdirectory(exec)
add_subdirectory(expression)
add_subdirectory(external/date)
add_subdirectory(external/duckdb)
add_subdirectory(external/md5)
add_subdirectory(functions)
add_subdirectory(parse)
add_subdirectory(serializers)
add_subdirectory(type)
add_subdirectory(vector)
add_subdirectory(row)
add_subdirectory(flag_definitions)
add_subdirectory(codegen)
if(${CODEGEN_SUPPORT})
  add_subdirectory(experimental/codegen)
endif()
