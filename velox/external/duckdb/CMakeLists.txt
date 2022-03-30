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
add_subdirectory(tpch)

add_compile_definitions(DISABLE_DUCKDB_REMOTE_INSTALL)
add_compile_definitions(BUILD_PARQUET_EXTENSION)
# Certain Clang compiler versions throw "deprecated declarations" warnings for
# DuckDB's parquet-amalgamation.h. This warning is from the Thrift library
# that DuckDB embeds and is also on Thrift master.
# The workaround, for now, is to disable this warning if the compiler is Clang.
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wno-deprecated-declarations)
endif()
add_library(duckdb duckdb.cpp parquet-amalgamation.cpp)

set_property(TARGET duckdb PROPERTY JOB_POOL_COMPILE high_memory_pool)
