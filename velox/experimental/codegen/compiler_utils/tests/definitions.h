/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include "velox/experimental/codegen/compiler_utils/CompilerOptions.h"
namespace facebook::velox::codegen::compiler_utils::test {

extern const std::string CLANG_PATH;
extern const std::string LD_PATH;
extern const std::string FMT_INCLUDE;
extern const std::string FMT_LIB;

compiler_utils::CompilerOptions testCompilerOptions();
} // namespace facebook::velox::codegen::compiler_utils::test
