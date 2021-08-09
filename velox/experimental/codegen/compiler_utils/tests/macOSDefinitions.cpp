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

#include <string>
#include "velox/experimental/codegen/compiler_utils/tests/definitions.h"
namespace facebook {
namespace velox {
namespace codegen {
namespace compiler_utils {
namespace test {
const std::string CLANG_PATH = "/usr/bin/clang";
const std::string LD_PATH = "/usr/local/ld.lld";
const std::string FMT_INCLUDE = "/usr/local/include/";
const std::string FMT_LIB = "/usr/local/lib/libfmt.dylib";

compiler_utils::CompilerOptions testCompilerOptions() {
  return compiler_utils::CompilerOptions()
      .withCompilerPath(CLANG_PATH)
      .withOptimizationLevel("-O3")
      .withTempDirectory("/tmp")
      .withExtraCompileOptions({"-std=c++17", "-fPIC", "-g"})
      .withFormatterPath("/usr/local/bin/clang-format")
      .withExtraLinkOptions({"-Wl,-flat_namespace"});
  ;
}
} // namespace test
} // namespace compiler_utils
} // namespace codegen
} // namespace velox
} // namespace facebook
