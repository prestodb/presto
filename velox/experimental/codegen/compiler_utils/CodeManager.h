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

#include "velox/experimental/codegen/compiler_utils/Compiler.h"
#include "velox/experimental/codegen/compiler_utils/CompilerOptions.h"
#include "velox/experimental/codegen/library_loader/NativeLibraryLoader.h"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"

namespace facebook::velox::codegen {

using compiler_utils::Compiler;
using compiler_utils::CompilerOptions;
using native_loader::NativeLibraryLoader;
/// Manages the life cycle of a generated code
/// Right now this is simply a combo of a compiler/loader object
class CodeManager {
 public:
  explicit CodeManager(DefaultScopedTimer::EventSequence& eventSequence)
      : compiler_(eventSequence), loader_(), eventSequence_(eventSequence) {}
  CodeManager(
      const CompilerOptions& options,
      DefaultScopedTimer::EventSequence& eventSequence)
      : compiler_(options, eventSequence),
        loader_(),
        eventSequence_(eventSequence) {}
  compiler_utils::Compiler& compiler() {
    return compiler_;
  };

  NativeLibraryLoader& loader() {
    return loader_;
  }

 private:
  Compiler compiler_;
  NativeLibraryLoader loader_;
  DefaultScopedTimer::EventSequence eventSequence_;
};
} // namespace facebook::velox::codegen
