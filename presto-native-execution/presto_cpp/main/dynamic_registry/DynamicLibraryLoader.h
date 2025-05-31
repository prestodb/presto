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

#include <fstream>
#include "presto_cpp/main/JsonSignatureParser.h"
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Fs.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::presto {

using fnSignaturePtrMap = std::
    unordered_map<std::string, std::vector<velox::exec::FunctionSignaturePtr>>;

class DynamicLibraryLoader {
 public:
  DynamicLibraryLoader(const fs::path& filePath, const std::string& pluginDir)
      : base(fs::path(pluginDir)),
        defaultPrefix(
            SystemConfig::instance()->prestoDefaultNamespacePrefix()) {
    initializeFunctionMapsFromConfig(filePath);
  }

  // Makes a final comparison and reports errors on discrepencies between the
  // actual registrations and config.
  int64_t compareConfigWithRegisteredFunctionSignatures(
      const facebook::velox::FunctionSignatureMap& fnSignaturesBefore);

  fnSignaturePtrMap getFunctionMap() const {
    return *(functionMap_.rlock());
  }

  std::unordered_map<std::string, std::string> getEntrypointMap() const {
    return *(entrypointMap_.rlock());
  }

  bool loadDynamicFunctions();

 private:
  void initializeFunctionMapsFromConfig(const fs::path& filePath);

  std::optional<std::string> constructAbsoluteFilePath(
      const std::string& subDirName,
      const std::string& fileName);

  std::string prestoFunctionName(
      const std::string& baseFunctionName,
      const std::string& nameSpace);

  const fs::path base;
  const std::string defaultPrefix;
  // To group possible functions with the same name but different
  // signatures.
  folly::Synchronized<std::unordered_map<
      std::string,
      std::vector<velox::exec::FunctionSignaturePtr>>>
      functionMap_;

  // subdir/filename -> vector of non-duplicate entrypoints
  folly::Synchronized<std::unordered_map<std::string, std::string>>
      entrypointMap_;
};
} // namespace facebook::presto
