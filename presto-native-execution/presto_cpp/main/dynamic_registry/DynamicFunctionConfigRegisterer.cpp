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

#include "presto_cpp/main/dynamic_registry/DynamicFunctionConfigRegisterer.h"
#include <fstream>
#include "presto_cpp/main/JsonSignatureParser.h"
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Fs.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::presto {
namespace {
#ifndef DYNAMIC_LIB_FILE_EXT
#error DYNAMIC_LIB_FILE_EXT must be defined in CMAKE
#endif
namespace fs = std::filesystem;
std::string constructAbsoluteFilePath(
    const std::string& subDirName,
    const std::string& fileName,
    const std::string& pluginDir) {
  const fs::path base(pluginDir);
  const fs::path subDir(subDirName);
  fs::path candidate;
  fs::path inputPath(fileName);
  const char* kFileExtension = DYNAMIC_LIB_FILE_EXT;

  // If fileName already has an extension, check if it is of type kFileExtension
  if (inputPath.has_extension()) {
    auto ext = inputPath.extension().string();
    if (ext == kFileExtension) {
      candidate = fmt::format("{}/{}/{}", base, subDirName, fileName);
    } else {
      candidate.clear();
    }
  } else {
    // Check if filepath exists with valid extension appended.
    candidate =
        fmt::format("{}/{}/{}{}", base, subDirName, fileName, kFileExtension);
  }
  // Return candidate only if valid shared library match.
  if (!candidate.empty() && fs::exists(candidate) &&
      !fs::is_directory(candidate)) {
    return candidate.string();
  }
  LOG(ERROR) << "The file path "
             << fmt::format(
                    "{}/{}/{}{}",
                    base.string(),
                    subDir.string(),
                    fileName,
                    kFileExtension)
             << " is invalid and will therefore not be read";
  return "";
}

bool isConfigSignatureRegistered(
    std::vector<const facebook::velox::exec::FunctionSignature*>
        registeredFnSignatures,
    facebook::velox::exec::FunctionSignaturePtr localFnSignature) {
  return std::find_if(
             registeredFnSignatures.begin(),
             registeredFnSignatures.end(),
             [&](const facebook::velox::exec::FunctionSignature* sig) {
               return *sig == *localFnSignature;
             }) != registeredFnSignatures.end();
}
} // namespace

int64_t DynamicLibraryValidator::compareConfigWithRegisteredFunctionSignatures(
    facebook::velox::FunctionSignatureMap fnSignaturesBefore) {
  int64_t missingConfigRegistrations = 0;
  auto lockedFunctionMap = *(functionMap_.wlock());
  for (const auto& it : lockedFunctionMap) {
    LOG(INFO) << "Checking function: " << it.first;
    auto registeredFnSignaturesAfter =
        velox::exec::simpleFunctions().getFunctionSignatures(it.first);
    auto registeredFnSignaturesBefore = fnSignaturesBefore[it.first];
    int64_t numNewRegistrations = registeredFnSignaturesAfter.size() -
        registeredFnSignaturesBefore.size();
    if (numNewRegistrations != it.second.size()) {
      LOG(ERROR) << it.first << " has " << numNewRegistrations
                 << " new registrations in config file, but "
                 << it.second.size() << " actual registerations";
    }

    for (int i = 0; i < it.second.size(); i++) {
      if (!isConfigSignatureRegistered(
              registeredFnSignaturesAfter, it.second[i])) {
        LOG(ERROR) << "Function " << it.first << " with config signature "
                   << it.second[i]->toString()
                   << " was not registered successfully.";
        missingConfigRegistrations++;
        continue;
      }
    }
  }
  LOG(ERROR) << "Found " << missingConfigRegistrations
             << " missing config registrations";
  return missingConfigRegistrations;
}

std::string prestoFunctionName(
    const std::string& baseFunctionName,
    const std::string& nameSpace) {
  std::string name;
  if (nameSpace.empty()) {
    auto systemConfig = SystemConfig::instance();
    std::string defaultPrefix = systemConfig->prestoDefaultNamespacePrefix();
    name = fmt::format("{}{}", defaultPrefix, baseFunctionName);
  } else {
    name = fmt::format("{}.{}", nameSpace, baseFunctionName);
  }
  return name;
}

void DynamicLibraryValidator::processConfigFile(
    const fs::path& filePath,
    const std::string& pluginDir) {
  std::ifstream stream{filePath};
  std::stringstream buffer;
  buffer << stream.rdbuf();
  LOG(INFO) << "Processing config file.";

  for (const auto& it : JsonSignatureParser(
           buffer.str(), JsonSignatureScope::DynamiclibrariesUdf)) {
    for (const auto& function : it.second) {
      std::string filePath = constructAbsoluteFilePath(
          function.subDirectory, function.fileName, pluginDir);
      if (!filePath.empty()) {
        auto fnName = prestoFunctionName(it.first, function.nameSpace);
        auto lockedFunctionMap = functionMap_.wlock();
        (*lockedFunctionMap)[fnName].emplace_back(function.signature);
        auto lockedEntrypointMap = entrypointMap_.wlock();
        (*lockedEntrypointMap)[filePath] = function.entrypoint;
      }
    }
  }
}

} // namespace facebook::presto
