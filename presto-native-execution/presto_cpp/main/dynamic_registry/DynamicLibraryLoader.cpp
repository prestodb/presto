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

#include "presto_cpp/main/dynamic_registry/DynamicLibraryLoader.h"

#include <fstream>

#include "presto_cpp/main/JsonSignatureParser.h"
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Fs.h"
#include "velox/common/dynamic_registry/DynamicLibraryLoader.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::presto {
namespace {
#ifndef DYNAMIC_LIB_FILE_EXT
#error DYNAMIC_LIB_FILE_EXT must be defined in CMAKE
#endif
namespace fs = std::filesystem;

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

std::string DynamicLibraryLoader::constructAbsoluteFilePath(
    const std::string& subDirName,
    const std::string& fileName) {
  const fs::path subDir(subDirName);
  fs::path candidate;
  fs::path inputPath(fileName);
  const char* kFileExtension = DYNAMIC_LIB_FILE_EXT;

  // If fileName already has an extension, check if it is of type kFileExtension
  if (inputPath.has_extension()) {
    auto ext = inputPath.extension().string();
    if (ext == kFileExtension) {
      candidate = fmt::format("{}/{}/{}", this->base, subDirName, fileName);
    } else {
      candidate.clear();
    }
  } else {
    // Check if filepath exists with valid extension appended.
    candidate = fmt::format(
        "{}/{}/{}{}", this->base, subDirName, fileName, kFileExtension);
  }
  // Return candidate only if valid shared library match.
  if (!candidate.empty() && fs::exists(candidate) &&
      !fs::is_directory(candidate)) {
    return candidate.string();
  }
  LOG(ERROR) << "The file path "
             << fmt::format(
                    "{}/{}/{}{}",
                    this->base.string(),
                    subDir.string(),
                    fileName,
                    kFileExtension)
             << " is invalid and will therefore not be read";
  return "";
}

int64_t DynamicLibraryLoader::compareConfigWithRegisteredFunctionSignatures(
    const facebook::velox::FunctionSignatureMap& fnSignaturesBefore) {
  int64_t missingConfigRegistrations = 0;
  auto lockedFunctionMap = *(functionMap_.wlock());
  for (const auto& it : lockedFunctionMap) {
    LOG(INFO) << "Checking function: " << it.first;
    auto registeredFnSignaturesAfter =
        velox::exec::simpleFunctions().getFunctionSignatures(it.first);
    std::vector<facebook::velox::exec::FunctionSignaturePtr> emptyMap = {};
    auto registeredFnSignaturesBefore =
        fnSignaturesBefore.find(it.first) != fnSignaturesBefore.end()
        ? it.second
        : emptyMap;
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
      }
    }
  }
  LOG(ERROR) << "Found " << missingConfigRegistrations
             << " missing config registrations";
  return missingConfigRegistrations;
}

std::string DynamicLibraryLoader::prestoFunctionName(
    const std::string& baseFunctionName,
    const std::string& nameSpace) {
  std::string name;
  if (nameSpace.empty()) {
    name = fmt::format("{}{}", this->defaultPrefix, baseFunctionName);
  } else {
    name = fmt::format("{}.{}", nameSpace, baseFunctionName);
  }
  return name;
}

void DynamicLibraryLoader::initializeFunctionMapsFromConfig(
    const fs::path& filePath) {
  if (this->base.empty() || fs::is_empty(filePath)) {
    return;
  }
  std::ifstream stream{filePath};
  std::stringstream buffer;
  buffer << stream.rdbuf();
  LOG(INFO) << "Processing config file located at: " << filePath.c_str();

  for (const auto& it : JsonSignatureParser(
           buffer.str(), JsonSignatureScope::DynamiclibrariesUdf)) {
    for (const auto& function : it.second) {
      std::string filePath =
          constructAbsoluteFilePath(function.subDirectory, function.fileName);
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

bool DynamicLibraryLoader::loadDynamicFunctions() {
  auto filenameAndEntrypointMap = *(entrypointMap_.rlock());
  auto registeredFnSignaturesBefore = velox::getFunctionSignatures();
  for (const auto& entryPointItr : filenameAndEntrypointMap) {
    auto absoluteFilePath = entryPointItr.first;
    auto entrypoint = entryPointItr.second;
    // Only load dynamic library for signatures provided by the entrypoint
    // in a particular config file
    velox::loadDynamicLibrary(absoluteFilePath, entrypoint);
  }
  auto missedConfigRegistrations =
      compareConfigWithRegisteredFunctionSignatures(
          registeredFnSignaturesBefore);
  if (missedConfigRegistrations > 0) {
    LOG(ERROR) << "Config file has " << missedConfigRegistrations
               << " extra signatures that were not registered";
    return false;
  }
  return true;
}

} // namespace facebook::presto
