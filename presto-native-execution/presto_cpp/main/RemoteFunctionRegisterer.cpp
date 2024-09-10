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

#include "presto_cpp/main/RemoteFunctionRegisterer.h"
#include <fstream>
#include "presto_cpp/main/JsonSignatureParser.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Fs.h"
#include "velox/functions/remote/client/Remote.h"

namespace facebook::presto {
namespace {

using velox::functions::remote::PageFormat;

std::string genFunctionName(
    const std::string& baseFunctionName,
    const std::string& schemaName,
    const std::string_view& prefix) {
  auto name = schemaName.empty()
      ? baseFunctionName
      : fmt::format("{}.{}", schemaName, baseFunctionName);

  return prefix.empty() ? name : fmt::format("{}.{}", prefix, name);
}

PageFormat fromSerdeString(const std::string_view& serdeName) {
  if (serdeName == "presto_page") {
    return PageFormat::PRESTO_PAGE;
  } else if (serdeName == "spark_unsafe_row") {
    return PageFormat::SPARK_UNSAFE_ROW;
  } else {
    VELOX_FAIL(
        "Unknown serde name for remote function server: '{}'", serdeName);
  }
}

// Reads file at `filePath`, decodes the json signatures and registers them as
// remote functions pointing to `location`. Returns the number of signatures
// registered.
size_t processFile(
    const fs::path& filePath,
    const folly::SocketAddress& location,
    const std::string_view& prefix,
    const std::string_view& serde) {
  std::ifstream stream{filePath};
  std::stringstream buffer;
  buffer << stream.rdbuf();

  velox::functions::RemoteVectorFunctionMetadata metadata;
  metadata.location = location;
  metadata.serdeFormat = fromSerdeString(serde);

  // First group possible functions with the same name but different
  // schemas.
  std::
      unordered_map<std::string, std::vector<velox::exec::FunctionSignaturePtr>>
          functionMap;

  for (const auto& it : JsonSignatureParser(buffer.str())) {
    for (const auto& item : it.second) {
      functionMap[genFunctionName(it.first, item.schema, prefix)].emplace_back(
          item.signature);
    }
  }

  size_t signaturesCount = 0;

  // Register signatures in Velox.
  for (const auto& it : functionMap) {
    velox::functions::registerRemoteFunction(it.first, it.second, metadata);
    signaturesCount += it.second.size();
  }
  return signaturesCount;
}

} // namespace

size_t registerRemoteFunctions(
    const std::string& inputPath,
    const folly::SocketAddress& location,
    const std::string_view& prefix,
    const std::string_view& serde) {
  size_t signaturesCount = 0;
  const fs::path path{inputPath};

  if (fs::is_directory(path)) {
    for (auto& entryPath : fs::recursive_directory_iterator(path)) {
      if (entryPath.is_regular_file()) {
        signaturesCount += processFile(entryPath, location, prefix, serde);
      }
    }
  } else if (fs::is_regular_file(path)) {
    signaturesCount += processFile(path, location, prefix, serde);
  }
  return signaturesCount;
}

} // namespace facebook::presto
