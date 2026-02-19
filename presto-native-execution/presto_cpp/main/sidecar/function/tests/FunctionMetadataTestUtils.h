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

#include <algorithm>
#include <string>

#include <gtest/gtest.h>
#include <folly/hash/Hash.h>

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/types/tests/TestUtils.h"

namespace facebook::presto::test::function {

using json = nlohmann::json;

inline void sortMetadataList(json::array_t& list) {
  for (auto& metadata : list) {
    // Sort constraint arrays for deterministic test comparisons.
    for (auto const& [key, val] : metadata.items()) {
      if (key.ends_with("Constraints") && metadata[key].is_array()) {
        std::sort(
            metadata[key].begin(),
            metadata[key].end(),
            [](const json& a, const json& b) { return a.dump() < b.dump(); });
      }
    }
  }
  std::sort(list.begin(), list.end(), [](const json& a, const json& b) {
    return folly::hasher<std::string>()(
               a["functionKind"].dump() + a["paramTypes"].dump()) <
        folly::hasher<std::string>()(
               b["functionKind"].dump() + b["paramTypes"].dump());
  });
}

// Shared test helper that loads expected metadata from disk and compares it to
// the provided metadata list for the given function name.
inline void testFunctionMetadata(
  const json& functionMetadata,
  const std::string& name,
  const std::string& expectedFile,
  size_t expectedSize) {
  json::array_t metadataList = functionMetadata.at(name);
  EXPECT_EQ(metadataList.size(), expectedSize);
  std::string expectedStr = slurp(test::utils::getDataPath(
    "/github/presto-trunk/presto-native-execution/presto_cpp/main/sidecar/function/tests/data/",
    expectedFile));
  auto expected = json::parse(expectedStr);

  json::array_t expectedList = expected[name];
  sortMetadataList(expectedList);
  sortMetadataList(metadataList);
  for (size_t i = 0; i < expectedSize; i++) {
    EXPECT_EQ(expectedList[i], metadataList[i]) << "Position: " << i;
  }
}

} // namespace facebook::presto::test::function
