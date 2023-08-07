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
#include <gtest/gtest.h>
#include <fstream>
#include "velox/common/base/Fs.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/expression/VectorFunction.h"

using namespace facebook::velox;

namespace facebook::presto::test {
namespace {

class RemoteFunctionRegistererTest : public testing::Test {
 public:
  void writeToFile(const std::string& filePath, std::string_view data) {
    std::ofstream outputFile(filePath, std::ofstream::binary);
    outputFile.write(data.data(), data.size());
    outputFile.close();
  }
};

TEST_F(RemoteFunctionRegistererTest, singleFile) {
  std::string_view json = R"(
  {
    "udfSignatureMap": {
      "mock1": [
        {
          "outputType": "varchar",
          "paramTypes": [
            "varchar"
          ],
          "schema": "mock_schema"
        }
      ],
      "mock2": [
        {
          "outputType": "boolean",
          "paramTypes": [],
          "schema": "mock_schema"
        }
      ]
    }
  })";

  // Write to a single output file.
  auto path = exec::test::TempFilePath::create();
  writeToFile(path->path, json);

  // Check functions do not exist first.
  EXPECT_TRUE(exec::getVectorFunctionSignatures("mock1") == std::nullopt);
  EXPECT_TRUE(exec::getVectorFunctionSignatures("mock2") == std::nullopt);

  // Read and register functions in that file.
  EXPECT_EQ(registerRemoteFunctions(path->path, {}), 2);
  EXPECT_TRUE(exec::getVectorFunctionSignatures("mock1") != std::nullopt);
  EXPECT_TRUE(exec::getVectorFunctionSignatures("mock2") != std::nullopt);
}

std::string getJson(const std::string& functionName) {
  return fmt::format(
      "{{ \"udfSignatureMap\": {{ \"{}\": [ "
      "  {{ \"outputType\": \"boolean\",\"paramTypes\": [],"
      "     \"schema\": \"mock_schema\" }}"
      " ] }} }}",
      functionName);
}

TEST_F(RemoteFunctionRegistererTest, directory) {
  auto tempDir = exec::test::TempDirectoryPath::create();

  // Create the following structure:
  //
  // $ tmpDir/remote1.json
  // $ tmpDir/remote2.json
  // $ tmpDir/subdir/remote3.json
  //
  // to ensure files can be read recursively.
  writeToFile(tempDir->path + "/remote1.json", getJson("mock1"));
  writeToFile(tempDir->path + "/remote2.json", getJson("mock2"));

  const std::string tempSubdir = tempDir->path + "/subdir";
  fs::create_directory(tempSubdir);
  writeToFile(tempSubdir + "/remote3.json", getJson("mock3"));

  EXPECT_EQ(registerRemoteFunctions(tempDir->path, {}), 3);
}

} // namespace
} // namespace facebook::presto::test
