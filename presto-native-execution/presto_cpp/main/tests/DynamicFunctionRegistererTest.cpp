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

#include <gtest/gtest.h>
#include <fstream>
#include "presto_cpp/main/dynamic_registry/DynamicFunctionConfigRegisterer.h"
#include "presto_cpp/main/dynamic_registry/DynamicFunctionRegistrar.h"
#include "presto_cpp/main/dynamic_registry/DynamicSignalHandler.h"
#include "velox/common/base/Fs.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/tests/RegistryTestUtil.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#ifndef DYNAMIC_LIB_FILE_EXT
#error DYNAMIC_LIB_FILE_EXT must be defined in CMAKE
#endif
const char* kFileExtension = DYNAMIC_LIB_FILE_EXT;

using namespace facebook::velox;
namespace facebook::presto::test {
namespace {
class DynamicFunctionRegistererTest : public testing::Test {
 public:
  template <typename T>
  struct TestDynamicFunctionMock {
    VELOX_DEFINE_FUNCTION_TYPES(T);
    FOLLY_ALWAYS_INLINE bool call(out_type<bool>& result) {
      result = "123";
      return true;
    }
  };
  template <typename T>
  struct TestDynamicFunctionMock2 {
    VELOX_DEFINE_FUNCTION_TYPES(T);
    FOLLY_ALWAYS_INLINE bool call(
        out_type<Varchar>& result,
        const arg_type<Varchar>& in) {
      result = in;
      return true;
    }
  };
  void UdfBoolDynamicFnRegistration(
      std::string fn_name,
      std::string nameSpace = "") {
    facebook::presto::registerPrestoFunction<TestDynamicFunctionMock, bool>(
        fn_name.c_str(), nameSpace.c_str());
  }
  void UdfVarcharDynamicFnRegistration(
      std::string fn_name,
      std::string nameSpace = "") {
    facebook::presto::registerPrestoFunction<
        TestDynamicFunctionMock2,
        facebook::velox::Varchar,
        facebook::velox::Varchar>(fn_name.c_str(), nameSpace.c_str());
  }

  void writeToFile(const std::string& filePath, std::string_view data) {
    std::ofstream outputFile(filePath, std::ofstream::binary);
    outputFile.write(data.data(), data.size());
    outputFile.close();
  }

 private:
  std::shared_ptr<SystemConfig> systemConfig;
};

TEST_F(DynamicFunctionRegistererTest, twoFunctions) {
  std::string_view json = R"(
  {
    "dynamicLibrariesUdfMap": {
      "tmp": {
        "mock1": [
          {
            "entrypoint": "registry123",
            "fileName" : "test",
            "outputType": "boolean",
            "paramTypes": [
            ]
          }
        ],
        "mock2": [
          {
            "fileName" : "test",
            "outputType": "varchar",
            "paramTypes": [
              "varchar"
            ]
          }
        ]
      }
    }
  })";

  // Emulate user provided config file.
  auto path = exec::test::TempFilePath::create();
  auto pathStr = path->getPath();
  std::filesystem::path fsPath(pathStr);
  auto fileName = fsPath.filename().string();
  std::ofstream{fsPath.parent_path() / "test.so"}.put('a');

  // Create dummy shared library to emulate the shared library path validation.
  auto filePathForDummySharedFile = exec::test::TempFilePath::create();
  std::filesystem::path sharedFilePath(filePathForDummySharedFile->getPath());
  auto dummySharedFile = sharedFilePath.replace_extension(kFileExtension);

  writeToFile(pathStr, json);

  // Check functions do not exist first.
  auto registeredFnSignaturesBefore = velox::getFunctionSignatures();
  EXPECT_TRUE(
      registeredFnSignaturesBefore.find("presto.default.mock1") ==
      registeredFnSignaturesBefore.end());
  EXPECT_TRUE(
      registeredFnSignaturesBefore.find("presto.default.mock2") ==
      registeredFnSignaturesBefore.end());

  DynamicLibraryValidator testDynamicLibraryValidator(path->getPath(), "/");

  // Read and register functions in that file. Emulating the dynamic loading
  // process.
  UdfBoolDynamicFnRegistration("mock1");
  UdfVarcharDynamicFnRegistration("mock2");

  auto registeredFnSignaturesAfter = velox::getFunctionSignatures();
  EXPECT_TRUE(
      registeredFnSignaturesAfter.find("presto.default.mock1") !=
      registeredFnSignaturesAfter.end());
  EXPECT_TRUE(
      registeredFnSignaturesAfter.find("presto.default.mock2") !=
      registeredFnSignaturesAfter.end());

  // Config validations.
  EXPECT_EQ(
      testDynamicLibraryValidator.compareConfigWithRegisteredFunctionSignatures(
          registeredFnSignaturesBefore),
      0);
}

TEST_F(DynamicFunctionRegistererTest, newNamespace) {
  std::string_view json = R"(
  {
    "dynamicLibrariesUdfMap": {
      "tmp": {
        "mock3": [
          {
            "fileName": "test",
            "nameSpace": "test.namespace",
            "outputType": "boolean",
            "paramTypes": [
            ]
          }
        ]
      }
    }
  })";

  // Emulate user provided config file.
  auto path = exec::test::TempFilePath::create();
  auto pathStr = path->getPath();
  std::filesystem::path fsPath(pathStr);
  auto fileName = fsPath.filename().string();
  std::ofstream{fsPath.parent_path() / "test.so"}.put('a');

  // Create dummy shared library to emulate the shared library path validation.
  auto filePathForDummySharedFile = exec::test::TempFilePath::create();
  std::filesystem::path sharedFilePath(filePathForDummySharedFile->getPath());
  auto strDummySharedFile =
      sharedFilePath.replace_extension(kFileExtension).string();

  // Write to a single output file.
  writeToFile(pathStr, json);

  // Check functions do not exist first.
  auto registeredFnSignaturesBefore = velox::getFunctionSignatures();
  EXPECT_TRUE(
      registeredFnSignaturesBefore.find("test.namespace.mock3") ==
      registeredFnSignaturesBefore.end());

  DynamicLibraryValidator testDynamicLibraryValidator(path->getPath(), "/");

  // Read and register functions in that file. Emulating the dynamic loading
  // process
  UdfBoolDynamicFnRegistration("mock3", "test.namespace");

  // EXPECT_EQ(velox::loadDynamicLibrary(path->getPath(), "registry123"), );
  auto registeredFnSignaturesAfter = velox::getFunctionSignatures();
  EXPECT_TRUE(
      registeredFnSignaturesAfter.find("test.namespace.mock3") !=
      registeredFnSignaturesAfter.end());

  // A return value greater than 0 indicates discrepency between the
  // registerations and config.
  EXPECT_EQ(
      testDynamicLibraryValidator.compareConfigWithRegisteredFunctionSignatures(
          registeredFnSignaturesBefore),
      0);
}

} // namespace
} // namespace facebook::presto::test
