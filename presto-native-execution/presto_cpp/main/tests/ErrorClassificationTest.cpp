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

#include "presto_cpp/main/common/Exception.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/gcs/GcsFileSystem.h"

namespace facebook::presto::test {

namespace {

class ErrorClassificationTest : public ::testing::Test {
};

TEST_F(ErrorClassificationTest, gcsFileNotFound) {
#ifdef VELOX_ENABLE_GCS
  auto fs = std::make_shared<velox::filesystems::GcsFileSystem>(
                std::make_shared<velox::config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
  fs->initializeClient();

   bool isThrownHiveFileNotFound{false};
   try {
     velox::filesystems::FileOptions fileOptions;
     fs->openFileForRead("gs://nonexistent-bucket/nonexistent-file.parquet", fileOptions);
     FAIL() << "Expected an exception";
   } catch (const velox::VeloxException& e) {
     auto translatedError = VeloxToPrestoExceptionTranslator::translate(e);
     EXPECT_EQ(translatedError.errorCode.code, presto_error_code::kHiveFileNotFoundCode);
     EXPECT_EQ(translatedError.errorCode.name, presto_error_name::kHiveFileNotFoundName);
     EXPECT_EQ(translatedError.errorCode.type, protocol::ErrorType::EXTERNAL);
     isThrownHiveFileNotFound = true;
   }
   EXPECT_TRUE(isThrownHiveFileNotFound);

  bool isThrownHiveCannotOpenSplit{false};
  try {
    velox::filesystems::FileOptions fileOptions;
    // We use an invalid path to test the HIVE_CANNOT_OPEN_SPLIT error, because
    // currently there are only two types error, when call checkGcsStatus during
    // GCS operations, one is HIVE_FILE_NOT_FOUND and the other one is HIVE_CANNOT_OPEN_SPLIT
    fs->openFileForRead("hahaha", fileOptions);
    FAIL() << "Expected an exception";
  } catch (const velox::VeloxException& e) {
    auto translatedError = VeloxToPrestoExceptionTranslator::translate(e);
    EXPECT_EQ(translatedError.errorCode.code, presto_error_code::kHiveCannotOpenSplitCode);
    EXPECT_EQ(translatedError.errorCode.name, presto_error_name::kHiveCannotOpenSplitName);
    EXPECT_EQ(translatedError.errorCode.type, protocol::ErrorType::EXTERNAL);
    isThrownHiveCannotOpenSplit = true;
  }
  EXPECT_TRUE(isThrownHiveCannotOpenSplit);
#endif
}

}

}
