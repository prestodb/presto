/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "velox/connectors/hive/storage_adapters/gcs/GCSFileSystem.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/storage_adapters/gcs/GCSUtil.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include <boost/process.hpp>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <google/cloud/storage/client.h>
#include "gtest/gtest.h"
#include "gtest/internal/custom/gtest.h"

namespace bp = boost::process;
namespace gc = google::cloud;
namespace gcs = google::cloud::storage;
constexpr char const* kTestBenchPort{"9001"};

const std::string kLoremIpsum =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"
    "incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis "
    "nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu"
    "fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in"
    "culpa qui officia deserunt mollit anim id est laborum.";

class GcsTestbench : public testing::Environment {
 public:
  GcsTestbench() : port_(kTestBenchPort) {
    std::vector<std::string> names{"python3", "python"};
    // If the build script or application developer provides a value in the
    // PYTHON environment variable, then just use that.
    if (const auto* env = std::getenv("PYTHON")) {
      names = {env};
    }
    auto error = std::string(
        "Coud not start GCS emulator."
        " Used the following list of python interpreter names:");
    for (const auto& interpreter : names) {
      auto exe_path = bp::search_path(interpreter);
      error += " " + interpreter;
      if (exe_path.empty()) {
        error += " (exe not found)";
        continue;
      }

      server_process_ = bp::child(
          boost::this_process::environment(),
          exe_path,
          "-m",
          "testbench",
          "--port",
          port_,
          group_);
      if (server_process_.valid() && server_process_.running())
        break;
      error += " (failed to start)";
      server_process_.terminate();
      server_process_.wait();
    }
    if (server_process_.valid() && server_process_.valid())
      return;
    error_ = std::move(error);
  }

  ~GcsTestbench() override {
    // Brutal shutdown, kill the full process group because the GCS testbench
    // may launch additional children.
    group_.terminate();
    if (server_process_.valid()) {
      server_process_.wait();
    }
  }

  const std::string& port() const {
    return port_;
  }

  const std::string& error() const {
    return error_;
  }

 private:
  std::string port_;
  bp::child server_process_;
  bp::group group_;
  std::string error_;
};

using namespace facebook::velox;

class GCSFileSystemTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (testbench_ == nullptr) {
      testbench_ = std::make_shared<GcsTestbench>();
    }

    ASSERT_THAT(testbench_, ::testing::NotNull());
    ASSERT_THAT(testbench_->error(), ::testing::IsEmpty());

    // Create a bucket and a small file in the testbench. This makes it easier
    // to bootstrap GcsFileSystem and its tests.
    auto client = gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>(
                "http://localhost:" + testbench_->port())
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials()));

    bucket_name_ = "test1-gcs";
    google::cloud::StatusOr<gcs::BucketMetadata> bucket =
        client.CreateBucketForProject(
            bucket_name_, "ignored-by-testbench", gcs::BucketMetadata{});
    ASSERT_TRUE(bucket.ok()) << "Failed to create bucket <" << bucket_name_
                             << ">, status=" << bucket.status();

    object_name_ = "test-object-name";
    google::cloud::StatusOr<gcs::ObjectMetadata> object =
        client.InsertObject(bucket_name_, object_name_, kLoremIpsum);
    ASSERT_TRUE(object.ok()) << "Failed to create object <" << object_name_
                             << ">, status=" << object.status();
  }

  std::shared_ptr<const config::ConfigBase> testGcsOptions() const {
    std::unordered_map<std::string, std::string> configOverride = {};

    configOverride["hive.gcs.scheme"] = "http";
    configOverride["hive.gcs.endpoint"] = "localhost:" + testbench_->port();
    return std::make_shared<const config::ConfigBase>(
        std::move(configOverride));
  }

  std::string preexistingBucketName() {
    return bucket_name_;
  }

  std::string preexistingBucketPath() {
    return bucket_name_ + '/';
  }

  std::string preexistingObjectName() {
    return object_name_;
  }

  std::string preexistingObjectPath() {
    return preexistingBucketPath() + preexistingObjectName();
  }
  static void TearDownTestSuite() {}

  static std::shared_ptr<GcsTestbench> testbench_;
  static std::string bucket_name_;
  static std::string object_name_;
};

std::shared_ptr<GcsTestbench> GCSFileSystemTest::testbench_ =
    nullptr; // will be destroyed on destructor
std::string GCSFileSystemTest::bucket_name_;
std::string GCSFileSystemTest::object_name_;

TEST_F(GCSFileSystemTest, readFile) {
  const std::string gcsFile =
      gcsURI(preexistingBucketName(), preexistingObjectName());

  filesystems::GCSFileSystem gcfs(testGcsOptions());
  gcfs.initializeClient();
  auto readFile = gcfs.openFileForRead(gcsFile);
  std::int64_t size = readFile->size();
  std::int64_t ref_size = kLoremIpsum.length();
  EXPECT_EQ(size, ref_size);
  EXPECT_EQ(readFile->pread(0, size), kLoremIpsum);

  char buffer1[size];
  ASSERT_EQ(readFile->pread(0, size, &buffer1), kLoremIpsum);
  ASSERT_EQ(readFile->size(), ref_size);

  char buffer2[50];
  ASSERT_EQ(readFile->pread(10, 50, &buffer2), kLoremIpsum.substr(10, 50));
  ASSERT_EQ(readFile->size(), ref_size);

  EXPECT_EQ(readFile->pread(10, size - 10), kLoremIpsum.substr(10));

  char buff1[10];
  char buff2[20];
  char buff3[30];
  std::vector<folly::Range<char*>> buffers = {
      folly::Range<char*>(buff1, 10),
      folly::Range<char*>(nullptr, 20),
      folly::Range<char*>(buff2, 20),
      folly::Range<char*>(nullptr, 30),
      folly::Range<char*>(buff3, 30)};
  ASSERT_EQ(10 + 20 + 20 + 30 + 30, readFile->preadv(0, buffers));
  ASSERT_EQ(std::string_view(buff1, sizeof(buff1)), kLoremIpsum.substr(0, 10));
  ASSERT_EQ(std::string_view(buff2, sizeof(buff2)), kLoremIpsum.substr(30, 20));
  ASSERT_EQ(std::string_view(buff3, sizeof(buff3)), kLoremIpsum.substr(80, 30));
}

TEST_F(GCSFileSystemTest, writeAndReadFile) {
  const std::string newFile = "readWriteFile.txt";
  const std::string gcsFile = gcsURI(preexistingBucketName(), newFile);

  filesystems::GCSFileSystem gcfs(testGcsOptions());
  gcfs.initializeClient();
  auto writeFile = gcfs.openFileForWrite(gcsFile);
  std::string dataContent =
      "Dance me to your beauty with a burning violin"
      "Dance me through the panic till I'm gathered safely in"
      "Lift me like an olive branch and be my homeward dove"
      "Dance me to the end of love";

  EXPECT_EQ(writeFile->size(), 0);
  std::int64_t contentSize = dataContent.length();
  writeFile->append(dataContent.substr(0, 10));
  EXPECT_EQ(writeFile->size(), 10);
  writeFile->append(dataContent.substr(10, contentSize - 10));
  EXPECT_EQ(writeFile->size(), contentSize);
  writeFile->flush();
  writeFile->close();
  VELOX_ASSERT_THROW(
      writeFile->append(dataContent.substr(0, 10)), "File is not open");

  auto readFile = gcfs.openFileForRead(gcsFile);
  std::int64_t size = readFile->size();
  EXPECT_EQ(readFile->size(), contentSize);
  EXPECT_EQ(readFile->pread(0, size), dataContent);
}

TEST_F(GCSFileSystemTest, openExistingFileForWrite) {
  const std::string newFile = "readWriteFile.txt";
  const std::string gcsFile = gcsURI(preexistingBucketName(), newFile);

  filesystems::GCSFileSystem gcfs(testGcsOptions());
  gcfs.initializeClient();
  VELOX_ASSERT_THROW(gcfs.openFileForWrite(gcsFile), "File already exists");
}

TEST_F(GCSFileSystemTest, renameNotImplemented) {
  const char* file = "newTest.txt";
  const std::string gcsExistingFile =
      gcsURI(preexistingBucketName(), preexistingObjectName());
  const std::string gcsNewFile = gcsURI(preexistingBucketName(), file);
  filesystems::GCSFileSystem gcfs(testGcsOptions());
  gcfs.initializeClient();
  gcfs.openFileForRead(gcsExistingFile);
  VELOX_ASSERT_THROW(
      gcfs.rename(gcsExistingFile, gcsNewFile, true),
      "rename for GCS not implemented");
}

TEST_F(GCSFileSystemTest, mkdirNotImplemented) {
  const char* dir = "newDirectory";
  const std::string gcsNewDirectory = gcsURI(preexistingBucketName(), dir);
  filesystems::GCSFileSystem gcfs(testGcsOptions());
  gcfs.initializeClient();
  VELOX_ASSERT_THROW(
      gcfs.mkdir(gcsNewDirectory), "mkdir for GCS not implemented");
}

TEST_F(GCSFileSystemTest, rmdirNotImplemented) {
  const char* dir = "Directory";
  const std::string gcsDirectory = gcsURI(preexistingBucketName(), dir);
  filesystems::GCSFileSystem gcfs(testGcsOptions());
  gcfs.initializeClient();
  VELOX_ASSERT_THROW(gcfs.rmdir(gcsDirectory), "rmdir for GCS not implemented");
}

TEST_F(GCSFileSystemTest, missingFile) {
  const char* file = "newTest.txt";
  const std::string gcsFile = gcsURI(preexistingBucketName(), file);
  filesystems::GCSFileSystem gcfs(testGcsOptions());
  gcfs.initializeClient();
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      gcfs.openFileForRead(gcsFile),
      error_code::kFileNotFound,
      "\\\"message\\\": \\\"Live version of object test1-gcs/newTest.txt does not exist.\\\"");
}

TEST_F(GCSFileSystemTest, missingBucket) {
  filesystems::GCSFileSystem gcfs(testGcsOptions());
  gcfs.initializeClient();
  const char* gcsFile = "gs://dummy/foo.txt";
  VELOX_ASSERT_RUNTIME_THROW_CODE(
      gcfs.openFileForRead(gcsFile),
      error_code::kFileNotFound,
      "\\\"message\\\": \\\"Bucket dummy does not exist.\\\"");
}

TEST_F(GCSFileSystemTest, credentialsConfig) {
  std::unordered_map<std::string, std::string> configOverride = {};

  // credentials from arrow gcsfs test case
  // While this service account key has the correct format, it cannot be used
  // for authentication because the key has been deactivated on the server-side,
  // *and* the account(s) involved are deleted *and* they are not the accounts
  // or projects do not match its contents.
  auto creds = R"""({
      "type": "service_account",
      "project_id": "foo-project",
      "private_key_id": "a1a111aa1111a11a11a11aa111a111a1a1111111",
      "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFA"
      "ASCBKcwggSjAgEAAoIBAQCltiF2oP3KJJ+S\ntTc1McylY+TuAi3AdohX7mmqIjd8a3eBYDHs7"
      "FlnUrFC4CRijCr0rUqYfg2pmk4a\n6TaKbQRAhWDJ7XD931g7EBvCtd8+JQBNWVKnP9ByJUaO0h"
      "WVniM50KTsWtyX3up/\nfS0W2R8Cyx4yvasE8QHH8gnNGtr94iiORDC7De2BwHi/iU8FxMVJAIyD"
      "LNfyk0hN\neheYKfIDBgJV2v6VaCOGWaZyEuD0FJ6wFeLybFBwibrLIBE5Y/StCrZoVZ5LocFP\n"
      "T4o8kT7bU6yonudSCyNMedYmqHj/iF8B2UN1WrYx8zvoDqZk0nxIglmEYKn/6U7U\ngyETGcW9Ag"
      "MBAAECggEAC231vmkpwA7JG9UYbviVmSW79UecsLzsOAZnbtbn1VLT\nPg7sup7tprD/LXHoyIxK7S"
      "/jqINvPU65iuUhgCg3Rhz8+UiBhd0pCH/arlIdiPuD\n2xHpX8RIxAq6pGCsoPJ0kwkHSw8UTnxPV8Z"
      "CPSRyHV71oQHQgSl/WjNhRi6PQroB\nSqc/pS1m09cTwyKQIopBBVayRzmI2BtBxyhQp9I8t5b7PYkE"
      "ZDQlbdq0j5Xipoov\n9EW0+Zvkh1FGNig8IJ9Wp+SZi3rd7KLpkyKPY7BK/g0nXBkDxn019cET0SdJOH"
      "QG\nDiHiv4yTRsDCHZhtEbAMKZEpku4WxtQ+JjR31l8ueQKBgQDkO2oC8gi6vQDcx/CX\nZ23x2ZUyar"
      "6i0BQ8eJFAEN+IiUapEeCVazuxJSt4RjYfwSa/p117jdZGEWD0GxMC\n+iAXlc5LlrrWs4MWUc0AHTgX"
      "na28/vii3ltcsI0AjWMqaybhBTTNbMFa2/fV2OX2\nUimuFyBWbzVc3Zb9KAG4Y7OmJQKBgQC5324IjX"
      "Pq5oH8UWZTdJPuO2cgRsvKmR/r\n9zl4loRjkS7FiOMfzAgUiXfH9XCnvwXMqJpuMw2PEUjUT+OyWjJO"
      "NEK4qGFJkbN5\n3ykc7p5V7iPPc7Zxj4mFvJ1xjkcj+i5LY8Me+gL5mGIrJ2j8hbuv7f+PWIauyjnp\n"
      "Nx/0GVFRuQKBgGNT4D1L7LSokPmFIpYh811wHliE0Fa3TDdNGZnSPhaD9/aYyy78\nLkxYKuT7WY7UVv"
      "LN+gdNoVV5NsLGDa4cAV+CWPfYr5PFKGXMT/Wewcy1WOmJ5des\nAgMC6zq0TdYmMBN6WpKUpEnQtbmh"
      "3eMnuvADLJWxbH3wCkg+4xDGg2bpAoGAYRNk\nMGtQQzqoYNNSkfus1xuHPMA8508Z8O9pwKU795R3zQ"
      "s1NAInpjI1sOVrNPD7Ymwc\nW7mmNzZbxycCUL/yzg1VW4P1a6sBBYGbw1SMtWxun4ZbnuvMc2CTCh+43"
      "/1l+FHe\nMmt46kq/2rH2jwx5feTbOE6P6PINVNRJh/9BDWECgYEAsCWcH9D3cI/QDeLG1ao7\nrE2Nckn"
      "P8N783edM07Z/zxWsIsXhBPY3gjHVz2LDl+QHgPWhGML62M0ja/6SsJW3\nYvLLIc82V7eqcVJTZtaFkuh"
      "t68qu/Jn1ezbzJMJ4YXDYo1+KFi+2CAGR06QILb+I\nlUtj+/nH3HDQjM4ltYfTPUg=\n"
      "-----END PRIVATE KEY-----\n",
      "client_email": "foo-email@foo-project.iam.gserviceaccount.com",
      "client_id": "100000000000000000001",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/foo-email%40foo-project.iam.gserviceaccount.com"
  })""";
  auto jsonFile = exec::test::TempFilePath::create();
  std::ofstream credsOut(jsonFile->getPath());
  credsOut << creds;
  credsOut.close();
  configOverride["hive.gcs.json-key-file-path"] = jsonFile->getPath();
  configOverride["hive.gcs.scheme"] = "http";
  configOverride["hive.gcs.endpoint"] = "localhost:" + testbench_->port();
  std::shared_ptr<const config::ConfigBase> conf =
      std::make_shared<const config::ConfigBase>(std::move(configOverride));

  filesystems::GCSFileSystem gcfs(conf);
  gcfs.initializeClient();
  try {
    const std::string gcsFile =
        gcsURI(preexistingBucketName(), preexistingObjectName());
    gcfs.openFileForRead(gcsFile);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_THAT(
        err.message(), testing::HasSubstr("gs://test1-gcs/test-object-name"));
    EXPECT_THAT(
        err.message(), testing::HasSubstr("Invalid ServiceAccountCredentials"));
  }
}
