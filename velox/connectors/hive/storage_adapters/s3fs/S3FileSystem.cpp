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

#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/core/Context.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <memory>
#include <stdexcept>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

namespace facebook::velox {
namespace {
// Reference: https://issues.apache.org/jira/browse/ARROW-8692
// https://github.com/apache/arrow/blob/master/cpp/src/arrow/filesystem/s3fs.cc#L843
// A non-copying iostream. See
// https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf,
                         public std::iostream {
 public:
  StringViewStream(const void* data, int64_t nbytes)
      : Aws::Utils::Stream::PreallocatedStreamBuf(
            reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
            static_cast<size_t>(nbytes)),
        std::iostream(this) {}
};

// By default, the AWS SDK reads object data into an auto-growing StringStream.
// To avoid copies, read directly into a pre-allocated buffer instead.
// See https://github.com/aws/aws-sdk-cpp/issues/64 for an alternative but
// functionally similar recipe.
Aws::IOStreamFactory AwsWriteableStreamFactory(void* data, int64_t nbytes) {
  return [=]() { return Aws::New<StringViewStream>("", data, nbytes); };
}

class S3ReadFile final : public ReadFile {
 public:
  S3ReadFile(const std::string& path, Aws::S3::S3Client* client)
      : client_(client) {
    bucketAndKeyFromS3Path(path, bucket_, key_);
  }

  // Gets the length of the file.
  // Checks if there are any issues reading the file.
  void initialize() {
    // Make it a no-op if invoked twice.
    if (length_ != -1) {
      return;
    }

    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(awsString(bucket_));
    request.SetKey(awsString(key_));

    auto outcome = client_->HeadObject(request);
    VELOX_CHECK_AWS_OUTCOME(
        outcome, "Failed to get metadata for S3 object", bucket_, key_);
    length_ = outcome.GetResult().GetContentLength();
    VELOX_CHECK_GE(length_, 0);
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer)
      const override {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(uint64_t offset, uint64_t length) const override {
    std::string result(length, 0);
    char* position = result.data();
    preadInternal(offset, length, position);
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    // 'buffers' contains Ranges(data, size)  with some gaps (data = nullptr) in
    // between. This call must populate the ranges (except gap ranges)
    // sequentially starting from 'offset'. AWS S3 GetObject does not support
    // multi-range. AWS S3 also charges by number of read requests and not size.
    // The idea here is to use a single read spanning all the ranges and then
    // populate individual ranges. We pre-allocate a buffer to support this.
    size_t length = 0;
    for (const auto range : buffers) {
      length += range.size();
    }
    // TODO: allocate from a memory pool
    std::string result(length, 0);
    preadInternal(offset, length, static_cast<char*>(result.data()));
    size_t resultOffset = 0;
    for (auto range : buffers) {
      if (range.data()) {
        memcpy(range.data(), &(result.data()[resultOffset]), range.size());
      }
      resultOffset += range.size();
    }
    return length;
  }

  uint64_t size() const override {
    return length_;
  }

  uint64_t memoryUsage() const override {
    // TODO: Check if any buffers are being used by the S3 library
    return sizeof(Aws::S3::S3Client) + kS3MaxKeySize + 2 * sizeof(std::string) +
        sizeof(int64_t);
  }

  bool shouldCoalesce() const final {
    return false;
  }

  std::string getName() const final {
    return fmt::format("s3://{}/{}", bucket_, key_);
  }

  uint64_t getNaturalReadSize() const final {
    return 72 << 20;
  }

 private:
  // The assumption here is that "position" has space for at least "length"
  // bytes.
  void preadInternal(uint64_t offset, uint64_t length, char* position) const {
    // Read the desired range of bytes.
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::Model::GetObjectResult result;

    request.SetBucket(awsString(bucket_));
    request.SetKey(awsString(key_));
    std::stringstream ss;
    ss << "bytes=" << offset << "-" << offset + length - 1;
    request.SetRange(awsString(ss.str()));
    request.SetResponseStreamFactory(
        AwsWriteableStreamFactory(position, length));
    auto outcome = client_->GetObject(request);
    VELOX_CHECK_AWS_OUTCOME(outcome, "Failed to get S3 object", bucket_, key_);
  }

  Aws::S3::S3Client* client_;
  std::string bucket_;
  std::string key_;
  int64_t length_ = -1;
};

Aws::Utils::Logging::LogLevel inferS3LogLevel(std::string level) {
  // Convert to upper case.
  std::transform(
      level.begin(), level.end(), level.begin(), [](unsigned char c) {
        return std::toupper(c);
      });
  if (level == "FATAL") {
    return Aws::Utils::Logging::LogLevel::Fatal;
  } else if (level == "TRACE") {
    return Aws::Utils::Logging::LogLevel::Trace;
  } else if (level == "OFF") {
    return Aws::Utils::Logging::LogLevel::Off;
  } else if (level == "ERROR") {
    return Aws::Utils::Logging::LogLevel::Error;
  } else if (level == "WARN") {
    return Aws::Utils::Logging::LogLevel::Warn;
  } else if (level == "INFO") {
    return Aws::Utils::Logging::LogLevel::Info;
  } else if (level == "DEBUG") {
    return Aws::Utils::Logging::LogLevel::Debug;
  }
  return Aws::Utils::Logging::LogLevel::Fatal;
}
} // namespace

namespace filesystems {
using namespace connector::hive;
class S3FileSystem::Impl {
 public:
  Impl(const Config* config) : config_(config) {
    const size_t origCount = initCounter_++;
    if (origCount == 0) {
      Aws::SDKOptions awsOptions;
      awsOptions.loggingOptions.logLevel =
          inferS3LogLevel(HiveConfig::s3GetLogLevel(config_));
      // In some situations, curl triggers a SIGPIPE signal causing the entire
      // process to be terminated without any notification.
      // This behavior is seen via Prestissimo on AmazonLinux2 on AWS EC2.
      // Relevant documentation in AWS SDK C++
      // https://github.com/aws/aws-sdk-cpp/blob/276ee83080fcc521d41d456dbbe61d49392ddf77/src/aws-cpp-sdk-core/include/aws/core/Aws.h#L96
      // This option allows the AWS SDK C++ to catch the SIGPIPE signal and
      // log a message.
      awsOptions.httpOptions.installSigPipeHandler = true;
      Aws::InitAPI(awsOptions);
    }
  }

  ~Impl() {
    const size_t newCount = --initCounter_;
    if (newCount == 0) {
      Aws::SDKOptions awsOptions;
      awsOptions.loggingOptions.logLevel =
          inferS3LogLevel(HiveConfig::s3GetLogLevel(config_));
      Aws::ShutdownAPI(awsOptions);
    }
  }

  // Configure and return an AWSCredentialsProvider with access key and secret
  // key.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
  getAccessKeySecretKeyCredentialsProvider(
      const std::string& accessKey,
      const std::string& secretKey) const {
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
        awsString(accessKey), awsString(secretKey));
  }

  // Return a default AWSCredentialsProvider.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
  getDefaultCredentialsProvider() const {
    return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
  }

  // Configure and return an AWSCredentialsProvider with S3 IAM Role.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
  getIAMRoleCredentialsProvider(
      const std::string& s3IAMRole,
      const std::string& sessionName) const {
    return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
        awsString(s3IAMRole), awsString(sessionName));
  }

  // Return an AWSCredentialsProvider based on the config.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> getCredentialsProvider()
      const {
    auto accessKey = HiveConfig::s3AccessKey(config_);
    auto secretKey = HiveConfig::s3SecretKey(config_);
    const auto iamRole = HiveConfig::s3IAMRole(config_);

    int keyCount = accessKey.has_value() + secretKey.has_value();
    // keyCount=0 means both are not specified
    // keyCount=2 means both are specified
    // keyCount=1 means only one of them is specified and is an error
    VELOX_USER_CHECK(
        (keyCount != 1),
        "Invalid configuration: both access key and secret key must be specified");

    int configCount = (accessKey.has_value() && secretKey.has_value()) +
        iamRole.has_value() + HiveConfig::s3UseInstanceCredentials(config_);
    VELOX_USER_CHECK(
        (configCount <= 1),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");

    if (accessKey.has_value() && secretKey.has_value()) {
      return getAccessKeySecretKeyCredentialsProvider(
          accessKey.value(), secretKey.value());
    }

    if (HiveConfig::s3UseInstanceCredentials(config_)) {
      return getDefaultCredentialsProvider();
    }

    if (iamRole.has_value()) {
      return getIAMRoleCredentialsProvider(
          iamRole.value(), HiveConfig::s3IAMRoleSessionName(config_));
    }

    return getDefaultCredentialsProvider();
  }

  // Use the input Config parameters and initialize the S3Client.
  void initializeClient() {
    Aws::Client::ClientConfiguration clientConfig;

    clientConfig.endpointOverride = HiveConfig::s3Endpoint(config_);

    if (HiveConfig::s3UseSSL(config_)) {
      clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    } else {
      clientConfig.scheme = Aws::Http::Scheme::HTTP;
    }

    auto credentialsProvider = getCredentialsProvider();

    client_ = std::make_shared<Aws::S3::S3Client>(
        credentialsProvider,
        clientConfig,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        HiveConfig::s3UseVirtualAddressing(config_));
  }

  // Make it clear that the S3FileSystem instance owns the S3Client.
  // Once the S3FileSystem is destroyed, the S3Client fails to work
  // due to the Aws::ShutdownAPI invocation in the destructor.
  Aws::S3::S3Client* s3Client() const {
    return client_.get();
  }

  std::string getLogLevelName() const {
    return GetLogLevelName(inferS3LogLevel(HiveConfig::s3GetLogLevel(config_)));
  }

 private:
  const Config* config_;
  std::shared_ptr<Aws::S3::S3Client> client_;
  static std::atomic<size_t> initCounter_;
};

std::atomic<size_t> S3FileSystem::Impl::initCounter_(0);
folly::once_flag S3FSInstantiationFlag;

S3FileSystem::S3FileSystem(std::shared_ptr<const Config> config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

void S3FileSystem::initializeClient() {
  impl_->initializeClient();
}

std::string S3FileSystem::getLogLevelName() const {
  return impl_->getLogLevelName();
}

std::unique_ptr<ReadFile> S3FileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& /*unused*/) {
  const std::string file = s3Path(path);
  auto s3file = std::make_unique<S3ReadFile>(file, impl_->s3Client());
  s3file->initialize();
  return s3file;
}

std::unique_ptr<WriteFile> S3FileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  VELOX_NYI();
}

std::string S3FileSystem::name() const {
  return "S3";
}

static std::function<std::shared_ptr<FileSystem>(
    std::shared_ptr<const Config>,
    std::string_view)>
    filesystemGenerator = [](std::shared_ptr<const Config> properties,
                             std::string_view filePath) {
      // Only one instance of S3FileSystem is supported for now.
      // TODO: Support multiple S3FileSystem instances using a cache
      // Initialize on first access and reuse after that.
      static std::shared_ptr<FileSystem> s3fs;
      folly::call_once(S3FSInstantiationFlag, [&properties]() {
        std::shared_ptr<S3FileSystem> fs;
        if (properties != nullptr) {
          fs = std::make_shared<S3FileSystem>(properties);
        } else {
          fs = std::make_shared<S3FileSystem>(
              std::make_shared<core::MemConfig>());
        }
        fs->initializeClient();
        s3fs = fs;
      });
      return s3fs;
    };

void registerS3FileSystem() {
  registerFileSystem(isS3File, filesystemGenerator);
}

} // namespace filesystems
} // namespace facebook::velox
