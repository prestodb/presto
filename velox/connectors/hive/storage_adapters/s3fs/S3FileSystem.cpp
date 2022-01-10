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
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

namespace facebook::velox {
namespace {
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
        outcome, "Failed to initialize S3 file", bucket_, key_);
    length_ = outcome.GetResult().GetContentLength();
    VELOX_CHECK_GE(length_, 0);
  }

  std::string_view pread(uint64_t offset, uint64_t length, Arena* arena)
      const override {
    char* position = arena->reserve(length);
    preadInternal(offset, length, position);
    return {position, length};
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer)
      const override {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(uint64_t offset, uint64_t length) const override {
    // TODO: use allocator that doesn't initialize memory?
    std::string result(length, 0);
    char* position = result.data();
    preadInternal(offset, length, position);
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    VELOX_NYI();
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
    // TODO: Avoid copy below by using  req.SetResponseStreamFactory();
    // Reference: ARROW-8692
    auto outcome = client_->GetObject(request);
    VELOX_CHECK_AWS_OUTCOME(
        outcome, "Failure in S3ReadFile::preadInternal", bucket_, key_);

    result = std::move(outcome).GetResultWithOwnership();
    auto& stream = result.GetBody();
    stream.read(reinterpret_cast<char*>(position), length);
  }

  Aws::S3::S3Client* client_;
  std::string bucket_;
  std::string key_;
  int64_t length_ = -1;
};
} // namespace

namespace filesystems {

class S3Config {
 public:
  S3Config(const Config* config) : config_(config) {}

  // Virtual addressing is used for AWS S3 and is the default (path-style-access
  // is false). Path access style is used for some on-prem systems like Minio.
  bool useVirtualAddressing() const {
    return !config_->get("hive.s3.path-style-access", false);
  }

  bool useSSL() const {
    return config_->get("hive.s3.ssl.enabled", true);
  }

  bool useInstanceCredentials() const {
    return config_->get("hive.s3.use-instance-credentials", false);
  }

  std::string endpoint() const {
    return config_->get("hive.s3.endpoint", std::string(""));
  }

  std::optional<std::string> accessKey() const {
    if (config_->isValueExists("hive.s3.aws-access-key")) {
      return config_->get("hive.s3.aws-access-key").value();
    }
    return {};
  }

  std::optional<std::string> secretKey() const {
    if (config_->isValueExists("hive.s3.aws-secret-key")) {
      return config_->get("hive.s3.aws-secret-key").value();
    }
    return {};
  }

 private:
  const Config* config_;
};

class S3FileSystem::Impl {
 public:
  Impl(const Config* config) : s3Config_(config) {
    const size_t origCount = initCounter_++;
    if (origCount == 0) {
      Aws::SDKOptions awsOptions;
      awsOptions.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Fatal;
      Aws::InitAPI(awsOptions);
    }
  }

  ~Impl() {
    const size_t newCount = --initCounter_;
    if (newCount == 0) {
      Aws::SDKOptions awsOptions;
      awsOptions.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Fatal;
      Aws::ShutdownAPI(awsOptions);
    }
  }

  // Configure default AWS credentials provider chain.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
  getDefaultCredentialProvider() const {
    return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
  }

  // Configure with access and secret keys.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
  getAccessSecretCredentialProvider(
      const std::string& accessKey,
      const std::string& secretKey) const {
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
        awsString(accessKey), awsString(secretKey), awsString(""));
  }

  // Use the input Config parameters and initialize the S3Client.
  void initializeClient() {
    Aws::Client::ClientConfiguration clientConfig;

    clientConfig.endpointOverride = s3Config_.endpoint();

    if (s3Config_.useSSL()) {
      clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    } else {
      clientConfig.scheme = Aws::Http::Scheme::HTTP;
    }

    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentialsProvider;
    auto accessKey = s3Config_.accessKey();
    auto secretKey = s3Config_.secretKey();
    if (accessKey.has_value() && secretKey.has_value() &&
        !s3Config_.useInstanceCredentials()) {
      credentialsProvider = getAccessSecretCredentialProvider(
          accessKey.value(), secretKey.value());
    } else {
      credentialsProvider = getDefaultCredentialProvider();
    }

    client_ = std::make_shared<Aws::S3::S3Client>(
        credentialsProvider,
        clientConfig,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        s3Config_.useVirtualAddressing());
  }

  // Make it clear that the S3FileSystem instance owns the S3Client.
  // Once the S3FileSystem is destroyed, the S3Client fails to work
  // due to the Aws::ShutdownAPI invocation in the destructor.
  Aws::S3::S3Client* s3Client() const {
    return client_.get();
  }

 private:
  const S3Config s3Config_;
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

std::unique_ptr<ReadFile> S3FileSystem::openFileForRead(std::string_view path) {
  const std::string file = s3Path(path);
  auto s3file = std::make_unique<S3ReadFile>(file, impl_->s3Client());
  s3file->initialize();
  return s3file;
}

std::unique_ptr<WriteFile> S3FileSystem::openFileForWrite(
    std::string_view path) {
  VELOX_NYI();
}

std::string S3FileSystem::name() const {
  return "S3";
}

static std::function<std::shared_ptr<FileSystem>(std::shared_ptr<const Config>)>
    filesystemGenerator = [](std::shared_ptr<const Config> properties) {
      // Only one instance of S3FileSystem is supported for now.
      // TODO: Support multiple S3FileSystem instances using a cache
      // Initialize on first access and reuse after that.
      static std::shared_ptr<FileSystem> s3fs;
      folly::call_once(S3FSInstantiationFlag, [&properties]() {
        auto fs = std::make_shared<S3FileSystem>(properties);
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
