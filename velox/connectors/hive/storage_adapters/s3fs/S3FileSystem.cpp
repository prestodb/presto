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
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3WriteFile.h"
#include "velox/core/QueryConfig.h"
#include "velox/dwio/common/DataBuffer.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <memory>
#include <stdexcept>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/AdaptiveRetryStrategy.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

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
    getBucketAndKeyFromS3Path(path, bucket_, key_);
  }

  // Gets the length of the file.
  // Checks if there are any issues reading the file.
  void initialize(const filesystems::FileOptions& options) {
    if (options.fileSize.has_value()) {
      VELOX_CHECK_GE(
          options.fileSize.value(), 0, "File size must be non-negative");
      length_ = options.fileSize.value();
    }

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

class S3WriteFile::Impl {
 public:
  explicit Impl(
      const std::string& path,
      Aws::S3::S3Client* client,
      memory::MemoryPool* pool)
      : client_(client), pool_(pool) {
    VELOX_CHECK_NOT_NULL(client);
    VELOX_CHECK_NOT_NULL(pool);
    getBucketAndKeyFromS3Path(path, bucket_, key_);
    currentPart_ = std::make_unique<dwio::common::DataBuffer<char>>(*pool_);
    currentPart_->reserve(kPartUploadSize);
    // Check that the object doesn't exist, if it does throw an error.
    {
      Aws::S3::Model::HeadObjectRequest request;
      request.SetBucket(awsString(bucket_));
      request.SetKey(awsString(key_));
      auto objectMetadata = client_->HeadObject(request);
      VELOX_CHECK(!objectMetadata.IsSuccess(), "S3 object already exists");
    }

    // Create bucket if not present.
    {
      Aws::S3::Model::HeadBucketRequest request;
      request.SetBucket(awsString(bucket_));
      auto bucketMetadata = client_->HeadBucket(request);
      if (!bucketMetadata.IsSuccess()) {
        Aws::S3::Model::CreateBucketRequest request;
        request.SetBucket(bucket_);
        auto outcome = client_->CreateBucket(request);
        VELOX_CHECK_AWS_OUTCOME(
            outcome, "Failed to create S3 bucket", bucket_, "");
      }
    }

    // Initiate the multi-part upload.
    {
      Aws::S3::Model::CreateMultipartUploadRequest request;
      request.SetBucket(awsString(bucket_));
      request.SetKey(awsString(key_));

      /// If we do not set anything then the SDK will default to application/xml
      /// which confuses some tools
      /// (https://github.com/apache/arrow/issues/11934). So we instead default
      /// to application/octet-stream which is less misleading.
      request.SetContentType(kApplicationOctetStream);
      // The default algorithm used is MD5. However, MD5 is not supported with
      // fips and can cause a SIGSEGV. Set CRC32 instead which is a standard for
      // checksum computation and is not restricted by fips.
      request.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::CRC32);

      auto outcome = client_->CreateMultipartUpload(request);
      VELOX_CHECK_AWS_OUTCOME(
          outcome, "Failed initiating multiple part upload", bucket_, key_);
      uploadState_.id = outcome.GetResult().GetUploadId();
    }

    fileSize_ = 0;
  }

  // Appends data to the end of the file.
  void append(std::string_view data) {
    VELOX_CHECK(!closed(), "File is closed");
    if (data.size() + currentPart_->size() >= kPartUploadSize) {
      upload(data);
    } else {
      // Append to current part.
      currentPart_->unsafeAppend(data.data(), data.size());
    }
    fileSize_ += data.size();
  }

  // No-op.
  void flush() {
    VELOX_CHECK(!closed(), "File is closed");
    /// currentPartSize must be less than kPartUploadSize since
    /// append() would have already flushed after reaching kUploadPartSize.
    VELOX_CHECK_LT(currentPart_->size(), kPartUploadSize);
  }

  // Complete the multipart upload and close the file.
  void close() {
    if (closed()) {
      return;
    }
    uploadPart({currentPart_->data(), currentPart_->size()}, true);
    VELOX_CHECK_EQ(uploadState_.partNumber, uploadState_.completedParts.size());
    // Complete the multipart upload.
    {
      Aws::S3::Model::CompletedMultipartUpload completedUpload;
      completedUpload.SetParts(uploadState_.completedParts);
      Aws::S3::Model::CompleteMultipartUploadRequest request;
      request.SetBucket(awsString(bucket_));
      request.SetKey(awsString(key_));
      request.SetUploadId(uploadState_.id);
      request.SetMultipartUpload(std::move(completedUpload));

      auto outcome = client_->CompleteMultipartUpload(request);
      VELOX_CHECK_AWS_OUTCOME(
          outcome, "Failed to complete multiple part upload", bucket_, key_);
    }
    currentPart_->clear();
  }

  // Current file size, i.e. the sum of all previous appends.
  uint64_t size() const {
    return fileSize_;
  }

  int numPartsUploaded() const {
    return uploadState_.partNumber;
  }

 private:
  static constexpr int64_t kPartUploadSize = 10 * 1024 * 1024;
  static constexpr const char* kApplicationOctetStream =
      "application/octet-stream";

  bool closed() const {
    return (currentPart_->capacity() == 0);
  }

  // Holds state for the multipart upload.
  struct UploadState {
    Aws::Vector<Aws::S3::Model::CompletedPart> completedParts;
    int64_t partNumber = 0;
    Aws::String id;
  };
  UploadState uploadState_;

  // Data can be smaller or larger than the kPartUploadSize.
  // Complete the currentPart_ and upload kPartUploadSize chunks of data.
  // Save the remaining into currentPart_.
  void upload(const std::string_view data) {
    auto dataPtr = data.data();
    auto dataSize = data.size();
    // Fill-up the remaining currentPart_.
    auto remainingBufferSize = currentPart_->capacity() - currentPart_->size();
    currentPart_->unsafeAppend(dataPtr, remainingBufferSize);
    uploadPart({currentPart_->data(), currentPart_->size()});
    dataPtr += remainingBufferSize;
    dataSize -= remainingBufferSize;
    while (dataSize > kPartUploadSize) {
      uploadPart({dataPtr, kPartUploadSize});
      dataPtr += kPartUploadSize;
      dataSize -= kPartUploadSize;
    }
    // Stash the remaining at the beginning of currentPart.
    currentPart_->unsafeAppend(0, dataPtr, dataSize);
  }

  void uploadPart(const std::string_view part, bool isLast = false) {
    // Only the last part can be less than kPartUploadSize.
    VELOX_CHECK(isLast || (!isLast && (part.size() == kPartUploadSize)));
    // Upload the part.
    {
      Aws::S3::Model::UploadPartRequest request;
      request.SetBucket(bucket_);
      request.SetKey(key_);
      request.SetUploadId(uploadState_.id);
      request.SetPartNumber(++uploadState_.partNumber);
      request.SetContentLength(part.size());
      request.SetBody(
          std::make_shared<StringViewStream>(part.data(), part.size()));
      // The default algorithm used is MD5. However, MD5 is not supported with
      // fips and can cause a SIGSEGV. Set CRC32 instead which is a standard for
      // checksum computation and is not restricted by fips.
      request.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::CRC32);
      auto outcome = client_->UploadPart(request);
      VELOX_CHECK_AWS_OUTCOME(outcome, "Failed to upload", bucket_, key_);
      // Append ETag and part number for this uploaded part.
      // This will be needed for upload completion in Close().
      auto result = outcome.GetResult();
      Aws::S3::Model::CompletedPart part;

      part.SetPartNumber(uploadState_.partNumber);
      part.SetETag(result.GetETag());
      // Don't add the checksum to the part if the checksum is empty.
      // Some filesystems such as IBM COS require this to be not set.
      if (!result.GetChecksumCRC32().empty()) {
        part.SetChecksumCRC32(result.GetChecksumCRC32());
      }
      uploadState_.completedParts.push_back(std::move(part));
    }
  }

  Aws::S3::S3Client* client_;
  memory::MemoryPool* pool_;
  std::unique_ptr<dwio::common::DataBuffer<char>> currentPart_;
  std::string bucket_;
  std::string key_;
  size_t fileSize_ = -1;
};

S3WriteFile::S3WriteFile(
    const std::string& path,
    Aws::S3::S3Client* client,
    memory::MemoryPool* pool) {
  impl_ = std::make_shared<Impl>(path, client, pool);
}

void S3WriteFile::append(std::string_view data) {
  return impl_->append(data);
}

void S3WriteFile::flush() {
  impl_->flush();
}

void S3WriteFile::close() {
  impl_->close();
}

uint64_t S3WriteFile::size() const {
  return impl_->size();
}

int S3WriteFile::numPartsUploaded() const {
  return impl_->numPartsUploaded();
}

using namespace connector::hive;

// Initialize and Finalize the AWS SDK C++ library.
// Initialization must be done before creating a S3FileSystem.
// Finalization must be done after all S3FileSystem instances have been deleted.
// After Finalize, no new S3FileSystem can be created.
struct AwsInstance {
  AwsInstance() : isInitialized_(false), isFinalized_(false) {}
  ~AwsInstance() {
    finalize(/*from_destructor=*/true);
  }

  // Returns true iff the instance was newly initialized with config.
  bool initialize(const config::ConfigBase* config) {
    if (isFinalized_.load()) {
      VELOX_FAIL("Attempt to initialize S3 after it has been finalized.");
    }
    if (!isInitialized_.exchange(true)) {
      // Not already initialized.
      doInitialize(config);
      return true;
    }
    return false;
  }

  bool isInitialized() {
    return !isFinalized_ && isInitialized_;
  }

  void finalize(bool fromDestructor = false) {
    if (isFinalized_.exchange(true)) {
      // Already finalized.
      return;
    }
    if (isInitialized_.exchange(false)) {
      // Was initialized.
      if (fromDestructor) {
        VLOG(0)
            << "finalizeS3FileSystem() was not called even though S3 was initialized."
               "This could lead to a segmentation fault at exit";
      }
      Aws::ShutdownAPI(awsOptions_);
    }
  }

  std::string getLogLevelName() {
    return Aws::Utils::Logging::GetLogLevelName(
        awsOptions_.loggingOptions.logLevel);
  }

 private:
  void doInitialize(const config::ConfigBase* config) {
    std::shared_ptr<HiveConfig> hiveConfig = std::make_shared<HiveConfig>(
        std::make_shared<config::ConfigBase>(config->rawConfigsCopy()));
    awsOptions_.loggingOptions.logLevel =
        inferS3LogLevel(hiveConfig->s3GetLogLevel());
    // In some situations, curl triggers a SIGPIPE signal causing the entire
    // process to be terminated without any notification.
    // This behavior is seen via Prestissimo on AmazonLinux2 on AWS EC2.
    // Relevant documentation in AWS SDK C++
    // https://github.com/aws/aws-sdk-cpp/blob/276ee83080fcc521d41d456dbbe61d49392ddf77/src/aws-cpp-sdk-core/include/aws/core/Aws.h#L96
    // This option allows the AWS SDK C++ to catch the SIGPIPE signal and
    // log a message.
    awsOptions_.httpOptions.installSigPipeHandler = true;
    Aws::InitAPI(awsOptions_);
  }

  Aws::SDKOptions awsOptions_;
  std::atomic<bool> isInitialized_;
  std::atomic<bool> isFinalized_;
};

// Singleton to initialize AWS S3.
AwsInstance* getAwsInstance() {
  static auto instance = std::make_unique<AwsInstance>();
  return instance.get();
}

bool initializeS3(const config::ConfigBase* config) {
  return getAwsInstance()->initialize(config);
}

static std::atomic<int> fileSystemCount = 0;

void finalizeS3() {
  VELOX_CHECK((fileSystemCount == 0), "Cannot finalize S3 while in use");
  getAwsInstance()->finalize();
}

class S3FileSystem::Impl {
 public:
  Impl(const config::ConfigBase* config) {
    hiveConfig_ = std::make_shared<HiveConfig>(
        std::make_shared<config::ConfigBase>(config->rawConfigsCopy()));
    VELOX_CHECK(getAwsInstance()->isInitialized(), "S3 is not initialized");
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.endpointOverride = hiveConfig_->s3Endpoint();

    if (hiveConfig_->s3UseProxyFromEnv()) {
      auto proxyConfig = S3ProxyConfigurationBuilder(hiveConfig_->s3Endpoint())
                             .useSsl(hiveConfig_->s3UseSSL())
                             .build();
      if (proxyConfig.has_value()) {
        clientConfig.proxyScheme = Aws::Http::SchemeMapper::FromString(
            proxyConfig.value().scheme().c_str());
        clientConfig.proxyHost = awsString(proxyConfig.value().host());
        clientConfig.proxyPort = proxyConfig.value().port();
        clientConfig.proxyUserName = awsString(proxyConfig.value().username());
        clientConfig.proxyPassword = awsString(proxyConfig.value().password());
      }
    }

    if (hiveConfig_->s3UseSSL()) {
      clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    } else {
      clientConfig.scheme = Aws::Http::Scheme::HTTP;
    }

    if (hiveConfig_->s3ConnectTimeout().has_value()) {
      clientConfig.connectTimeoutMs =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              facebook::velox::config::toDuration(
                  hiveConfig_->s3ConnectTimeout().value()))
              .count();
    }

    if (hiveConfig_->s3SocketTimeout().has_value()) {
      clientConfig.requestTimeoutMs =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              facebook::velox::config::toDuration(
                  hiveConfig_->s3SocketTimeout().value()))
              .count();
    }

    if (hiveConfig_->s3MaxConnections().has_value()) {
      clientConfig.maxConnections = hiveConfig_->s3MaxConnections().value();
    }

    auto retryStrategy = getRetryStrategy();
    if (retryStrategy.has_value()) {
      clientConfig.retryStrategy = retryStrategy.value();
    }

    auto credentialsProvider = getCredentialsProvider();

    client_ = std::make_shared<Aws::S3::S3Client>(
        credentialsProvider,
        clientConfig,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        hiveConfig_->s3UseVirtualAddressing());
    ++fileSystemCount;
  }

  ~Impl() {
    client_.reset();
    --fileSystemCount;
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
    auto accessKey = hiveConfig_->s3AccessKey();
    auto secretKey = hiveConfig_->s3SecretKey();
    const auto iamRole = hiveConfig_->s3IAMRole();

    int keyCount = accessKey.has_value() + secretKey.has_value();
    // keyCount=0 means both are not specified
    // keyCount=2 means both are specified
    // keyCount=1 means only one of them is specified and is an error
    VELOX_USER_CHECK(
        (keyCount != 1),
        "Invalid configuration: both access key and secret key must be specified");

    int configCount = (accessKey.has_value() && secretKey.has_value()) +
        iamRole.has_value() + hiveConfig_->s3UseInstanceCredentials();
    VELOX_USER_CHECK(
        (configCount <= 1),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");

    if (accessKey.has_value() && secretKey.has_value()) {
      return getAccessKeySecretKeyCredentialsProvider(
          accessKey.value(), secretKey.value());
    }

    if (hiveConfig_->s3UseInstanceCredentials()) {
      return getDefaultCredentialsProvider();
    }

    if (iamRole.has_value()) {
      return getIAMRoleCredentialsProvider(
          iamRole.value(), hiveConfig_->s3IAMRoleSessionName());
    }

    return getDefaultCredentialsProvider();
  }

  // Return a client RetryStrategy based on the config.
  std::optional<std::shared_ptr<Aws::Client::RetryStrategy>> getRetryStrategy()
      const {
    auto retryMode = hiveConfig_->s3RetryMode();
    auto maxAttempts = hiveConfig_->s3MaxAttempts();
    if (retryMode.has_value()) {
      if (retryMode.value() == "standard") {
        if (maxAttempts.has_value()) {
          VELOX_USER_CHECK_GE(
              maxAttempts.value(),
              0,
              "Invalid configuration: specified 'hive.s3.max-attempts' value {} is < 0.",
              maxAttempts.value());
          return std::make_shared<Aws::Client::StandardRetryStrategy>(
              maxAttempts.value());
        } else {
          // Otherwise, use default value 3.
          return std::make_shared<Aws::Client::StandardRetryStrategy>();
        }
      } else if (retryMode.value() == "adaptive") {
        if (maxAttempts.has_value()) {
          VELOX_USER_CHECK_GE(
              maxAttempts.value(),
              0,
              "Invalid configuration: specified 'hive.s3.max-attempts' value {} is < 0.",
              maxAttempts.value());
          return std::make_shared<Aws::Client::AdaptiveRetryStrategy>(
              maxAttempts.value());
        } else {
          // Otherwise, use default value 3.
          return std::make_shared<Aws::Client::AdaptiveRetryStrategy>();
        }
      } else if (retryMode.value() == "legacy") {
        if (maxAttempts.has_value()) {
          VELOX_USER_CHECK_GE(
              maxAttempts.value(),
              0,
              "Invalid configuration: specified 'hive.s3.max-attempts' value {} is < 0.",
              maxAttempts.value());
          return std::make_shared<Aws::Client::DefaultRetryStrategy>(
              maxAttempts.value());
        } else {
          // Otherwise, use default value maxRetries = 10, scaleFactor = 25
          return std::make_shared<Aws::Client::DefaultRetryStrategy>();
        }
      } else {
        VELOX_USER_FAIL("Invalid retry mode for S3: {}", retryMode.value());
      }
    }
    return std::nullopt;
  }

  // Make it clear that the S3FileSystem instance owns the S3Client.
  // Once the S3FileSystem is destroyed, the S3Client fails to work
  // due to the Aws::ShutdownAPI invocation in the destructor.
  Aws::S3::S3Client* s3Client() const {
    return client_.get();
  }

  std::string getLogLevelName() const {
    return getAwsInstance()->getLogLevelName();
  }

 private:
  std::shared_ptr<HiveConfig> hiveConfig_;
  std::shared_ptr<Aws::S3::S3Client> client_;
};

S3FileSystem::S3FileSystem(std::shared_ptr<const config::ConfigBase> config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

std::string S3FileSystem::getLogLevelName() const {
  return impl_->getLogLevelName();
}

std::unique_ptr<ReadFile> S3FileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& options) {
  const auto file = s3Path(path);
  auto s3file = std::make_unique<S3ReadFile>(file, impl_->s3Client());
  s3file->initialize(options);
  return s3file;
}

std::unique_ptr<WriteFile> S3FileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& options) {
  const auto file = s3Path(path);
  auto s3file =
      std::make_unique<S3WriteFile>(file, impl_->s3Client(), options.pool);
  return s3file;
}

std::string S3FileSystem::name() const {
  return "S3";
}

} // namespace filesystems
} // namespace facebook::velox
