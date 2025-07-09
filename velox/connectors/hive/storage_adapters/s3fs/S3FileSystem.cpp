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
#include "velox/common/base/StatsReporter.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Counters.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3ReadFile.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3WriteFile.h"
#include "velox/dwio/common/DataBuffer.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <memory>
#include <stdexcept>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/AdaptiveRetryStrategy.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>

namespace facebook::velox::filesystems {
namespace {

folly::Synchronized<
    std::unordered_map<std::string, AWSCredentialsProviderFactory>>&
credentialsProviderFactories() {
  static folly::Synchronized<
      std::unordered_map<std::string, AWSCredentialsProviderFactory>>
      factories;
  return factories;
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> getCredentialsProviderByName(
    const std::string& providerName,
    const S3Config& s3Config) {
  return credentialsProviderFactories().withRLock([&](const auto& factories) {
    const auto it = factories.find(providerName);
    VELOX_CHECK(
        it != factories.end(),
        "CredentialsProviderFactory for '{}' not registered",
        providerName);
    const auto& factory = it->second;
    return factory(s3Config);
  });
}

Aws::Utils::Logging::LogLevel inferS3LogLevel(std::string_view logLevel) {
  std::string level = std::string(logLevel);
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

// Supported values are "Always", "RequestDependent", "Never"(default).
Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy inferPayloadSign(
    std::string sign) {
  // Convert to upper case.
  std::transform(sign.begin(), sign.end(), sign.begin(), [](unsigned char c) {
    return std::toupper(c);
  });
  if (sign == "ALWAYS") {
    return Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always;
  } else if (sign == "REQUESTDEPENDENT") {
    return Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent;
  }
  return Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;
}
} // namespace

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
  bool initialize(
      std::string_view logLevel,
      std::optional<std::string_view> logLocation) {
    if (isFinalized_.load()) {
      VELOX_FAIL("Attempt to initialize S3 after it has been finalized.");
    }
    if (!isInitialized_.exchange(true)) {
      // Not already initialized.
      doInitialize(logLevel, logLocation);
      return true;
    }
    return false;
  }

  bool isInitialized() const {
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

  std::string getLogLevelName() const {
    return Aws::Utils::Logging::GetLogLevelName(
        awsOptions_.loggingOptions.logLevel);
  }

  std::string getLogPrefix() const {
    return logPrefix_;
  }

 private:
  void doInitialize(
      std::string_view logLevel,
      std::optional<std::string_view> logLocation) {
    awsOptions_.loggingOptions.logLevel = inferS3LogLevel(logLevel);
    if (logLocation.has_value()) {
      logPrefix_ = fmt::format(
          "{}{}{}",
          logLocation.value(),
          logLocation.value().back() == '/' ? "" : "/",
          Aws::DEFAULT_LOG_PREFIX);
      awsOptions_.loggingOptions.defaultLogPrefix = logPrefix_.c_str();
      VLOG(0) << "Custom S3 log location prefix: " << logPrefix_;
    }
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
  std::string logPrefix_;
};

// Singleton to initialize AWS S3.
AwsInstance* getAwsInstance() {
  static auto instance = std::make_unique<AwsInstance>();
  return instance.get();
}

bool initializeS3(
    std::string_view logLevel,
    std::optional<std::string_view> logLocation) {
  return getAwsInstance()->initialize(logLevel, logLocation);
}

static std::atomic<int> fileSystemCount = 0;

void finalizeS3() {
  VELOX_CHECK((fileSystemCount == 0), "Cannot finalize S3 while in use");
  getAwsInstance()->finalize();
}

void registerCredentialsProvider(
    const std::string& providerName,
    const AWSCredentialsProviderFactory& factory) {
  VELOX_CHECK(
      !providerName.empty(), "CredentialsProviderFactory name cannot be empty");
  credentialsProviderFactories().withWLock([&](auto& factories) {
    VELOX_CHECK(
        factories.find(providerName) == factories.end(),
        "CredentialsProviderFactory '{}' already registered",
        providerName);
    factories.insert({providerName, factory});
  });
}

class S3FileSystem::Impl {
 public:
  Impl(const S3Config& s3Config) {
    VELOX_CHECK(getAwsInstance()->isInitialized(), "S3 is not initialized");
    Aws::S3::S3ClientConfiguration clientConfig;
    if (s3Config.endpoint().has_value()) {
      clientConfig.endpointOverride = s3Config.endpoint().value();
    }

    if (s3Config.endpointRegion().has_value()) {
      clientConfig.region = s3Config.endpointRegion().value();
    }

    if (s3Config.useProxyFromEnv()) {
      auto proxyConfig =
          S3ProxyConfigurationBuilder(
              s3Config.endpoint().has_value() ? s3Config.endpoint().value()
                                              : "")
              .useSsl(s3Config.useSSL())
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

    if (s3Config.useSSL()) {
      clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    } else {
      clientConfig.scheme = Aws::Http::Scheme::HTTP;
    }

    if (s3Config.connectTimeout().has_value()) {
      clientConfig.connectTimeoutMs =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              facebook::velox::config::toDuration(
                  s3Config.connectTimeout().value()))
              .count();
    }

    if (s3Config.socketTimeout().has_value()) {
      clientConfig.requestTimeoutMs =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              facebook::velox::config::toDuration(
                  s3Config.socketTimeout().value()))
              .count();
    }

    if (s3Config.maxConnections().has_value()) {
      clientConfig.maxConnections = s3Config.maxConnections().value();
    }

    auto retryStrategy = getRetryStrategy(s3Config);
    if (retryStrategy.has_value()) {
      clientConfig.retryStrategy = retryStrategy.value();
    }

    clientConfig.useVirtualAddressing = s3Config.useVirtualAddressing();
    clientConfig.payloadSigningPolicy =
        inferPayloadSign(s3Config.payloadSigningPolicy());

    auto credentialsProvider = getCredentialsProvider(s3Config);

    client_ = std::make_shared<Aws::S3::S3Client>(
        credentialsProvider, nullptr /* endpointProvider */, clientConfig);
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
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> getCredentialsProvider(
      const S3Config& s3Config) const {
    auto credentialsProvider = s3Config.credentialsProvider();
    if (credentialsProvider.has_value()) {
      const auto& name = credentialsProvider.value();
      // Create the credentials provider using the registered factory.
      return getCredentialsProviderByName(name, s3Config);
    }

    auto accessKey = s3Config.accessKey();
    auto secretKey = s3Config.secretKey();
    const auto iamRole = s3Config.iamRole();

    int keyCount = accessKey.has_value() + secretKey.has_value();
    // keyCount=0 means both are not specified
    // keyCount=2 means both are specified
    // keyCount=1 means only one of them is specified and is an error
    VELOX_USER_CHECK(
        (keyCount != 1),
        "Invalid configuration: both access key and secret key must be specified");

    int configCount = (accessKey.has_value() && secretKey.has_value()) +
        iamRole.has_value() + s3Config.useInstanceCredentials();
    VELOX_USER_CHECK(
        (configCount <= 1),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");

    if (accessKey.has_value() && secretKey.has_value()) {
      return getAccessKeySecretKeyCredentialsProvider(
          accessKey.value(), secretKey.value());
    }

    if (s3Config.useInstanceCredentials()) {
      return getDefaultCredentialsProvider();
    }

    if (iamRole.has_value()) {
      return getIAMRoleCredentialsProvider(
          iamRole.value(), s3Config.iamRoleSessionName());
    }

    return getDefaultCredentialsProvider();
  }

  // Return a client RetryStrategy based on the config.
  std::optional<std::shared_ptr<Aws::Client::RetryStrategy>> getRetryStrategy(
      const S3Config& s3Config) const {
    auto retryMode = s3Config.retryMode();
    auto maxAttempts = s3Config.maxAttempts();
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

  std::string getLogPrefix() const {
    return getAwsInstance()->getLogPrefix();
  }

 private:
  std::shared_ptr<Aws::S3::S3Client> client_;
};

S3FileSystem::S3FileSystem(
    std::string_view bucketName,
    const std::shared_ptr<const config::ConfigBase> config)
    : FileSystem(config) {
  S3Config s3Config(bucketName, config);
  impl_ = std::make_shared<Impl>(s3Config);
}

std::string S3FileSystem::getLogLevelName() const {
  return impl_->getLogLevelName();
}

std::string S3FileSystem::getLogPrefix() const {
  return impl_->getLogPrefix();
}

std::unique_ptr<ReadFile> S3FileSystem::openFileForRead(
    std::string_view s3Path,
    const FileOptions& options) {
  const auto path = getPath(s3Path);
  auto s3file = std::make_unique<S3ReadFile>(path, impl_->s3Client());
  s3file->initialize(options);
  return s3file;
}

std::unique_ptr<WriteFile> S3FileSystem::openFileForWrite(
    std::string_view s3Path,
    const FileOptions& options) {
  const auto path = getPath(s3Path);
  auto s3file =
      std::make_unique<S3WriteFile>(path, impl_->s3Client(), options.pool);
  return s3file;
}

std::string S3FileSystem::name() const {
  return "S3";
}

std::vector<std::string> S3FileSystem::list(std::string_view path) {
  std::string bucket;
  std::string key;
  getBucketAndKeyFromPath(getPath(path), bucket, key);

  Aws::S3::Model::ListObjectsRequest request;
  request.SetBucket(awsString(bucket));
  request.SetPrefix(awsString(key));

  auto outcome = impl_->s3Client()->ListObjects(request);
  VELOX_CHECK_AWS_OUTCOME(
      outcome, "Failed to list objects in S3 bucket", bucket, key);

  std::vector<std::string> objectKeys;
  const auto& result = outcome.GetResult();
  for (const auto& object : result.GetContents()) {
    objectKeys.emplace_back(object.GetKey());
  }

  return objectKeys;
}

bool S3FileSystem::exists(std::string_view path) {
  std::string bucket;
  std::string key;
  getBucketAndKeyFromPath(getPath(path), bucket, key);

  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(awsString(bucket));
  request.SetKey(awsString(key));

  return impl_->s3Client()->HeadObject(request).IsSuccess();
}

} // namespace facebook::velox::filesystems
