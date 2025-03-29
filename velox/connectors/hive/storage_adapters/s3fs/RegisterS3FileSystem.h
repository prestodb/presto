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

#pragma once

#include <functional>
#include <memory>
#include <string>

namespace Aws::Auth {
// Forward-declare the AWSCredentialsProvider class from the AWS SDK.
class AWSCredentialsProvider;
} // namespace Aws::Auth

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::filesystems {

using CacheKeyFn = std::function<
    std::string(std::shared_ptr<const config::ConfigBase>, std::string_view)>;

// Register the S3 filesystem.
void registerS3FileSystem(CacheKeyFn cacheKeyFunc = nullptr);

void registerS3Metrics();

/// Teardown the AWS SDK C++.
/// Velox users need to manually invoke this before exiting an application.
/// This is because Velox uses a static object to hold the S3 FileSystem
/// instance. AWS C++ SDK library also uses static global objects in its code.
/// The order of static object destruction is not determined by the C++
/// standard.
/// This could lead to a segmentation fault during the program exit.
/// Ref https://github.com/aws/aws-sdk-cpp/issues/1550#issuecomment-1412601061
void finalizeS3FileSystem();

class S3Config;

using AWSCredentialsProviderFactory =
    std::function<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>(
        const S3Config& config)>;

void registerAWSCredentialsProvider(
    const std::string& providerName,
    const AWSCredentialsProviderFactory& provider);

} // namespace facebook::velox::filesystems
