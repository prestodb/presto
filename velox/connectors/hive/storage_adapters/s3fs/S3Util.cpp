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

// Implementation of S3 filesystem and file interface.
// We provide a registration method for read and write files so the appropriate
// type of file can be constructed based on a filename. See the
// (register|generate)ReadFile and (register|generate)WriteFile functions.

#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"

namespace facebook::velox {

std::string getErrorStringFromS3Error(
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error) {
  switch (error.GetErrorType()) {
    case Aws::S3::S3Errors::NO_SUCH_BUCKET:
      return "No such bucket";
    case Aws::S3::S3Errors::NO_SUCH_KEY:
      return "No such key";
    case Aws::S3::S3Errors::RESOURCE_NOT_FOUND:
      return "Resource not found";
    case Aws::S3::S3Errors::ACCESS_DENIED:
      return "Access denied";
    case Aws::S3::S3Errors::SERVICE_UNAVAILABLE:
      return "Service unavailable";
    case Aws::S3::S3Errors::NETWORK_CONNECTION:
      return "Network connection";
    default:
      return "Unknown error";
  }
}

} // namespace facebook::velox
