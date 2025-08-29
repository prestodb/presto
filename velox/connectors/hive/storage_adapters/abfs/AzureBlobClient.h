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

#include <azure/storage/blobs/blob_client.hpp>

namespace facebook::velox::filesystems {

// Interface for Azure Blob Storage client operations.
class AzureBlobClient {
 public:
  virtual ~AzureBlobClient() {}

  virtual Azure::Response<Azure::Storage::Blobs::Models::BlobProperties>
  getProperties() = 0;

  virtual Azure::Response<Azure::Storage::Blobs::Models::DownloadBlobResult>
  download(const Azure::Storage::Blobs::DownloadBlobOptions& options) = 0;

  virtual std::string getUrl() = 0;
};

} // namespace facebook::velox::filesystems
