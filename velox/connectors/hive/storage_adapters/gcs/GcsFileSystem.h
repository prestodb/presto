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

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/gcs/GcsOAuthCredentialsProvider.h"

namespace facebook::velox::filesystems {

/// Implementation of GCS filesystem and file interface.
/// We provide a registration method for read and write files so the appropriate
/// type of file can be constructed based on a filename. See the
/// (register|generate)ReadFile and (register|generate)WriteFile functions.
class GcsFileSystem : public FileSystem {
 public:
  explicit GcsFileSystem(
      const std::string& bucket,
      std::shared_ptr<const config::ConfigBase> config);

  /// Initialize the google::cloud::storage::Client from the input Config
  /// parameters.
  void initializeClient();

  /// Initialize a ReadFile
  /// First the method google::cloud::storage::Client::GetObjectMetadata
  /// is used to validate
  /// [[https://cloud.google.com/storage/docs/samples/storage-get-metadata]]
  /// then the method google::cloud::storage::Client::ReadObject
  /// is used to read sequentially
  /// [[https://cloud.google.com/storage/docs/samples/storage-stream-file-download]].
  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options = {}) override;

  /// Initialize a WriteFile
  /// First the method google::cloud::storage::Client::GetObjectMetadata
  /// is used to validate
  /// [[https://cloud.google.com/storage/docs/samples/storage-get-metadata]]
  /// then the method google::cloud::storage::Client::WriteObject
  /// is used to append sequentially
  /// [[https://cloud.google.com/storage/docs/samples/storage-stream-file-upload]].
  /// The default buffer size is currently 8 MiB
  /// but this default value can change.
  /// [[https://cloud.google.com/storage/docs/resumable-uploads]].
  /// The in-memory buffer is kept until the instance is closed or there is an
  /// excess of data. If any previously buffered data and the data to append are
  /// larger than the maximum size of the internal buffer then the largest
  /// amount of data that is a multiple of the upload quantum (256KiB) is
  /// flushed. Any data in excess of a multiple of the upload quantum are
  /// buffered for the next upload.
  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options = {}) override;

  /// Returns the name of the adapter (GCS)
  std::string name() const override;

  /// Removes the objects associated to a path by using
  /// google::cloud::storage::Client::DeleteObject.
  void remove(std::string_view path) override;

  /// Check that the path exists by using
  /// google::cloud::storage::Client::GetObjectMetadata
  bool exists(std::string_view path) override;

  /// List the objects associated to a path using
  /// google::cloud::storage::Client::ListObjects
  std::vector<std::string> list(std::string_view path) override;

  /// Renames the original object to the new object using
  /// google::cloud::storage::Client::CopyObject and
  /// google::cloud::storage::Client::DeleteObject.
  /// Note that this process involves separate copy and delete operations, which
  /// may lead to temporary inconsistencies if either operation fails or if
  /// there is a delay between them.
  void rename(
      std::string_view originPath,
      std::string_view newPath,
      bool overwrite) override;

  /// Supports mkdir operation by using
  /// google::cloud::storage::Client::InsertObject
  void mkdir(std::string_view path, const DirectoryOptions& options = {})
      override;

  /// Deletes the objects associated to a path using
  /// google::cloud::storage::Client::ListObjects and
  /// google::cloud::storage::Client::DeleteObjects
  void rmdir(std::string_view path) override;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

using GcsOAuthCredentialsProviderFactory =
    std::function<std::shared_ptr<GcsOAuthCredentialsProvider>(
        const std::shared_ptr<connector::hive::HiveConfig>& hiveConfig)>;

void registerOAuthCredentialsProvider(
    const std::string& providerName,
    const GcsOAuthCredentialsProviderFactory& factory);

} // namespace facebook::velox::filesystems
