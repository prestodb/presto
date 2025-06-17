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

#include "velox/connectors/hive/storage_adapters/gcs/GcsWriteFile.h"
#include "velox/connectors/hive/storage_adapters/gcs/GcsUtil.h"

namespace facebook::velox::filesystems {

namespace gcs = ::google::cloud::storage;

class GcsWriteFile::Impl {
 public:
  Impl(const std::string& path, std::shared_ptr<gcs::Client> client)
      : client_(client) {
    setBucketAndKeyFromGcsPath(path, bucket_, key_);
  }

  ~Impl() {
    close();
  }

  void initialize() {
    // Make it a no-op if invoked twice.
    if (size_ != -1) {
      return;
    }

    // Check that it doesn't exist, if it does throw an error
    auto object_metadata = client_->GetObjectMetadata(bucket_, key_);
    VELOX_CHECK(!object_metadata.ok(), "File already exists");

    auto stream = client_->WriteObject(bucket_, key_);
    checkGcsStatus(
        stream.last_status(),
        "Failed to open GCS object for writing",
        bucket_,
        key_);
    stream_ = std::move(stream);
    size_ = 0;
  }

  void append(const std::string_view data) {
    VELOX_CHECK(isFileOpen(), "File is not open");
    stream_ << data;
    size_ += data.size();
  }

  void flush() {
    if (isFileOpen()) {
      stream_.flush();
    }
  }

  void close() {
    if (isFileOpen()) {
      stream_.flush();
      stream_.Close();
      closed_ = true;
    }
  }

  uint64_t size() const {
    return size_;
  }

 private:
  inline bool isFileOpen() {
    return (!closed_ && stream_.IsOpen());
  }

  gcs::ObjectWriteStream stream_;
  std::shared_ptr<gcs::Client> client_;
  std::string bucket_;
  std::string key_;
  std::atomic<int64_t> size_{-1};
  std::atomic<bool> closed_{false};
};

GcsWriteFile::GcsWriteFile(
    const std::string& path,
    std::shared_ptr<gcs::Client> client)
    : impl_(std::make_unique<Impl>(path, client)) {}

GcsWriteFile::~GcsWriteFile() = default;

void GcsWriteFile::initialize() {
  impl_->initialize();
}

void GcsWriteFile::append(const std::string_view data) {
  impl_->append(data);
}

void GcsWriteFile::flush() {
  impl_->flush();
}

void GcsWriteFile::close() {
  impl_->close();
}

uint64_t GcsWriteFile::size() const {
  return impl_->size();
}

} // namespace facebook::velox::filesystems
