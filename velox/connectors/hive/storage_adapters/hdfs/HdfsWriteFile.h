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

#include <hdfs/hdfs.h>
#include "velox/common/file/File.h"

namespace facebook::velox {

/// Implementation of hdfs write file. Nothing written to the file should be
/// read back until it is closed.
class HdfsWriteFile : public WriteFile {
 public:
  /// The constructor.
  /// @param hdfsClient The configured hdfs filesystem handle.
  /// @param path The file path to write.
  /// @param bufferSize Size of buffer for write - pass 0 if you want
  /// to use the default configured values.
  /// @param replication Block replication - pass 0 if you want to use
  /// the default configured values.
  /// @param blockSize Size of block - pass 0 if you want to use the
  /// default configured values.
  HdfsWriteFile(
      hdfsFS hdfsClient,
      std::string_view path,
      int bufferSize = 0,
      short replication = 0,
      int blockSize = 0);

  /// Get the file size.
  uint64_t size() const override;

  /// Flush the data.
  void flush() override;

  /// Write the data by append mode.
  void append(std::string_view data) override;

  /// Close the file.
  void close() override;

 private:
  /// The configured hdfs filesystem handle.
  hdfsFS hdfsClient_;
  /// The hdfs file handle for write.
  hdfsFile hdfsFile_;
  /// The hdfs file path.
  const std::string filePath_;
};
} // namespace facebook::velox
