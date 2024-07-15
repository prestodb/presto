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

#include <memory>

#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Writer.h"

namespace facebook::velox::dwio::common {

/// Writer factory interface.
///
/// Implement this interface to provide a factory of writers
/// for a particular file format. Factory objects should be
/// registered using registerWriteFactory method to become
/// available for connectors. Only a single writer factory
/// per file format is allowed.
class WriterFactory {
 public:
  /// Constructor.
  /// @param format File format this factory is designated to.
  explicit WriterFactory(FileFormat format) : format_(format) {}

  virtual ~WriterFactory() = default;

  /// Get the file format ths factory is designated to.
  FileFormat fileFormat() const {
    return format_;
  }

  /// Create a writer object.
  /// @param sink output sink
  /// @param options writer options
  /// @return writer object
  virtual std::unique_ptr<Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const std::shared_ptr<dwio::common::WriterOptions>& options) = 0;

#ifdef VELOX_ENABLE_BACKWARD_COMPATIBILITY
  // TODO: for backward compatibility with old clients. Remove once Prestissimo
  // code is moved to the new API above.
  std::unique_ptr<Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const dwio::common::WriterOptions& options) {
    return createWriter(
        std::move(sink),
        std::make_shared<dwio::common::WriterOptions>(options));
  }
#endif

 private:
  const FileFormat format_;
};

/// Register a writer factory. Only a single factory can be registered
/// for each file format. An attempt to register multiple factories for
/// a single file format would cause a failure.
// @return true
bool registerWriterFactory(std::shared_ptr<WriterFactory> factory);

/// Unregister a writer factory for a specified file format.
/// @return true for unregistered factory and false for a
/// missing factory for the specified format.
bool unregisterWriterFactory(FileFormat format);

/// Get writer factory object for a specified file format. Results in
/// a failure if there is no registered factory for this format.
/// @return WriterFactory object
std::shared_ptr<WriterFactory> getWriterFactory(FileFormat format);

/// Check if a writer factory object exists for a specified file format.
/// Returns true if there is a registered factory for this format, false
/// otherwise.
/// @return true
bool hasWriterFactory(FileFormat format);

} // namespace facebook::velox::dwio::common
