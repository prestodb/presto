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

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Reader.h"

namespace facebook::velox::dwio::common {

/**
 * Reader factory interface.
 *
 * Implement this interface to provide a factory of readers
 * for a particular file format. Factory objects should be
 * registered using registerReaderFactory method to become
 * available for connectors. Only a single reader factory
 * per file format is allowed.
 */
class ReaderFactory {
 public:
  /**
   * Constructor.
   * @param format File format this factory is designated to.
   */
  explicit ReaderFactory(FileFormat format) : format_(format) {}

  virtual ~ReaderFactory() = default;

  /**
   * Get the file format ths factory is designated to.
   */
  FileFormat fileFormat() const {
    return format_;
  }

  /**
   * Create a reader object.
   * @param stream input stream
   * @param options reader options
   * @return reader object
   */
  virtual std::unique_ptr<Reader> createReader(
      std::unique_ptr<BufferedInput>,
      const ReaderOptions& options) = 0;

 private:
  const FileFormat format_;
};

/**
 * Register a reader factory. Only a single factory can be registered
 * for each file format. An attempt to register multiple factories for
 * a single file format would cause a filure.
 * @return true
 */
bool registerReaderFactory(std::shared_ptr<ReaderFactory> factory);

/**
 * Unregister a reader factory for a specified file format.
 * @return true for unregistered factory and false for a
 * missing factory for the specfified format.
 */
bool unregisterReaderFactory(FileFormat format);

/**
 * Get reader factory object for a specified file format. Results in
 * a failure if there is no registered factory for this format.
 * @return ReaderFactory object
 */
std::shared_ptr<ReaderFactory> getReaderFactory(FileFormat format);

} // namespace facebook::velox::dwio::common
