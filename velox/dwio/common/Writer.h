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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "velox/vector/ComplexVector.h"

namespace facebook::velox::dwio::common {

/**
 * Abstract writer class.
 *
 * Writer object is used to write a single file.
 *
 * Writer objects are created through factories implementing
 * WriterFactory interface.
 */
class Writer {
 public:
  virtual ~Writer() = default;

  /**
   * Appends 'data' to writer. Data might still be in memory and not
   * yet written to the file.
   */
  virtual void write(const VectorPtr& data) = 0;

  /*
   * Forces the writer to flush data to the file.
   * Does not close the writer.
   */
  virtual void flush() = 0;

  /**
   *  Invokes flush and closes the writer.
   *  Data can no longer be written.
   */
  virtual void close() = 0;
};

} // namespace facebook::velox::dwio::common
