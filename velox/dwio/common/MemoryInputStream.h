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
#include <string>

#include "velox/dwio/common/InputStream.h"

namespace facebook::velox::dwio::common {

class MemoryInputStream : public ReferenceableInputStream {
 public:
  MemoryInputStream(const std::string& /* UNUSED */, const MetricsLogPtr& _log)
      : ReferenceableInputStream("MemoryInputStream", _log) {}

  MemoryInputStream(
      const char* FOLLY_NULLABLE _buffer,
      size_t _size,
      const MetricsLogPtr& _log = MetricsLog::voidLog())
      : ReferenceableInputStream("MemoryInputStream", _log),
        buffer(_buffer),
        size(_size),
        naturalReadSize(1024) {}

  ~MemoryInputStream() override = default;

  virtual uint64_t getLength() const override;

  virtual uint64_t getNaturalReadSize() const override;

  virtual void read(
      void* FOLLY_NONNULL buf,
      uint64_t length,
      uint64_t offset,
      MetricsLog::MetricsType /* UNUSED */) override;

  const char* FOLLY_NULLABLE getData() const;

  const void* FOLLY_NULLABLE readReference(
      void* FOLLY_NULLABLE /*buf*/,
      uint64_t /*length*/,
      uint64_t offset,
      MetricsLog::MetricsType /* UNUSED */) override;

  const void* FOLLY_NULLABLE readReferenceOnly(
      uint64_t /*length*/,
      uint64_t offset,
      MetricsLog::MetricsType /* UNUSED */) override;

 private:
  const char* FOLLY_NULLABLE buffer;
  uint64_t size;
  uint64_t naturalReadSize;
};

} // namespace facebook::velox::dwio::common
