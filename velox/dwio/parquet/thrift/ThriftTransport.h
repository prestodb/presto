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

#include <thrift/transport/TVirtualTransport.h>
#include "velox/dwio/common/BufferedInput.h"

namespace facebook::velox::parquet::thrift {

class ThriftBufferedTransport
    : public apache::thrift::transport::TVirtualTransport<
          ThriftBufferedTransport> {
 public:
  ThriftBufferedTransport(const void* inputBuf, uint64_t len)
      : inputBuf_(reinterpret_cast<const uint8_t*>(inputBuf)),
        size_(len),
        offset_(0) {}

  uint32_t read(uint8_t* outputBuf, uint32_t len) {
    DWIO_ENSURE(offset_ + len <= size_);
    memcpy(outputBuf, inputBuf_ + offset_, len);
    offset_ += len;
    return len;
  }

 private:
  const uint8_t* inputBuf_;
  const uint64_t size_;
  uint64_t offset_;
};

} // namespace facebook::velox::parquet::thrift
