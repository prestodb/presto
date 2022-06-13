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

#include <cstring>

#include "velox/dwio/common/MemoryInputStream.h"

namespace facebook::velox::dwio::common {

uint64_t MemoryInputStream::getLength() const {
  return size;
}

uint64_t MemoryInputStream::getNaturalReadSize() const {
  return naturalReadSize;
}

void MemoryInputStream::read(
    void* buf,
    uint64_t length,
    uint64_t offset,
    MetricsLog::MetricsType /* UNUSED */) {
  memcpy(buf, buffer + offset, length);
}

const char* MemoryInputStream::getData() const {
  return buffer;
}

const void* MemoryInputStream::readReference(
    void* /*buf*/,
    uint64_t /*length*/,
    uint64_t offset,
    MetricsLog::MetricsType /* UNUSED */) {
  return buffer + offset;
}

const void* MemoryInputStream::readReferenceOnly(
    uint64_t /*length*/,
    uint64_t offset,
    MetricsLog::MetricsType /* UNUSED */) {
  return buffer + offset;
}

} // namespace facebook::velox::dwio::common
