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

#include "velox/dwio/text/reader/ReaderBase.h"

namespace facebook::velox::text {
ReaderBase::ReaderBase(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const ReaderOptions& options)
    : options_(options), input_(std::move(input)) {
  return;
}

/// TODO: Add implementation
common::CompressionKind ReaderBase::getCompression() const {
  return common::CompressionKind::CompressionKind_NONE;
}

/// TODO: Add implementation
uint64_t ReaderBase::getNumberOfRows() const {
  return 0;
}

/// TODO: Add implementation
std::unique_ptr<RowReader> ReaderBase::createRowReader() const {
  return nullptr;
}

/// TODO: Add implementation
std::unique_ptr<RowReader> ReaderBase::createRowReader(
    const RowReaderOptions& /*options*/) const {
  return nullptr;
}

/// TODO: Add implementation
std::unique_ptr<RowReader> ReaderBase::createRowReader(
    const RowReaderOptions& /*options*/,
    bool /*prestoTextReader*/) const {
  return nullptr;
}

/// TODO: Add implementation
uint64_t ReaderBase::getFileLength() const {
  return 0;
}

/// TODO: Add implementation
const std::shared_ptr<const RowType>& ReaderBase::getType() const {
  static const std::shared_ptr<const RowType> dummyType;
  return dummyType;
}

/// TODO: Add implementation
uint64_t ReaderBase::getMemoryUse() {
  return 0;
}

} // namespace facebook::velox::text
