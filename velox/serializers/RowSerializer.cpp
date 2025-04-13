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

#include "velox/serializers/RowSerializer.h"

namespace facebook::velox::serializer {

namespace detail {

// static
RowGroupHeader RowGroupHeader::read(ByteInputStream* source) {
  RowGroupHeader header;
  header.uncompressedSize = source->read<int32_t>();
  header.compressedSize = source->read<int32_t>();
  header.compressed = source->read<char>();

  VELOX_CHECK_GE(header.uncompressedSize, 0);
  VELOX_CHECK_GE(header.compressedSize, 0);

  return header;
}

// static
size_t RowGroupHeader::size() {
  return sizeof(int32_t) * 2 + sizeof(char);
}

void RowGroupHeader::write(OutputStream* out) {
  out->write(reinterpret_cast<char*>(&uncompressedSize), sizeof(int32_t));
  out->write(reinterpret_cast<char*>(&compressedSize), sizeof(int32_t));
  const char writeValue = compressed ? 1 : 0;
  out->write(reinterpret_cast<const char*>(&writeValue), sizeof(char));
}

void RowGroupHeader::write(char* out) {
  ::memcpy(out, reinterpret_cast<char*>(&uncompressedSize), sizeof(int32_t));
  ::memcpy(
      out + sizeof(int32_t),
      reinterpret_cast<char*>(&compressedSize),
      sizeof(int32_t));
  const char writeValue = compressed ? 1 : 0;
  ::memcpy(
      out + sizeof(int32_t) * 2,
      reinterpret_cast<const char*>(&writeValue),
      sizeof(char));
}

std::string RowGroupHeader::debugString() const {
  return fmt::format(
      "uncompressedSize: {}, compressedSize: {}, compressed: {}",
      succinctBytes(uncompressedSize),
      succinctBytes(compressedSize),
      compressed);
}

} // namespace detail

RowIteratorImpl::RowIteratorImpl(ByteInputStream* source, size_t endOffset)
    : RowIterator(source, endOffset) {}

RowIteratorImpl::RowIteratorImpl(
    std::unique_ptr<ByteInputStream> source,
    std::unique_ptr<folly::IOBuf> buf,
    size_t endOffset)
    : RowIterator(source.get(), endOffset),
      sourceHolder_(std::move(source)),
      bufHolder_(std::move(buf)) {
  VELOX_CHECK_NOT_NULL(source_, "Source cannot be null");
}

bool RowIteratorImpl::hasNext() const {
  return source_->tellp() < endOffset_;
}

std::unique_ptr<std::string> RowIteratorImpl::next() {
  const auto rowSize = readRowSize();
  auto serializedBuffer = std::make_unique<std::string>();
  serializedBuffer->reserve(rowSize);

  const auto row = source_->nextView(rowSize);
  serializedBuffer->append(row.data(), row.size());
  // If we couldn't read the entire row at once, we need to concatenate it
  // in a different buffer.
  if (serializedBuffer->size() < rowSize) {
    concatenatePartialRow(source_, rowSize, *serializedBuffer);
  }

  VELOX_CHECK_EQ(serializedBuffer->size(), rowSize);
  return serializedBuffer;
}

TRowSize RowIteratorImpl::readRowSize() {
  return folly::Endian::big(source_->read<TRowSize>());
}

// static
void RowIteratorImpl::concatenatePartialRow(
    ByteInputStream* source,
    TRowSize rowSize,
    std::string& rowBuffer) {
  while (rowBuffer.size() < rowSize) {
    const std::string_view rowFragment =
        source->nextView(rowSize - rowBuffer.size());

    VELOX_CHECK_GT(
        rowFragment.size(),
        0,
        "Unable to read full serialized row. Needed {} but read {} bytes.",
        rowSize - rowBuffer.size(),
        rowFragment.size());
    rowBuffer.append(rowFragment.data(), rowFragment.size());
  }
}

} // namespace facebook::velox::serializer
