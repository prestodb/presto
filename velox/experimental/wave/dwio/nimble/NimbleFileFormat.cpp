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

#include "velox/experimental/wave/dwio/nimble/NimbleFileFormat.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/nimble/Encoding.h"
#include "velox/experimental/wave/dwio/nimble/EncodingIdentifier.h"
#include "velox/experimental/wave/dwio/nimble/EncodingPrimitives.h"
#include "velox/experimental/wave/dwio/nimble/Types.h"

using namespace facebook::wave::nimble;

DECLARE_int32(wave_reader_rows_per_tb);

namespace facebook::velox::wave {

/// NimbleEncoding implementation
NimbleEncoding::NimbleEncoding(std::string_view encodingData)
    : encodingData_(encodingData) {
  encodingType_ = encoding::peek<uint8_t, EncodingType>(encodingData_.data());
  dataType_ = encoding::peek<uint8_t, DataType>(
      encodingData_.data() + Encoding::kDataTypeOffset);
  numValues_ = encoding::peek<uint32_t>(
      encodingData_.data() + Encoding::kRowCountOffset);
}

uint8_t NimbleEncoding::childrenCount() const {
  const auto encodingType = this->encodingType();
  switch (encodingType) {
    case EncodingType::Trivial: {
      if (dataType() == DataType::String) {
        VELOX_NYI("Unsupported encoding type");
      }
      return 0;
    }
    case EncodingType::Dictionary:
    case EncodingType::Nullable: {
      return 2;
    }
    case EncodingType::RLE: {
      if (dataType() == DataType::Bool) {
        VELOX_NYI("Unsupported encoding type");
      }
      return 2;
    }
    default:
      VELOX_NYI(
          "Unsupported encoding type: {}", static_cast<int>(encodingType));
  }
}

facebook::velox::TypePtr NimbleEncoding::nimbleDataTypeToVeloxType(
    DataType nimbleDataType) const {
  switch (nimbleDataType) {
    case DataType::Int8:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::TINYINT>::create();
    case DataType::Uint8:
      // Velox doesn't have unsigned types, map to signed equivalent
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::TINYINT>::create();
    case DataType::Int16:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::SMALLINT>::create();
    case DataType::Uint16:
      // Velox doesn't have unsigned types, map to signed equivalent
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::SMALLINT>::create();
    case DataType::Int32:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::INTEGER>::create();
    case DataType::Uint32:
      // Velox doesn't have unsigned types, map to signed equivalent
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::INTEGER>::create();
    case DataType::Int64:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::BIGINT>::create();
    case DataType::Uint64:
      // Velox doesn't have unsigned types, map to signed equivalent
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::BIGINT>::create();
    case DataType::Float:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::REAL>::create();
    case DataType::Double:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::DOUBLE>::create();
    case DataType::Bool:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::BOOLEAN>::create();
    case DataType::String:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::VARCHAR>::create();
    case DataType::Undefined:
    default:
      return facebook::velox::ScalarType<
          facebook::velox::TypeKind::UNKNOWN>::create();
  }
}

facebook::velox::TypePtr NimbleEncoding::veloxType() const {
  return nimbleDataTypeToVeloxType(dataType());
}

facebook::velox::TypeKind NimbleEncoding::typeKind() const {
  return veloxType()->kind();
}

NimbleEncoding* NimbleEncoding::childAt(uint8_t childIndex) {
  VELOX_CHECK_LT(childIndex, childrenCount(), "Child index out of range");
  return (childIndex >= children_.size())
      ? createChildEncodingByIndex(childIndex)
      : children_[childIndex].get();
}

NimbleEncoding* NimbleEncoding::createChildEncodingByIndex(uint8_t childIndex) {
  VELOX_CHECK_LT(childIndex, childrenCount(), "Child index out of range");

  const auto encodingType = this->encodingType();
  const auto dataType = this->dataType();
  const char* pos = encodedDataPtr();

  std::string_view childEncodingData;

  switch (encodingType) {
    case EncodingType::Dictionary: {
      if (childIndex == 0) {
        const uint32_t alphabetBytes = encoding::readUint32(pos);
        childEncodingData = std::string_view(pos, alphabetBytes);
      } else if (childIndex == 1) {
        const uint32_t alphabetBytes = encoding::readUint32(pos);
        pos += alphabetBytes;
        const size_t indicesBytes =
            encodingData_.size() - (pos - encodingData_.data());
        childEncodingData = std::string_view(pos, indicesBytes);
      }
      break;
    }

    case EncodingType::RLE: {
      if (childIndex == 0) {
        const uint32_t runLengthBytes = encoding::readUint32(pos);
        childEncodingData = std::string_view(pos, runLengthBytes);
      } else if (childIndex == 1 && dataType != DataType::Bool) {
        const uint32_t runLengthBytes = encoding::readUint32(pos);
        pos += runLengthBytes;
        const size_t runValuesBytes =
            encodingData_.size() - (pos - encodingData_.data());
        childEncodingData = std::string_view(pos, runValuesBytes);
      }
      break;
    }

    case EncodingType::Nullable: {
      if (childIndex == 0) {
        const uint32_t dataBytes = encoding::readUint32(pos);
        childEncodingData = std::string_view(pos, dataBytes);
      } else if (childIndex == 1) {
        const uint32_t dataBytes = encoding::readUint32(pos);
        pos += dataBytes;
        const size_t nullsBytes =
            encodingData_.size() - (pos - encodingData_.data());
        childEncodingData = std::string_view(pos, nullsBytes);
      }
      break;
    }

    default:
      VELOX_NYI("Encoding type does not support child streams");
  }

  if (childEncodingData.empty()) {
    VELOX_NYI("Invalid child index for this encoding type");
  }

  auto child = create(childEncodingData);
  NimbleEncoding* childPtr = child.get();
  children_.push_back(std::move(child));
  return childPtr;
}

#define DEFINE_ENCODING_ACCESSOR(methodName)     \
  NimbleEncoding* NimbleEncoding::methodName() { \
    return nullptr;                              \
  }

DEFINE_ENCODING_ACCESSOR(alphabetEncoding)
DEFINE_ENCODING_ACCESSOR(indicesEncoding)
DEFINE_ENCODING_ACCESSOR(lengthsEncoding)
DEFINE_ENCODING_ACCESSOR(runLengthsEncoding)
DEFINE_ENCODING_ACCESSOR(runValuesEncoding)
DEFINE_ENCODING_ACCESSOR(nullsEncoding)
DEFINE_ENCODING_ACCESSOR(nonNullsEncoding)

bool NimbleEncoding::isReadyToDecode() const {
  for (auto& child : children_) {
    if (!child->isDecoded()) {
      return false;
    }
  }
  return true;
}

#define NIMBLE_ENCODING_FACTORY_CASE(type, className) \
  case EncodingType::type:                            \
    return std::make_unique<className>(encodingData);

std::unique_ptr<NimbleEncoding> NimbleEncoding::create(
    std::string_view encodingData) {
  EncodingType encodingType =
      encoding::peek<uint8_t, EncodingType>(encodingData.data());

  switch (encodingType) {
    NIMBLE_ENCODING_FACTORY_CASE(Trivial, NimbleTrivialEncoding)
    NIMBLE_ENCODING_FACTORY_CASE(RLE, NimbleRLEEncoding)
    NIMBLE_ENCODING_FACTORY_CASE(Dictionary, NimbleDictionaryEncoding)
    NIMBLE_ENCODING_FACTORY_CASE(Nullable, NimbleNullableEncoding)
    default:
      VELOX_NYI("Unsupported encoding type");
  }
}

#undef NIMBLE_ENCODING_FACTORY_CASE

std::unique_ptr<GpuDecode> NimbleEncoding::makeStepCommon(
    ColumnOp& op,
    ReadStream& stream,
    int32_t blockIdx) {
  auto rowsPerBlock = FLAGS_wave_reader_rows_per_tb;
  auto maxRowsPerThread = (rowsPerBlock / kBlockSize);
  [[maybe_unused]] int32_t numBlocks =
      bits::roundUp(numValues(), rowsPerBlock) / rowsPerBlock;
  auto rowsInBlock =
      std::min<int32_t>(rowsPerBlock, numValues() - (blockIdx * rowsPerBlock));

  auto step = std::make_unique<GpuDecode>();
  step->numRowsPerThread = bits::roundUp(rowsInBlock, kBlockSize) / kBlockSize;
  step->gridNumRowsPerThread = maxRowsPerThread;
  step->nthBlock = blockIdx;
  step->maxRow = (blockIdx * rowsPerBlock) + rowsInBlock;
  step->baseRow = rowsPerBlock * blockIdx;

  step->nullMode = NullMode::kDenseNonNull; // TODO(bowenwu): support null

  auto columnKind = static_cast<WaveTypeKind>(typeKind());
  step->dataType = columnKind;
  auto kindSize = waveTypeKindSize(columnKind);

  step->result =
      op.waveVector->values<char>() + kindSize * blockIdx * rowsPerBlock;
  step->resultNulls = nullptr;

  return step;
}

void NimbleEncoding::getDeviceEncodingInput(
    SplitStaging& staging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    const void* hostPtr,
    const void*& devicePtr) {
  auto* hostBasePtr = rootEncoding.encodedDataPtr();
  void* deviceBasePtr = rootEncoding.deviceEncodedDataPtr();
  std::ptrdiff_t offset = static_cast<const char*>(hostPtr) - hostBasePtr;
  if (deviceBasePtr != nullptr) {
    devicePtr = static_cast<char*>(deviceBasePtr) + offset;
  } else { // the root encoding is not transferred to the device yet
    devicePtr = reinterpret_cast<void*>(offset);
    staging.registerPointer(bufferId, &devicePtr, false);
  }
}

// NimbleDictionaryEncoding implementation
NimbleDictionaryEncoding::NimbleDictionaryEncoding(
    std::string_view encodingData)
    : NimbleEncoding(encodingData) {}

NimbleEncoding* NimbleDictionaryEncoding::alphabetEncoding() {
  return childAt(EncodingIdentifiers::Dictionary::Alphabet);
}

NimbleEncoding* NimbleDictionaryEncoding::indicesEncoding() {
  return childAt(EncodingIdentifiers::Dictionary::Indices);
}

std::unique_ptr<GpuDecode> NimbleDictionaryEncoding::makeStep(
    ColumnOp& op,
    ReadStream& stream,
    SplitStaging& staging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx) {
  // TODO(bowenwu): populate the the encoding specific data correctly
  return makeStepCommon(op, stream, blockIdx);
}

// NimbleTrivialEncoding implementation
NimbleTrivialEncoding::NimbleTrivialEncoding(std::string_view encodingData)
    : NimbleEncoding(encodingData) {}

NimbleEncoding* NimbleTrivialEncoding::lengthsEncoding() {
  VELOX_NYI("Trivial encoding does not support strings yet");
}

std::unique_ptr<GpuDecode> NimbleTrivialEncoding::makeStep(
    ColumnOp& op,
    ReadStream& stream,
    SplitStaging& staging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx) {
  auto step = makeStepCommon(op, stream, blockIdx);
  step->step = DecodeStep::kTrivial;
  step->data.trivial.dataType = step->dataType;
  step->data.trivial.result = step->result;
  step->data.trivial.begin = 0;
  step->data.trivial.end = numValues();
  step->data.trivial.scatter = nullptr;
  auto hostPtr = encodedDataPtr();
  encoding::readChar(hostPtr); // skip the lengths compression
  getDeviceEncodingInput(
      staging,
      bufferId,
      rootEncoding,
      hostPtr + step->baseRow * waveTypeKindSize(step->dataType),
      step->data.trivial.input);
  return step;
}

// NimbleRLEEncoding implementation
NimbleRLEEncoding::NimbleRLEEncoding(std::string_view encodingData)
    : NimbleEncoding(encodingData) {}

NimbleEncoding* NimbleRLEEncoding::runLengthsEncoding() {
  return childAt(EncodingIdentifiers::RunLength::RunLengths);
}

NimbleEncoding* NimbleRLEEncoding::runValuesEncoding() {
  if (dataType() != DataType::Bool) {
    return childAt(EncodingIdentifiers::RunLength::RunValues);
  }
  VELOX_NYI("RLE encoding does not support bools yet");
}

std::unique_ptr<GpuDecode> NimbleRLEEncoding::makeStep(
    ColumnOp& op,
    ReadStream& stream,
    SplitStaging& staging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx) {
  // TODO(bowenwu): populate the the encoding specific data correctly
  return makeStepCommon(op, stream, blockIdx);
}

// NimbleNullableEncoding implementation
NimbleNullableEncoding::NimbleNullableEncoding(std::string_view encodingData)
    : NimbleEncoding(encodingData) {}

NimbleEncoding* NimbleNullableEncoding::nullsEncoding() {
  return childAt(EncodingIdentifiers::Nullable::Nulls);
}

NimbleEncoding* NimbleNullableEncoding::nonNullsEncoding() {
  return childAt(EncodingIdentifiers::Nullable::Data);
}

std::unique_ptr<GpuDecode> NimbleNullableEncoding::makeStep(
    ColumnOp& op,
    ReadStream& stream,
    SplitStaging& staging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx) {
  // TODO(bowenwu): populate the the encoding specific data correctly
  return makeStepCommon(op, stream, blockIdx);
}

/// NimbleChunk implementation
NimbleChunk::NimbleChunk(std::string_view chunkData) : chunkData_(chunkData) {}

std::unique_ptr<NimbleEncoding> NimbleChunk::parseEncodingFromChunk(
    std::string_view chunkData) {
  VELOX_CHECK(
      chunkData.size() >= sizeof(uint32_t) + sizeof(char),
      "Chunk data too small to contain header");

  const char* pos = chunkData.data();

  auto encodingDataLength = encoding::readUint32(pos);

  const size_t headerSize = sizeof(uint32_t) + sizeof(char);
  VELOX_CHECK(
      chunkData.size() >= headerSize + encodingDataLength,
      "Chunk data size does not match header length");

  std::string_view encodingData =
      chunkData.substr(headerSize, encodingDataLength);

  return NimbleEncoding::create(encodingData);
}

/// NimbleChunkedStream implementation
NimbleChunkedStream::NimbleChunkedStream(
    velox::memory::MemoryPool& memoryPool,
    std::string_view streamData)
    : streamData_(streamData), pos_(streamData.data()) {}

bool NimbleChunkedStream::hasNext() {
  return pos_ - streamData_.data() < streamData_.size();
}

NimbleChunk NimbleChunkedStream::nextChunk() {
  VELOX_CHECK(
      sizeof(uint32_t) + sizeof(char) <=
          streamData_.size() - (pos_ - streamData_.data()),
      "Read beyond end of stream");

  // Read the chunk header to get length and compression type
  const char* headerPos = pos_;
  auto length = encoding::readUint32(pos_);
  [[maybe_unused]] auto compressionType =
      static_cast<CompressionType>(encoding::readChar(pos_));

  VELOX_CHECK(
      length <= streamData_.size() - (pos_ - streamData_.data()),
      "Read beyond end of stream");

  const size_t headerSize = sizeof(uint32_t) + sizeof(char);
  const size_t totalChunkSize = headerSize + length;

  std::string_view chunkData;
  chunkData = std::string_view(headerPos, totalChunkSize);

  pos_ += length;

  return NimbleChunk(chunkData);
}
} // namespace facebook::velox::wave
