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
NimbleEncoding::NimbleEncoding(std::string_view encodingData, bool encodeNulls)
    : encodingData_(encodingData), encodeNulls_(encodeNulls) {
  encodingType_ = encoding::peek<uint8_t, EncodingType>(encodingData_.data());
  dataType_ = encoding::peek<uint8_t, DataType>(
      encodingData_.data() + Encoding::kDataTypeOffset);
  numValues_ = encoding::peek<uint32_t>(
      encodingData_.data() + Encoding::kRowCountOffset);
}

NimbleEncoding::NimbleEncoding(
    facebook::wave::nimble::EncodingType encodingType,
    facebook::wave::nimble::DataType dataType,
    uint32_t numValues)
    : encodingData_{},
      encodingType_(encodingType),
      dataType_(dataType),
      numValues_(numValues),
      encodeNulls_(false) {}

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
  bool encodeNulls = false;

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
        encodeNulls = true;
      }
      break;
    }

    default:
      VELOX_NYI("Encoding type does not support child streams");
  }

  if (childEncodingData.empty()) {
    VELOX_NYI("Invalid child index for this encoding type");
  }

  auto child = create(childEncodingData, encodeNulls);
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
    return std::make_unique<className>(encodingData, encodeNulls);

std::unique_ptr<NimbleEncoding> NimbleEncoding::create(
    std::string_view encodingData,
    bool encodeNulls) {
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

namespace {
void setFilter(GpuDecode* step, ColumnReader* reader) {
  auto* veloxFilter = reader->scanSpec().filter();
  if (!veloxFilter) {
    step->filterKind = WaveFilterKind::kAlwaysTrue;
    return;
  }
  switch (veloxFilter->kind()) {
    case common::FilterKind::kBigintRange: {
      step->filterKind = WaveFilterKind::kBigintRange;
      step->nullsAllowed = veloxFilter->testNull();
      step->filter._.int64Range[0] =
          reinterpret_cast<const common::BigintRange*>(veloxFilter)->lower();
      step->filter._.int64Range[1] =
          reinterpret_cast<const common::BigintRange*>(veloxFilter)->upper();
      break;
    }

    default:
      VELOX_UNSUPPORTED(
          "Unsupported filter kind", static_cast<int32_t>(veloxFilter->kind()));
  }
}
} // namespace

std::unique_ptr<GpuDecode> NimbleEncoding::makeStepCommon(
    ColumnOp& op,
    const ColumnOp* previousFilter,
    ReadStream& stream,
    ResultStaging& deviceStaging,
    int32_t blockIdx,
    bool isRoot,
    bool processFilter,
    int32_t resultOffset) {
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
  step->baseRow = rowsPerBlock * blockIdx;
  if (processFilter && previousFilter) {
    step->maxRow = GpuDecode::kFilterHits;
  } else {
    step->maxRow = (blockIdx * rowsPerBlock) + rowsInBlock;
  }

  // if the encoding is not a root, we ignore the previous filter when decoding
  bool dense = (previousFilter == nullptr || !processFilter) &&
      simd::isDense(op.rows.data(), op.rows.size());
  bool nullable = isRoot && isNullableEncoding();
  step->nullMode = dense
      ? (nullable ? NullMode::kDenseNullable : NullMode::kDenseNonNull)
      : (nullable ? NullMode::kSparseNullable : NullMode::kSparseNonNull);

  // intermediate decoding steps don't need to worry about filters
  if (processFilter) {
    setFilter(step.get(), op.reader);
  }

  if (step->filterKind != WaveFilterKind::kAlwaysTrue && op.waveVector) {
    /// Filtres get to record an extra copy of their passing rows if they make
    /// values.
    if (blockIdx == 0) {
      op.extraRowCountId = deviceStaging.reserve(
          numBlocks * step->numRowsPerThread * sizeof(int32_t));
      deviceStaging.registerPointer(
          op.extraRowCountId, &step->filterRowCount, true);
      deviceStaging.registerPointer(
          op.extraRowCountId, &op.extraRowCount, true);
    } else {
      step->filterRowCount = reinterpret_cast<int32_t*>(
          blockIdx * sizeof(int32_t) * maxRowsPerThread);
      deviceStaging.registerPointer(
          op.extraRowCountId, &step->filterRowCount, false);
    }
  }

  if (processFilter && previousFilter) {
    if (previousFilter->deviceResult) {
      // This is when the previous filter is in the previous kernel and its
      // device side result is allocated.
      step->rows = previousFilter->deviceResult + blockIdx * rowsPerBlock;
    } else {
      step->rows =
          reinterpret_cast<int32_t*>(blockIdx * rowsPerBlock * sizeof(int32_t));
      deviceStaging.registerPointer(
          previousFilter->deviceResultId, &step->rows, false);
    }
  }

  if (processFilter && op.reader->scanSpec().filter()) {
    if (blockIdx == 0) {
      op.deviceResultId =
          deviceStaging.reserve(op.rows.size() * sizeof(int32_t));
      deviceStaging.registerPointer(op.deviceResultId, &step->resultRows, true);
      deviceStaging.registerPointer(op.deviceResultId, &op.deviceResult, true);
    } else {
      step->resultRows =
          reinterpret_cast<int32_t*>(blockIdx * rowsPerBlock * sizeof(int32_t));
      deviceStaging.registerPointer(
          op.deviceResultId, &step->resultRows, false);
    }
  }

  auto columnKind = static_cast<WaveTypeKind>(typeKind());
  step->dataType = columnKind;
  auto kindSize = waveTypeKindSize(columnKind);

  if (isRoot) {
    step->result = op.waveVector->values<char>();
  } else {
    if (decodedResultBuffer_ == nullptr) {
      decodedResultBuffer_ = stream.waveStream->deviceArena().allocate<char>(
          kindSize * numValues());
    }
    step->result = decodedResultBuffer_->as<char>();
  }
  step->resultNulls = nullptr;

  return step;
}

void NimbleEncoding::getDeviceEncodingInput(
    SplitStaging& staging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    const void* hostPtr,
    const void** devicePtr) {
  auto* hostBasePtr = rootEncoding.encodedDataPtr();
  void* deviceBasePtr = rootEncoding.deviceEncodedDataPtr();
  std::ptrdiff_t offset = static_cast<const char*>(hostPtr) - hostBasePtr;
  if (deviceBasePtr != nullptr) {
    *devicePtr = static_cast<char*>(deviceBasePtr) + offset;
  } else { // the root encoding is not transferred to the device yet
    *devicePtr = reinterpret_cast<void*>(offset);
    staging.registerPointer(bufferId, devicePtr, false);
  }
}

// NimbleDictionaryEncoding implementation
NimbleDictionaryEncoding::NimbleDictionaryEncoding(
    std::string_view encodingData,
    bool encodeNulls)
    : NimbleEncoding(encodingData, encodeNulls) {
  if (encodeNulls) {
    VELOX_NYI("Dictionary-encoded null is not supported for decoding yet.");
  }
}

NimbleEncoding* NimbleDictionaryEncoding::alphabetEncoding() {
  return childAt(EncodingIdentifiers::Dictionary::Alphabet);
}

NimbleEncoding* NimbleDictionaryEncoding::indicesEncoding() {
  return childAt(EncodingIdentifiers::Dictionary::Indices);
}

std::unique_ptr<GpuDecode> NimbleDictionaryEncoding::makeStep(
    ColumnOp& op,
    const ColumnOp* previousFilter,
    ReadStream& stream,
    SplitStaging& staging,
    ResultStaging& deviceStaging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx,
    int32_t resultOffset) {
  VELOX_NYI("Dicionary encoding is not supported yet.");
}

// NimbleTrivialEncoding implementation
NimbleTrivialEncoding::NimbleTrivialEncoding(
    std::string_view encodingData,
    bool encodeNulls)
    : NimbleEncoding(encodingData, encodeNulls) {}

NimbleEncoding* NimbleTrivialEncoding::lengthsEncoding() {
  VELOX_NYI("Trivial encoding does not support strings yet");
}

std::unique_ptr<GpuDecode> NimbleTrivialEncoding::makeStep(
    ColumnOp& op,
    const ColumnOp* previousFilter,
    ReadStream& stream,
    SplitStaging& splitStaging,
    ResultStaging& deviceStaging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx,
    int32_t resultOffset) {
  auto hostPtr = encodedDataPtr();
  auto compressionType =
      static_cast<CompressionType>(encoding::readChar(hostPtr));
  if (compressionType != CompressionType::Uncompressed) {
    VELOX_NYI("Trivial encoding does not support compression yet");
  }

  if (encodeNulls_) {
    return makeStepNulls(
        op,
        stream,
        splitStaging,
        deviceStaging,
        bufferId,
        rootEncoding,
        blockIdx,
        resultOffset);
  }
  bool isRoot = (this == &rootEncoding);

  // True if we process the filter in this step.
  bool processFilter = isRoot && !op.hasMultiChunks &&
      (previousFilter || op.reader->scanSpec().filter());

  auto step = makeStepCommon(
      op,
      previousFilter,
      stream,
      deviceStaging,
      blockIdx,
      isRoot,
      processFilter,
      resultOffset);
  auto kindSize = waveTypeKindSize(step->dataType);

  // True if it is a single-chunk, filter or non-filter column
  bool singleChunkFilter = isRoot && !op.hasMultiChunks &&
      (previousFilter || op.reader->scanSpec().filter());

  if (singleChunkFilter) {
    if (kindSize != 4 && kindSize != 8) {
      VELOX_NYI(
          "Unsupported data type for nullable encoding. The data type must be either 4 or 8 bytes.");
    }
    step->step =
        kindSize == 4 ? DecodeStep::kSelective32 : DecodeStep::kSelective64;
    step->encoding = DecodeStep::kDictionaryOnBitpack;
    step->dictMode = DictMode::kNone;
    step->data.dictionaryOnBitpack.dataType = step->dataType;
    step->data.dictionaryOnBitpack.alphabet = nullptr;
    step->data.dictionaryOnBitpack.scatter = nullptr;
    step->data.dictionaryOnBitpack.bitWidth = 8 * kindSize;

    // Input is exclusive to a chunk and does not need to be specified the chunk
    // offset. TB offset is also not needed if we set baseRow and maxRow
    // correctly.
    getDeviceEncodingInput(
        splitStaging,
        bufferId,
        rootEncoding,
        hostPtr,
        reinterpret_cast<const void**>(
            &step->data.dictionaryOnBitpack.indices));

    // Output is shared across chunks and must be specified the chunk offset and
    // the TB offset.
    step->result = step->data.dictionaryOnBitpack.result =
        static_cast<char*>(step->result) + resultOffset * kindSize +
        step->baseRow * kindSize;

    return step;
  }

  // True if it is a multi-chunk, non-filter column
  bool multiChunkFiltered = isRoot && previousFilter && op.hasMultiChunks &&
      !op.reader->scanSpec().filter();
  if (multiChunkFiltered) {
    step->step = kindSize == 4 ? DecodeStep::kSelective32Chunked
                               : DecodeStep::kSelective64Chunked;
    step->rows = previousFilter->deviceResult;
    step->filterRowCount = previousFilter->extraRowCount;
    step->result = op.waveVector->values<char>();
    step->data.selectiveChunked.chunkStart = resultOffset;
    getDeviceEncodingInput(
        splitStaging,
        bufferId,
        rootEncoding,
        hostPtr,
        reinterpret_cast<const void**>(&step->data.selectiveChunked.input));
    return step;
  }

  step->step = DecodeStep::kTrivial;
  step->data.trivial.dataType = step->dataType;
  step->data.trivial.result = static_cast<char*>(step->result) +
      kindSize * resultOffset + kindSize * step->baseRow;
  step->data.trivial.begin = 0;
  step->data.trivial.end = step->maxRow - step->baseRow;
  step->data.trivial.scatter = nullptr;
  getDeviceEncodingInput(
      splitStaging,
      bufferId,
      rootEncoding,
      hostPtr + step->baseRow * waveTypeKindSize(step->dataType),
      &step->data.trivial.input);
  return step;
}

std::unique_ptr<GpuDecode> NimbleTrivialEncoding::makeStepNulls(
    ColumnOp& op,
    ReadStream& stream,
    SplitStaging& splitStaging,
    ResultStaging& deviceStaging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx,
    int32_t offset) {
  constexpr int32_t kCountStride = 1024;

  if (stagedNulls_) {
    return nullptr;
  }
  stagedNulls_ = true;

  // For bools, we don't need to decode the data, just transfer the data
  // we do not transfer the nulls with the root encoding because we want to
  // make sure the null encodings are aligned.
  Staging staging(
      encodedDataPtr() + 1, // skip the compression type
      bits::nwords(numValues()) * sizeof(uint64_t),
      common::Region{0, 0});
  auto nullStageId = splitStaging.add(staging);
  splitStaging.registerPointer(nullStageId, &nulls_, true);

  auto rowsPerBlock = FLAGS_wave_reader_rows_per_tb;
  auto numBlocks = bits::roundUp(numValues(), rowsPerBlock) / rowsPerBlock;

  // A single thread block is enough to decode the nulls
  if (numBlocks == 1) {
    return nullptr;
  }

  auto count = std::make_unique<GpuDecode>();
  splitStaging.registerPointer(nullStageId, &count->data.countBits.bits, true);
  auto numStrides = bits::roundUp(numValues(), kCountStride) / kCountStride;
  auto resultId = deviceStaging.reserve(sizeof(int32_t) * numStrides);
  deviceStaging.registerPointer(resultId, &count->result, true);
  deviceStaging.registerPointer(resultId, &numNonNull_, true);
  count->step = DecodeStep::kCountBits;
  count->data.countBits.numBits = numValues();
  count->data.countBits.resultStride = kCountStride;

  return count;
}

// NimbleRLEEncoding implementation
NimbleRLEEncoding::NimbleRLEEncoding(
    std::string_view encodingData,
    bool encodeNulls)
    : NimbleEncoding(encodingData, encodeNulls) {
  if (encodeNulls) {
    VELOX_NYI("RLE-encoded null is not supported for decoding yet.");
  }
}

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
    const ColumnOp* previousFilter,
    ReadStream& stream,
    SplitStaging& staging,
    ResultStaging& deviceStaging,

    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx,
    int32_t resultOffset) {
  VELOX_NYI("RLE encoding is not supported yet.");
}

// NimbleNullableEncoding implementation
NimbleNullableEncoding::NimbleNullableEncoding(
    std::string_view encodingData,
    bool encodeNulls)
    : NimbleEncoding(encodingData, encodeNulls) {
  VELOX_CHECK(!encodeNulls, "Nullable encoding cannot be used to encode nulls");
}

NimbleEncoding* NimbleNullableEncoding::nullsEncoding() {
  return childAt(EncodingIdentifiers::Nullable::Nulls);
}

NimbleEncoding* NimbleNullableEncoding::nonNullsEncoding() {
  return childAt(EncodingIdentifiers::Nullable::Data);
}

std::unique_ptr<GpuDecode> NimbleNullableEncoding::makeStep(
    ColumnOp& op,
    const ColumnOp* previousFilter,
    ReadStream& stream,
    SplitStaging& staging,
    ResultStaging& deviceStaging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx,
    int32_t resultOffset) {
  VELOX_CHECK(
      this == &rootEncoding,
      "Only root encoding can be decoded for nullable encoding");
  bool isRoot = (this == &rootEncoding);
  bool processFilter = isRoot && !op.hasMultiChunks &&
      (previousFilter || op.reader->scanSpec().filter());
  auto step = makeStepCommon(
      op,
      previousFilter,
      stream,
      deviceStaging,
      blockIdx,
      isRoot,
      processFilter,
      resultOffset);
  auto kindSize = waveTypeKindSize(step->dataType);
  if (kindSize != 4 && kindSize != 8) {
    VELOX_NYI(
        "Unsupported data type for nullable encoding. The data type must be either 4 or 8 bytes.");
  }
  auto dataBufferPtr =
      childAt(EncodingIdentifiers::Nullable::Data)->decodedResultBuffer();

  bool multiChunkFiltered = isRoot && previousFilter && op.hasMultiChunks &&
      !op.reader->scanSpec().filter();
  if (multiChunkFiltered) {
    step->step = kindSize == 4 ? DecodeStep::kSelective32Chunked
                               : DecodeStep::kSelective64Chunked;
    step->rows = previousFilter->deviceResult;
    step->filterRowCount = previousFilter->extraRowCount;
    step->result = op.waveVector->values<char>();
    step->nulls =
        childAt(EncodingIdentifiers::Nullable::Nulls)->nulls(); // chunk local
    step->nonNullBases =
        childAt(EncodingIdentifiers::Nullable::Nulls)->numNonNull();

    step->resultNulls = op.waveVector->nulls();
    step->result = op.waveVector->values<void>();

    step->data.selectiveChunked.chunkStart = resultOffset;
    step->data.selectiveChunked.input =
        dataBufferPtr ? dataBufferPtr->as<void>() : nullptr;
    return step;
  }

  step->step =
      kindSize == 4 ? DecodeStep::kSelective32 : DecodeStep::kSelective64;
  step->encoding = DecodeStep::kDictionaryOnBitpack;
  step->dictMode = DictMode::kNone;
  step->data.dictionaryOnBitpack.dataType = step->dataType;
  step->data.dictionaryOnBitpack.alphabet = nullptr;
  step->data.dictionaryOnBitpack.scatter = nullptr;
  step->data.dictionaryOnBitpack.bitWidth = 8 * kindSize;

  // Input is exclusive to a chunk and does not need to be specified the chunk
  // offset. TB offset is also not needed if we set baseRow and maxRow
  // correctly.
  step->data.dictionaryOnBitpack.indices =
      dataBufferPtr ? dataBufferPtr->as<uint64_t>() : nullptr;
  step->nulls =
      childAt(EncodingIdentifiers::Nullable::Nulls)->nulls(); // chunk local
  step->nonNullBases =
      childAt(EncodingIdentifiers::Nullable::Nulls)->numNonNull();

  // Output is shared across chunks and must be specified the chunk offset and
  // the TB offset.
  step->resultNulls = op.waveVector->nulls() + resultOffset +
      step->baseRow; // shared across chunks
  step->result = step->data.dictionaryOnBitpack.result =
      static_cast<char*>(step->result) + resultOffset * kindSize +
      step->baseRow * kindSize;

  return step;
}

NimbleFilterEncoding::NimbleFilterEncoding(
    facebook::wave::nimble::DataType dataType,
    uint32_t numValues,
    bool hasNulls)
    : NimbleEncoding(
          hasNulls ? EncodingType::Nullable : EncodingType::Trivial,
          dataType,
          numValues) {}

std::unique_ptr<GpuDecode> NimbleFilterEncoding::makeStep(
    ColumnOp& op,
    const ColumnOp* previousFilter,
    ReadStream& stream,
    SplitStaging& staging,
    ResultStaging& deviceStaging,
    BufferId bufferId,
    const NimbleEncoding& rootEncoding,
    int32_t blockIdx,
    int32_t resultOffset) {
  auto step = makeStepCommon(
      op,
      previousFilter,
      stream,
      deviceStaging,
      blockIdx,
      true,
      true,
      resultOffset);

  auto columnKind = static_cast<WaveTypeKind>(typeKind());
  step->dataType = columnKind;
  auto kindSize = waveTypeKindSize(columnKind);
  if (kindSize != 4 && kindSize != 8) {
    VELOX_NYI(
        "Unsupported data type for filters. The data type must be either 4 or 8 bytes.");
  }
  step->step =
      kindSize == 4 ? DecodeStep::kSelective32 : DecodeStep::kSelective64;
  step->encoding = DecodeStep::kDictionaryOnBitpack;
  step->dictMode = DictMode::kNone;
  step->data.dictionaryOnBitpack.dataType = step->dataType;
  step->data.dictionaryOnBitpack.alphabet = nullptr;
  step->data.dictionaryOnBitpack.scatter = nullptr;
  step->data.dictionaryOnBitpack.bitWidth = 8 * kindSize;
  step->data.dictionaryOnBitpack.indices = op.waveVector->values<uint64_t>();
  step->nulls = reinterpret_cast<char*>(op.waveVector->nulls());
  step->isNullsBitmap = false;
  step->result = step->data.dictionaryOnBitpack.result =
      op.waveVector->values<char>() + step->baseRow * kindSize;
  step->resultNulls = op.waveVector->nulls() + step->baseRow;

  return step;
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

  return NimbleEncoding::create(encodingData, false);
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
