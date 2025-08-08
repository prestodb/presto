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

#include <string_view>
#include <vector>
#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/dwio/FormatData.h"
#include "velox/experimental/wave/dwio/decode/DecodeStep.h"
#include "velox/experimental/wave/dwio/nimble/Encoding.h"
#include "velox/experimental/wave/dwio/nimble/Types.h"
#include "velox/type/Type.h"

namespace facebook::velox::wave {

// Base class for all Nimble encodings
class NimbleEncoding {
 public:
  explicit NimbleEncoding(std::string_view encodingData, bool encodeNulls);
  explicit NimbleEncoding(
      facebook::wave::nimble::EncodingType encodingType,
      facebook::wave::nimble::DataType dataType,
      uint32_t numValues);

  virtual ~NimbleEncoding() = default;

  std::string_view encodingData() const {
    return encodingData_;
  }

  facebook::wave::nimble::EncodingType encodingType() const {
    return encodingType_;
  }

  // Get a string_view of just the encoded data region (excluding prefix)
  std::string_view encodedData() const {
    return encodingData_.substr(facebook::wave::nimble::Encoding::kPrefixSize);
  }

  // Get pointer to the encoded data region (after the 6-byte prefix)
  const char* encodedDataPtr() const {
    return encodedData().data();
  }

  facebook::wave::nimble::DataType dataType() const {
    return dataType_;
  }

  uint32_t numValues() const {
    return numValues_;
  }

  facebook::velox::TypePtr veloxType() const;
  facebook::velox::TypeKind typeKind() const;

  uint8_t childrenCount() const;
  NimbleEncoding* childAt(uint8_t childIndex);

#define NIMBLE_ENCODING_TYPE_CHECK(name, type)                          \
  bool is##name##Encoding() const {                                     \
    return encodingType_ == facebook::wave::nimble::EncodingType::type; \
  }

  NIMBLE_ENCODING_TYPE_CHECK(Trivial, Trivial)
  NIMBLE_ENCODING_TYPE_CHECK(RLE, RLE)
  NIMBLE_ENCODING_TYPE_CHECK(Dictionary, Dictionary)
  NIMBLE_ENCODING_TYPE_CHECK(Nullable, Nullable)

#undef NIMBLE_ENCODING_TYPE_CHECK

  // return nullptr if not applicable for this encoding type
  // Dictionary
  virtual NimbleEncoding* alphabetEncoding();
  virtual NimbleEncoding* indicesEncoding();
  // Trivial
  virtual NimbleEncoding* lengthsEncoding();
  // RLE
  virtual NimbleEncoding* runLengthsEncoding();
  virtual NimbleEncoding* runValuesEncoding();
  // Nullable
  virtual NimbleEncoding* nullsEncoding();
  virtual NimbleEncoding* nonNullsEncoding();

  static std::unique_ptr<NimbleEncoding> create(
      std::string_view encodingData,
      bool encodeNulls);

  // Check if the encoding is ready to be decoded
  bool isDecoded() const {
    return decoded_;
  }
  void setDecoded() {
    decoded_ = true;
  }
  bool isReadyToDecode() const;

  // Populates the encoding-specific members and does necessary preparation
  // required by decoding.
  virtual std::unique_ptr<GpuDecode> makeStep(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ReadStream& stream,
      SplitStaging& staging,
      ResultStaging& deviceStaging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      int32_t blockIdx,
      int32_t resultOffset) = 0;

  // Specially used for dealing with the null bitmasks. If an encoding
  // (currently only trivial, but may be more in the future) encodes null
  // bitmask rather than regular values, it needs to call this function.
  virtual std::unique_ptr<GpuDecode> makeStepNulls(
      ColumnOp& op,
      ReadStream& stream,
      SplitStaging& staging,
      ResultStaging& deviceStaging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      int32_t blockIdx,
      int32_t offset = 0) {
    return nullptr;
  }

  void* deviceEncodedDataPtr() const {
    return deviceEncodedData_;
  }

  void** deviceEncodedDataPtrPtr() {
    return &deviceEncodedData_;
  }

  facebook::velox::wave::WaveBufferPtr decodedResultBuffer() const {
    return decodedResultBuffer_;
  }

  char* nulls() const {
    return nulls_;
  }

  int32_t* numNonNull() const {
    return numNonNull_;
  }

 protected:
  facebook::velox::TypePtr nimbleDataTypeToVeloxType(
      facebook::wave::nimble::DataType nimbleDataType) const;

  // Populates the shared members of GpuDecode of different encodings.
  std::unique_ptr<GpuDecode> makeStepCommon(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ReadStream& stream,
      ResultStaging& deviceStaging,
      int32_t blockIdx,
      bool isRoot,
      bool processFilter,
      int32_t resultOffset);
  void getDeviceEncodingInput(
      SplitStaging& staging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      const void* hostPtr,
      const void** devicePtr);

 protected:
  std::string_view encodingData_;
  facebook::wave::nimble::EncodingType encodingType_;
  facebook::wave::nimble::DataType dataType_;
  uint32_t numValues_;
  std::vector<std::unique_ptr<NimbleEncoding>> children_;
  void* deviceEncodedData_{nullptr};
  facebook::velox::wave::WaveBufferPtr decodedResultBuffer_;
  bool decoded_{false};
  char* nulls_{nullptr};
  bool stagedNulls_{false};
  int32_t* numNonNull_{nullptr};

  // true if this encoding encodes the null bitmask from Nullable
  bool encodeNulls_{false};

 private:
  NimbleEncoding* createChildEncodingByIndex(uint8_t childIndex);
};

// Dictionary encoding subclass
class NimbleDictionaryEncoding : public NimbleEncoding {
 public:
  explicit NimbleDictionaryEncoding(
      std::string_view encodingData,
      bool encodeNulls);

  NimbleEncoding* alphabetEncoding() override;
  NimbleEncoding* indicesEncoding() override;
  std::unique_ptr<GpuDecode> makeStep(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ReadStream& stream,
      SplitStaging& staging,
      ResultStaging& deviceStaging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      int32_t blockIdx,
      int32_t resultOffset) override;
};

// Trivial encoding subclass
class NimbleTrivialEncoding : public NimbleEncoding {
 public:
  explicit NimbleTrivialEncoding(
      std::string_view encodingData,
      bool encodeNulls);

  NimbleEncoding* lengthsEncoding() override;
  std::unique_ptr<GpuDecode> makeStep(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ReadStream& stream,
      SplitStaging& staging,
      ResultStaging& deviceStaging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      int32_t blockIdx,
      int32_t offset = 0) override;
  std::unique_ptr<GpuDecode> makeStepNulls(
      ColumnOp& op,
      ReadStream& stream,
      SplitStaging& staging,
      ResultStaging& deviceStaging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      int32_t blockIdx,
      int32_t resultOffset) override;
};

// RLE encoding subclass
class NimbleRLEEncoding : public NimbleEncoding {
 public:
  explicit NimbleRLEEncoding(std::string_view encodingData, bool encodeNulls);

  NimbleEncoding* runLengthsEncoding() override;
  NimbleEncoding* runValuesEncoding() override;
  std::unique_ptr<GpuDecode> makeStep(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ReadStream& stream,
      SplitStaging& staging,
      ResultStaging& deviceStaging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      int32_t blockIdx,
      int32_t resultOffset) override;
};

// Nullable encoding subclass
class NimbleNullableEncoding : public NimbleEncoding {
 public:
  explicit NimbleNullableEncoding(
      std::string_view encodingData,
      bool encodeNulls);

  NimbleEncoding* nullsEncoding() override;
  NimbleEncoding* nonNullsEncoding() override;
  std::unique_ptr<GpuDecode> makeStep(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ReadStream& stream,
      SplitStaging& staging,
      ResultStaging& deviceStaging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      int32_t blockIdx,
      int32_t resultOffset) override;
};

class NimbleFilterEncoding : public NimbleEncoding {
 public:
  explicit NimbleFilterEncoding(
      facebook::wave::nimble::DataType dataType,
      uint32_t numValues,
      bool hasNulls);

  std::unique_ptr<GpuDecode> makeStep(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ReadStream& stream,
      SplitStaging& staging,
      ResultStaging& deviceStaging,
      BufferId bufferId,
      const NimbleEncoding& rootEncoding,
      int32_t blockIdx,
      int32_t resultOffset) override;
};

class NimbleChunk {
 public:
  // Chunk layout: length + compression type + encoding data
  explicit NimbleChunk(std::string_view chunkData);

  // Get the raw chunk data including header
  std::string_view chunkData() const {
    return chunkData_;
  }

  static std::unique_ptr<NimbleEncoding> parseEncodingFromChunk(
      std::string_view chunkData);

 private:
  std::string_view chunkData_;
};

class NimbleChunkedStream {
 public:
  NimbleChunkedStream(
      velox::memory::MemoryPool& memoryPool,
      std::string_view streamData);

  bool hasNext();

  NimbleChunk nextChunk();

 private:
  std::string_view streamData_;
  const char* pos_;
};

class NimbleStripe {
 public:
  NimbleStripe(
      std::vector<std::unique_ptr<NimbleChunkedStream>>&& in,
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      int32_t totalRows)
      : typeWithId_(type), streams_(std::move(in)), totalRows_(totalRows) {}

  NimbleChunkedStream* findStream(const dwio::common::TypeWithId& child) const {
    for (auto i = 0; i < typeWithId_->size(); ++i) {
      if (typeWithId_->childAt(i)->id() == child.id()) {
        return streams_[i].get();
      }
    }
    VELOX_FAIL("No such column {}", child.id());
  }

  int32_t numStreams() const {
    return streams_.size();
  }

  int32_t totalRows() const {
    return totalRows_;
  }

 private:
  std::shared_ptr<const dwio::common::TypeWithId> typeWithId_;
  std::vector<std::unique_ptr<NimbleChunkedStream>> streams_;
  int32_t totalRows_;
};

} // namespace facebook::velox::wave
