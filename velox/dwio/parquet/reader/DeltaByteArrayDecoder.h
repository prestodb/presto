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

#include "velox/common/base/BitUtil.h"
#include "velox/dwio/parquet/reader/DeltaBpDecoder.h"

namespace facebook::velox::parquet {

// DeltaByteArrayDecoder is adapted from Apache Arrow:
// https://github.com/apache/arrow/blob/apache-arrow-15.0.0/cpp/src/parquet/encoding.cc#L2758-L2889
class DeltaLengthByteArrayDecoder {
 public:
  explicit DeltaLengthByteArrayDecoder(const char* start) {
    lengthDecoder_ = std::make_unique<DeltaBpDecoder>(start);
    decodeLengths();
    bufferStart_ = lengthDecoder_->bufferStart();
  }

  std::string_view readString() {
    const int64_t length = bufferedLength_[lengthIdx_++];
    VELOX_CHECK_GE(length, 0, "negative string delta length");
    bufferStart_ += length;
    return std::string_view(bufferStart_ - length, length);
  }

 private:
  void decodeLengths() {
    int64_t numLength = lengthDecoder_->validValuesCount();
    bufferedLength_.resize(numLength);
    lengthDecoder_->readValues<uint32_t>(bufferedLength_.data(), numLength);

    lengthIdx_ = 0;
    numValidValues_ = numLength;
  }

  const char* bufferStart_;
  std::unique_ptr<DeltaBpDecoder> lengthDecoder_;
  int32_t numValidValues_{0};
  uint32_t lengthIdx_{0};
  std::vector<uint32_t> bufferedLength_;
};

// DeltaByteArrayDecoder is adapted from Apache Arrow:
// https://github.com/apache/arrow/blob/apache-arrow-15.0.0/cpp/src/parquet/encoding.cc#L3301-L3545
class DeltaByteArrayDecoder {
 public:
  explicit DeltaByteArrayDecoder(const char* start) {
    prefixLenDecoder_ = std::make_unique<DeltaBpDecoder>(start);
    int64_t numPrefix = prefixLenDecoder_->validValuesCount();
    bufferedPrefixLength_.resize(numPrefix);
    prefixLenDecoder_->readValues<uint32_t>(
        bufferedPrefixLength_.data(), numPrefix);
    prefixLenOffset_ = 0;
    numValidValues_ = numPrefix;

    suffixDecoder_ = std::make_unique<DeltaLengthByteArrayDecoder>(
        prefixLenDecoder_->bufferStart());
  }

  void skip(uint64_t numValues) {
    skip<false>(numValues, 0, nullptr);
  }

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    for (int32_t i = 0; i < numValues; ++i) {
      readString();
    }
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    int32_t current = visitor.start();
    skip<hasNulls>(current, 0, nulls);
    int32_t toSkip;
    bool atEnd = false;
    const bool allowNulls = hasNulls && visitor.allowNulls();
    for (;;) {
      if (hasNulls && allowNulls && bits::isBitNull(nulls, current)) {
        toSkip = visitor.processNull(atEnd);
      } else {
        if (hasNulls && !allowNulls) {
          toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
          if (!Visitor::dense) {
            skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        }

        // We are at a non-null value on a row to visit.
        toSkip = visitor.process(readString(), atEnd);
      }
      ++current;
      if (toSkip) {
        skip<hasNulls>(toSkip, current, nulls);
        current += toSkip;
      }
      if (atEnd) {
        return;
      }
    }
  }

  std::string_view readString() {
    auto suffix = suffixDecoder_->readString();
    bool isFirstRun = (prefixLenOffset_ == 0);
    const int64_t prefixLength = bufferedPrefixLength_[prefixLenOffset_++];

    VELOX_CHECK_GE(
        prefixLength, 0, "negative prefix length in DELTA_BYTE_ARRAY");

    buildReadValue(isFirstRun, prefixLength, suffix);

    numValidValues_--;
    return {lastValue_};
  }

 private:
  void buildReadValue(
      bool isFirstRun,
      const int64_t prefixLength,
      std::string_view suffix) {
    VELOX_CHECK_LE(
        prefixLength,
        lastValue_.size(),
        "prefix length too large in DELTA_BYTE_ARRAY");

    if (prefixLength == 0) {
      // prefix is empty.
      lastValue_ = std::string{suffix};
      return;
    }

    if (!isFirstRun) {
      if (suffix.empty()) {
        // suffix is empty: read value can simply point to the prefix
        // of the lastValue_. This is not possible for the first run since
        // the prefix would point to the mutable `lastValue_`.
        lastValue_ = lastValue_.substr(0, prefixLength);
        return;
      }
    }

    lastValue_.resize(prefixLength + suffix.size());

    // Both prefix and suffix are non-empty, so we need to decode the string
    // into read value.
    // Just keep the prefix in lastValue_, and copy the suffix.
    memcpy(lastValue_.data() + prefixLength, suffix.data(), suffix.size());
  }

  std::unique_ptr<DeltaBpDecoder> prefixLenDecoder_;
  std::unique_ptr<DeltaBpDecoder> suffixLenDecoder_;
  std::unique_ptr<DeltaLengthByteArrayDecoder> suffixDecoder_;

  std::string lastValue_;
  int32_t numValidValues_{0};
  uint32_t prefixLenOffset_{0};
  std::vector<uint32_t> bufferedPrefixLength_;
};

} // namespace facebook::velox::parquet
