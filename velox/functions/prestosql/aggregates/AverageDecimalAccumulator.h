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

#include "velox/common/base/IOUtils.h"
#include "velox/type/DecimalUtil.h"

namespace facebook::velox::aggregate {

/**
 *  AverageDecimalAccumulator appends an additional member, overflow count
 *  to SumCount base class:
 *    SUM: Total sum.
 *    COUNT: Total number of rows so far.
 *    OVERFLOW: Number of times the sum overflow/underflow occurred.
 */
struct AverageDecimalAccumulator {
 public:
  void mergeWith(const StringView& serializedData) {
    VELOX_CHECK_EQ(serializedData.size(), serializedSize());
    auto serialized = serializedData.data();
    common::InputByteStream stream(serialized);
    count += stream.read<int64_t>();
    overflow += stream.read<int64_t>();
    uint64_t lowerSum = stream.read<uint64_t>();
    int64_t upperSum = stream.read<int64_t>();
    overflow += DecimalUtil::addWithOverflow(
        this->sum, buildInt128(upperSum, lowerSum), this->sum);
  }

  void serialize(StringView& serialized) {
    VELOX_CHECK_EQ(serialized.size(), serializedSize());
    char* outputBuffer = const_cast<char*>(serialized.data());
    common::OutputByteStream outStream(outputBuffer);
    outStream.append((char*)&count, sizeof(int64_t));
    outStream.append((char*)&overflow, sizeof(int64_t));
    uint64_t lower = LOWER(sum);
    int64_t upper = UPPER(sum);
    outStream.append((char*)&lower, sizeof(int64_t));
    outStream.append((char*)&upper, sizeof(int64_t));
  }

  /*
   * Total size = sizeOf(count) + sizeOf(overflow) + sizeOf(sum)
   *            = 8 + 8 + 16 = 32.
   */
  inline static size_t serializedSize() {
    return sizeof(int64_t) * 4;
  }

  int128_t sum{0};
  int64_t count{0};
  int64_t overflow{0};
};
} // namespace facebook::velox::aggregate
