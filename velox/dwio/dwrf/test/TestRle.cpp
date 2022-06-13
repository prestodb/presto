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

#include <gtest/gtest.h>

#include "velox/common/base/Nulls.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/dwrf/common/IntDecoder.h"
#include "velox/dwio/dwrf/test/OrcTest.h"

using namespace facebook::velox;
using namespace facebook::velox::dwrf;

std::vector<int64_t> decodeRLEv2(
    const unsigned char* bytes,
    unsigned long l,
    size_t n,
    size_t count,
    const uint64_t* nulls = nullptr) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::make_unique<dwio::common::SeekableArrayInputStream>(bytes, l),
      RleVersion_2,
      *scopedPool,
      true /* doesn't matter */,
      INT_BYTE_SIZE /* doesn't matter */);
  std::vector<int64_t> results;
  size_t totalRead = 0;
  std::vector<uint64_t> remainingNulls;
  for (size_t i = 0; i < count; i += n) {
    size_t remaining = count - i;
    size_t nread = std::min(n, remaining);
    if (nulls) {
      remainingNulls.reserve(bits::nwords(nread));
      for (int32_t j = 0; j < nread; j++) {
        bits::setNull(
            remainingNulls.data(), j, bits::isBitNull(nulls, j + totalRead));
      }
    }
    std::vector<int64_t> data(nread);
    rle->next(data.data(), nread, nulls ? remainingNulls.data() : nullptr);
    totalRead += nread;
    results.insert(results.end(), data.begin(), data.end());
  }

  return results;
}

void checkResults(
    const std::vector<int64_t>& e,
    const std::vector<int64_t>& a,
    size_t n,
    const uint64_t* nulls = nullptr) {
  EXPECT_EQ(e.size(), a.size()) << "vectors differ in size";
  for (size_t i = 0; i < e.size(); ++i) {
    if (!nulls || !bits::isBitNull(nulls, i)) {
      EXPECT_EQ(e[i], a[i]) << "Output wrong at " << i << ", n=" << n;
    }
  }
}

TEST(RLEv2, basicDelta0) {
  const size_t count = 20;
  std::vector<int64_t> values;
  for (size_t i = 0; i < count; ++i) {
    values.push_back(static_cast<int64_t>(i));
  }

  const unsigned char bytes[] = {0xc0, 0x13, 0x00, 0x02};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, count), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, count), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, count), 7);
  checkResults(values, decodeRLEv2(bytes, l, count, count), count);
};

TEST(RLEv2, basicDelta1) {
  std::vector<int64_t> values(5);
  values[0] = -500;
  values[1] = -400;
  values[2] = -350;
  values[3] = -325;
  values[4] = -310;

  const unsigned char bytes[] = {
      0xce, 0x04, 0xe7, 0x07, 0xc8, 0x01, 0x32, 0x19, 0x0f};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, basicDelta2) {
  std::vector<int64_t> values(5);
  values[0] = -500;
  values[1] = -600;
  values[2] = -650;
  values[3] = -675;
  values[4] = -710;

  const unsigned char bytes[] = {
      0xce, 0x04, 0xe7, 0x07, 0xc7, 0x01, 0x32, 0x19, 0x23};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, basicDelta3) {
  std::vector<int64_t> values(5);
  values[0] = 500;
  values[1] = 400;
  values[2] = 350;
  values[3] = 325;
  values[4] = 310;

  const unsigned char bytes[] = {
      0xce, 0x04, 0xe8, 0x07, 0xc7, 0x01, 0x32, 0x19, 0x0f};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, basicDelta4) {
  std::vector<int64_t> values(5);
  values[0] = 500;
  values[1] = 600;
  values[2] = 650;
  values[3] = 675;
  values[4] = 710;

  const unsigned char bytes[] = {
      0xce, 0x04, 0xe8, 0x07, 0xc8, 0x01, 0x32, 0x19, 0x23};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, delta0Width) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {
      0x4e, 0x2, 0x0, 0x1, 0x2, 0xc0, 0x2, 0x42, 0x0};
  std::unique_ptr<IntDecoder<false>> decoder = IntDecoder<false>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(
              buffer, VELOX_ARRAY_SIZE(buffer))),
      RleVersion_2,
      *scopedPool,
      true /* doesn't matter */,
      INT_BYTE_SIZE /* doesn't matter */);
  int64_t values[6];
  decoder->next(values, 6, 0);
  EXPECT_EQ(0, values[0]);
  EXPECT_EQ(1, values[1]);
  EXPECT_EQ(2, values[2]);
  EXPECT_EQ(0x42, values[3]);
  EXPECT_EQ(0x42, values[4]);
  EXPECT_EQ(0x42, values[5]);
}

TEST(RLEv2, basicDelta0WithNulls) {
  std::vector<int64_t> values;
  uint64_t nulls[1];
  size_t idx = 0;
  for (size_t i = 0; i < 20; ++i) {
    values.push_back(static_cast<int64_t>(i));
    bits::clearNull(nulls, idx++);
    // throw in a null every third value
    bool addNull = (i % 3 == 0);
    if (addNull) {
      values.push_back(-1);
      bits::setNull(nulls, idx++);
    }
  }

  const unsigned char bytes[] = {0xc0, 0x13, 0x00, 0x02};
  unsigned long l = sizeof(bytes) / sizeof(char);
  const size_t count = values.size();
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, count, nulls), 1, nulls);
  checkResults(values, decodeRLEv2(bytes, l, 3, count, nulls), 3, nulls);
  checkResults(values, decodeRLEv2(bytes, l, 7, count, nulls), 7, nulls);
  checkResults(
      values, decodeRLEv2(bytes, l, count, count, nulls), count, nulls);
};

TEST(RLEv2, shortRepeats) {
  const size_t runLength = 7;
  const size_t nVals = 10;
  const size_t count = nVals * runLength;
  std::vector<int64_t> values;
  for (size_t i = 0; i < nVals; ++i) {
    for (size_t j = 0; j < runLength; ++j) {
      values.push_back(static_cast<int64_t>(i));
    }
  }

  const unsigned char bytes[] = {0x04, 0x00, 0x04, 0x02, 0x04, 0x04, 0x04,
                                 0x06, 0x04, 0x08, 0x04, 0x0a, 0x04, 0x0c,
                                 0x04, 0x0e, 0x04, 0x10, 0x04, 0x12};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, count), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, count), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, count), 7);
  checkResults(values, decodeRLEv2(bytes, l, count, count), count);
};

TEST(RLEv2, multiByteShortRepeats) {
  const size_t runLength = 7;
  const size_t nVals = 3;
  const size_t count = nVals * runLength;
  std::vector<int64_t> values;
  for (size_t i = 0; i < nVals; ++i) {
    for (size_t j = 0; j < runLength; ++j) {
      values.push_back(static_cast<int64_t>(i) + (1L << 62));
    }
  }

  const unsigned char bytes[] = {0x3c, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
                                 0x00, 0x00, 0x3c, 0x80, 0x00, 0x00, 0x00,
                                 0x00, 0x00, 0x00, 0x02, 0x3c, 0x80, 0x00,
                                 0x00, 0x00, 0x00, 0x00, 0x00, 0x04};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, count), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, count), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, count), 7);
  checkResults(values, decodeRLEv2(bytes, l, count, count), count);
};

TEST(RLEv2, 0to2Repeat1Direct) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {0x46, 0x02, 0x02, 0x40};
  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(
              buffer, VELOX_ARRAY_SIZE(buffer))),
      RleVersion_2,
      *scopedPool,
      true /* doesn't matter */,
      INT_BYTE_SIZE /* doesn't matter */);
  std::vector<int64_t> data(3);
  rle->next(data.data(), 3, nullptr);

  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(i, data[i]) << "Output wrong at " << i;
  }
};

TEST(RLEv2, bitSize2Direct) {
  // 0,1 repeated 10 times (signed ints)
  const size_t count = 20;
  std::vector<int64_t> values;
  for (size_t i = 0; i < count; ++i) {
    values.push_back(i % 2);
  }

  const unsigned char bytes[] = {0x42, 0x13, 0x22, 0x22, 0x22, 0x22, 0x22};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, count), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, count), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, count), 7);
  checkResults(values, decodeRLEv2(bytes, l, count, count), count);
};

TEST(RLEv2, bitSize4Direct) {
  // 0,2 repeated 10 times (signed ints)
  const size_t count = 20;
  std::vector<int64_t> values;
  for (size_t i = 0; i < count; ++i) {
    values.push_back((i % 2) * 2);
  }

  const unsigned char bytes[] = {
      0x46, 0x13, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04};
  unsigned long l = sizeof(bytes) / sizeof(char);

  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, count), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, count), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, count), 7);
  checkResults(values, decodeRLEv2(bytes, l, count, count), count);
};

TEST(RLEv2, multipleRunsDirect) {
  std::vector<int64_t> values;
  // 0,1 repeated 10 times (signed ints)
  for (size_t i = 0; i < 20; ++i) {
    values.push_back(i % 2);
  }
  // 0,2 repeated 10 times (signed ints)
  for (size_t i = 0; i < 20; ++i) {
    values.push_back((i % 2) * 2);
  }

  const unsigned char bytes[] = {
      0x42,
      0x13,
      0x22,
      0x22,
      0x22,
      0x22,
      0x22,
      0x46,
      0x13,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04};
  unsigned long l = sizeof(bytes) / sizeof(char);

  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, largeNegativesDirect) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {
      0x7e, 0x04, 0xcf, 0xca, 0xcc, 0x91, 0xba, 0x38, 0x93, 0xab, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x02, 0x99, 0xa5, 0xcc, 0x28, 0x03, 0xf7, 0xe0, 0xff};
  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(
              buffer, VELOX_ARRAY_SIZE(buffer))),
      RleVersion_2,
      *scopedPool,
      true /* doesn't matter */,
      INT_BYTE_SIZE /* doesn't matter */);
  std::vector<int64_t> data(5);
  rle->next(data.data(), 5, nullptr);

  EXPECT_EQ(-7486502418706614742, data[0]) << "Output wrong at " << 0;
  EXPECT_EQ(0, data[1]) << "Output wrong at " << 1;
  EXPECT_EQ(1, data[2]) << "Output wrong at " << 2;
  EXPECT_EQ(1, data[3]) << "Output wrong at " << 3;
  EXPECT_EQ(-5535739865598783616, data[4]) << "Output wrong at " << 4;
};

TEST(RLEv2, overflowDirect) {
  std::vector<int64_t> values(4);
  values[0] = 4513343538618202719l;
  values[1] = 4513343538618202711l;
  values[2] = 2911390882471569739l;
  values[3] = -9181829309989854913l;

  const unsigned char bytes[] = {
      0x7e, 0x03, 0x7d, 0x45, 0x3c, 0x12, 0x41, 0x48, 0xf4, 0xbe, 0x7d, 0x45,
      0x3c, 0x12, 0x41, 0x48, 0xf4, 0xae, 0x50, 0xce, 0xad, 0x2a, 0x30, 0x0e,
      0xd2, 0x96, 0xfe, 0xd8, 0xd2, 0x38, 0x54, 0x6e, 0x3d, 0x81};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, basicPatched0) {
  long v[] = {2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090};
  std::vector<int64_t> values;
  for (size_t i = 0; i < sizeof(v) / sizeof(long); ++i) {
    values.push_back(v[i]);
  }

  const unsigned char bytes[] = {
      0x8e,
      0x09,
      0x2b,
      0x21,
      0x07,
      0xd0,
      0x1e,
      0x00,
      0x14,
      0x70,
      0x28,
      0x32,
      0x3c,
      0x46,
      0x50,
      0x5a,
      0xfc,
      0xe8};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, basicPatched1) {
  long v[] = {20, 2,   3,    2,   1,  3, 17,  71,  35, 2,   1,    139,   2,
              2,  3,   1783, 475, 2,  1, 1,   3,   1,  3,   2,    32,    1,
              2,  3,   1,    8,   30, 1, 3,   414, 1,  1,   135,  3,     3,
              1,  414, 2,    1,   2,  2, 594, 2,   5,  6,   4,    11,    1,
              2,  2,   1,    1,   52, 4, 1,   2,   7,  1,   17,   334,   1,
              2,  1,   2,    2,   6,  1, 266, 1,   2,  217, 2,    6,     2,
              13, 2,   2,    1,   2,  3, 5,   1,   2,  1,   7244, 11813, 1,
              33, 2,   -13,  1,   2,  3, 13,  1,   92, 3,   13,   5,     14,
              9,  141, 12,   6,   15, 25};
  std::vector<int64_t> values;
  for (size_t i = 0; i < sizeof(v) / sizeof(long); ++i) {
    values.push_back(v[i]);
  }

  const unsigned char bytes[] = {
      0x90, 0x6d, 0x04, 0xa4, 0x8d, 0x10, 0x83, 0xc2, 0x00, 0xf0, 0x70, 0x40,
      0x3c, 0x54, 0x18, 0x03, 0xc1, 0xc9, 0x80, 0x78, 0x3c, 0x21, 0x04, 0xf4,
      0x03, 0xc1, 0xc0, 0xe0, 0x80, 0x38, 0x20, 0x0f, 0x16, 0x83, 0x81, 0xe1,
      0x00, 0x70, 0x54, 0x56, 0x0e, 0x08, 0x6a, 0xc1, 0xc0, 0xe4, 0xa0, 0x40,
      0x20, 0x0e, 0xd5, 0x83, 0xc1, 0xc0, 0xf0, 0x79, 0x7c, 0x1e, 0x12, 0x09,
      0x84, 0x43, 0x00, 0xe0, 0x78, 0x3c, 0x1c, 0x0e, 0x20, 0x84, 0x41, 0xc0,
      0xf0, 0xa0, 0x38, 0x3d, 0x5b, 0x07, 0x03, 0xc1, 0xc0, 0xf0, 0x78, 0x4c,
      0x1d, 0x17, 0x07, 0x03, 0xdc, 0xc0, 0xf0, 0x98, 0x3c, 0x34, 0x0f, 0x07,
      0x83, 0x81, 0xe1, 0x00, 0x90, 0x38, 0x1e, 0x0e, 0x2c, 0x8c, 0x81, 0xc2,
      0xe0, 0x78, 0x00, 0x1c, 0x0f, 0x08, 0x06, 0x81, 0xc6, 0x90, 0x80, 0x68,
      0x24, 0x1b, 0x0b, 0x26, 0x83, 0x21, 0x30, 0xe0, 0x98, 0x3c, 0x6f, 0x06,
      0xb7, 0x03, 0x70};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, mixedPatchedAndShortRepeats) {
  long v[] = {20,  2,   3,    2,   1,  3,  17,  71,   35,  2,   1,    139,   2,
              2,   3,   1783, 475, 2,  1,  1,   3,    1,   3,   2,    32,    1,
              2,   3,   1,    8,   30, 1,  3,   414,  1,   1,   135,  3,     3,
              1,   414, 2,    1,   2,  2,  594, 2,    5,   6,   4,    11,    1,
              2,   2,   1,    1,   52, 4,  1,   2,    7,   1,   17,   334,   1,
              2,   1,   2,    2,   6,  1,  266, 1,    2,   217, 2,    6,     2,
              13,  2,   2,    1,   2,  3,  5,   1,    2,   1,   7244, 11813, 1,
              33,  2,   -13,  1,   2,  3,  13,  1,    92,  3,   13,   5,     14,
              9,   141, 12,   6,   15, 25, 1,   1,    1,   46,  2,    1,     1,
              141, 3,   1,    1,   1,  1,  2,   1,    4,   34,  5,    78,    8,
              1,   2,   2,    1,   9,  10, 2,   1,    4,   13,  1,    5,     4,
              4,   19,  5,    1,   1,  1,  68,  33,   399, 1,   1885, 25,    5,
              2,   4,   1,    1,   2,  16, 1,   2966, 3,   1,   1,    25501, 1,
              1,   1,   66,   1,   3,  8,  131, 14,   5,   1,   2,    2,     1,
              1,   8,   1,    1,   2,  1,  5,   9,    2,   3,   112,  13,    2,
              2,   1,   5,    10,  3,  1,  1,   13,   2,   3,   4,    1,     3,
              1,   1,   2,    1,   1,  2,  4,   2,    207, 1,   1,    2,     4,
              3,   3,   2,    2,   16};
  std::vector<int64_t> values;
  for (size_t i = 0; i < sizeof(v) / sizeof(long); ++i) {
    values.push_back(v[i]);
  }

  const unsigned char bytes[] = {
      0x90, 0x6d, 0x04, 0xa4, 0x8d, 0x10, 0x83, 0xc2, 0x00, 0xf0, 0x70, 0x40,
      0x3c, 0x54, 0x18, 0x03, 0xc1, 0xc9, 0x80, 0x78, 0x3c, 0x21, 0x04, 0xf4,
      0x03, 0xc1, 0xc0, 0xe0, 0x80, 0x38, 0x20, 0x0f, 0x16, 0x83, 0x81, 0xe1,
      0x00, 0x70, 0x54, 0x56, 0x0e, 0x08, 0x6a, 0xc1, 0xc0, 0xe4, 0xa0, 0x40,
      0x20, 0x0e, 0xd5, 0x83, 0xc1, 0xc0, 0xf0, 0x79, 0x7c, 0x1e, 0x12, 0x09,
      0x84, 0x43, 0x00, 0xe0, 0x78, 0x3c, 0x1c, 0x0e, 0x20, 0x84, 0x41, 0xc0,
      0xf0, 0xa0, 0x38, 0x3d, 0x5b, 0x07, 0x03, 0xc1, 0xc0, 0xf0, 0x78, 0x4c,
      0x1d, 0x17, 0x07, 0x03, 0xdc, 0xc0, 0xf0, 0x98, 0x3c, 0x34, 0x0f, 0x07,
      0x83, 0x81, 0xe1, 0x00, 0x90, 0x38, 0x1e, 0x0e, 0x2c, 0x8c, 0x81, 0xc2,
      0xe0, 0x78, 0x00, 0x1c, 0x0f, 0x08, 0x06, 0x81, 0xc6, 0x90, 0x80, 0x68,
      0x24, 0x1b, 0x0b, 0x26, 0x83, 0x21, 0x30, 0xe0, 0x98, 0x3c, 0x6f, 0x06,
      0xb7, 0x03, 0x70, 0x00, 0x02, 0x5e, 0x05, 0x00, 0x5c, 0x00, 0x04, 0x00,
      0x02, 0x00, 0x02, 0x01, 0x1a, 0x00, 0x06, 0x01, 0x02, 0x8a, 0x16, 0x00,
      0x41, 0x01, 0x04, 0x00, 0xe1, 0x10, 0xd1, 0xc0, 0x04, 0x10, 0x08, 0x24,
      0x10, 0x03, 0x30, 0x01, 0x03, 0x0d, 0x21, 0x00, 0xb0, 0x00, 0x02, 0x5e,
      0x12, 0x00, 0x88, 0x00, 0x42, 0x03, 0x1e, 0x00, 0x02, 0x0e, 0xba, 0x00,
      0x32, 0x00, 0x0a, 0x00, 0x04, 0x00, 0x08, 0x00, 0x02, 0x00, 0x02, 0x00,
      0x04, 0x00, 0x20, 0x00, 0x02, 0x17, 0x2c, 0x00, 0x06, 0x00, 0x02, 0x00,
      0x02, 0xc7, 0x3a, 0x00, 0x02, 0x8c, 0x36, 0x00, 0xa2, 0x01, 0x82, 0x00,
      0x10, 0x70, 0x43, 0x42, 0x00, 0x02, 0x04, 0x00, 0x00, 0xe0, 0x00, 0x01,
      0x00, 0x10, 0x40, 0x10, 0x5b, 0xc6, 0x01, 0x02, 0x00, 0x20, 0x90, 0x40,
      0x00, 0x0c, 0x02, 0x08, 0x18, 0x00, 0x40, 0x00, 0x01, 0x00, 0x00, 0x08,
      0x30, 0x33, 0x80, 0x00, 0x02, 0x0c, 0x10, 0x20, 0x20, 0x47, 0x80, 0x13,
      0x4c};
  unsigned long l = sizeof(bytes) / sizeof(char);
  // Read 1 at a time, then 3 at a time, etc.
  checkResults(values, decodeRLEv2(bytes, l, 1, values.size()), 1);
  checkResults(values, decodeRLEv2(bytes, l, 3, values.size()), 3);
  checkResults(values, decodeRLEv2(bytes, l, 7, values.size()), 7);
  checkResults(
      values,
      decodeRLEv2(bytes, l, values.size(), values.size()),
      values.size());
};

TEST(RLEv2, basicDirectSeek) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  // 0,1 repeated 10 times (signed ints) followed by
  // 0,2 repeated 10 times (signed ints)
  const unsigned char bytes[] = {
      0x42,
      0x13,
      0x22,
      0x22,
      0x22,
      0x22,
      0x22,
      0x46,
      0x13,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04,
      0x04};
  unsigned long l = sizeof(bytes) / sizeof(char);

  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(bytes, l)),
      RleVersion_2,
      *scopedPool,
      true /* doesn't matter */,
      INT_BYTE_SIZE /* doesn't matter */);
  std::vector<uint64_t> position;
  position.push_back(7); // byte position; skip first 20 [0 to 19]
  position.push_back(13); // value position; skip 13 more [20 to 32]

  dwio::common::PositionProvider location(position);
  rle->seekToRowGroup(location);
  std::vector<int64_t> data(3);
  rle->next(data.data(), 3, nullptr);
  EXPECT_EQ(2, data[0]);
  EXPECT_EQ(0, data[1]);
  EXPECT_EQ(2, data[2]);
  rle->next(data.data(), 3, nullptr);
  EXPECT_EQ(0, data[0]);
  EXPECT_EQ(2, data[1]);
  EXPECT_EQ(0, data[2]);
  rle->next(data.data(), 1, nullptr);
  EXPECT_EQ(2, data[0]);
};

TEST(RLEv2, bitsLeftByPreviousStream) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  // test for #109
  // 118 DIRECT values, followed by PATHCED values
  const unsigned char bytes[] = {
      0x5a, 0x75, 0x92, 0x42, 0x49, 0x09, 0x2b, 0xa4, 0xae, 0x92, 0xc2, 0x4b,
      0x89, 0x2f, 0x24, 0xbc, 0x93, 0x2a, 0x4c, 0xa9, 0x34, 0x24, 0xe0, 0x93,
      0x92, 0x4e, 0xe9, 0x40, 0xa5, 0x04, 0x94, 0x12, 0x62, 0xa9, 0xc9, 0xa7,
      0x26, 0x9c, 0xaa, 0x73, 0x09, 0xcd, 0x27, 0x34, 0x9c, 0xf2, 0x74, 0x49,
      0xd3, 0x27, 0x50, 0x9d, 0x42, 0x75, 0x29, 0xd4, 0xa7, 0x5a, 0x9d, 0xaa,
      0x79, 0x89, 0xe9, 0x27, 0xa4, 0x9e, 0xea, 0x7c, 0x29, 0xf6, 0x27, 0xdc,
      0x9f, 0xb2, 0x7f, 0x4a, 0x00, 0xa8, 0x14, 0xa0, 0x72, 0x82, 0x8a, 0x19,
      0x28, 0x6e, 0xa2, 0x52, 0x89, 0x4a, 0x28, 0x28, 0xa6, 0xa2, 0x9a, 0x8b,
      0x6a, 0x2d, 0xa8, 0xb8, 0xa2, 0xe2, 0x8b, 0xaa, 0x53, 0xa9, 0x54, 0xa5,
      0x92, 0x98, 0x6a, 0x62, 0xa9, 0x9c, 0xa6, 0x8a, 0x9b, 0xea, 0x70, 0x29,
      0xd2, 0xa7, 0x52, 0x9d, 0x4a, 0x77, 0x29, 0xe0, 0xa7, 0xa2, 0x9e, 0xaa,
      0x7b, 0x29, 0xf0, 0xa7, 0xd2, 0xa0, 0x0a, 0x84, 0x2a, 0x18, 0xa8, 0x72,
      0xa1, 0xca, 0x89, 0x2a, 0x30, 0xa9, 0x4a, 0xa5, 0x4a, 0x96, 0x2a, 0xae,
      0xab, 0x02, 0xac, 0x2b, 0x8d, 0x2e, 0x60, 0xb9, 0x82, 0xe7, 0x2b, 0x9f,
      0xae, 0x84, 0xba, 0x52, 0xe9, 0xeb, 0xad, 0x2e, 0xb6, 0xbc, 0x32, 0xf1,
      0xcb, 0xcc, 0x2f, 0x42, 0xbd, 0x8a, 0xf7, 0xcb, 0xe1, 0xaf, 0xa4, 0xbe,
      0x9a, 0xfa, 0x6b, 0xeb, 0xaf, 0xba, 0xbe, 0xea, 0xfd, 0x2b, 0xf4, 0xaf,
      0xd8, 0xbf, 0xfb, 0x00,
      0x80, // <= end of DIRECT, start of PATCHED =>
      0x90, 0x6d, 0x04, 0xa4, 0x8d, 0x10, 0x83, 0xc2, 0x00, 0xf0, 0x70, 0x40,
      0x3c, 0x54, 0x18, 0x03, 0xc1, 0xc9, 0x80, 0x78, 0x3c, 0x21, 0x04, 0xf4,
      0x03, 0xc1, 0xc0, 0xe0, 0x80, 0x38, 0x20, 0x0f, 0x16, 0x83, 0x81, 0xe1,
      0x00, 0x70, 0x54, 0x56, 0x0e, 0x08, 0x6a, 0xc1, 0xc0, 0xe4, 0xa0, 0x40,
      0x20, 0x0e, 0xd5, 0x83, 0xc1, 0xc0, 0xf0, 0x79, 0x7c, 0x1e, 0x12, 0x09,
      0x84, 0x43, 0x00, 0xe0, 0x78, 0x3c, 0x1c, 0x0e, 0x20, 0x84, 0x41, 0xc0,
      0xf0, 0xa0, 0x38, 0x3d, 0x5b, 0x07, 0x03, 0xc1, 0xc0, 0xf0, 0x78, 0x4c,
      0x1d, 0x17, 0x07, 0x03, 0xdc, 0xc0, 0xf0, 0x98, 0x3c, 0x34, 0x0f, 0x07,
      0x83, 0x81, 0xe1, 0x00, 0x90, 0x38, 0x1e, 0x0e, 0x2c, 0x8c, 0x81, 0xc2,
      0xe0, 0x78, 0x00, 0x1c, 0x0f, 0x08, 0x06, 0x81, 0xc6, 0x90, 0x80, 0x68,
      0x24, 0x1b, 0x0b, 0x26, 0x83, 0x21, 0x30, 0xe0, 0x98, 0x3c, 0x6f, 0x06,
      0xb7, 0x03, 0x70};
  unsigned long l = sizeof(bytes) / sizeof(unsigned char);

  // PATCHED values.
  long v[] = {20, 2,   3,    2,   1,  3, 17,  71,  35, 2,   1,    139,   2,
              2,  3,   1783, 475, 2,  1, 1,   3,   1,  3,   2,    32,    1,
              2,  3,   1,    8,   30, 1, 3,   414, 1,  1,   135,  3,     3,
              1,  414, 2,    1,   2,  2, 594, 2,   5,  6,   4,    11,    1,
              2,  2,   1,    1,   52, 4, 1,   2,   7,  1,   17,   334,   1,
              2,  1,   2,    2,   6,  1, 266, 1,   2,  217, 2,    6,     2,
              13, 2,   2,    1,   2,  3, 5,   1,   2,  1,   7244, 11813, 1,
              33, 2,   -13,  1,   2,  3, 13,  1,   92, 3,   13,   5,     14,
              9,  141, 12,   6,   15, 25};
  unsigned long D = 118, P = sizeof(v) / sizeof(long), N = D + P;

  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(bytes, l)),
      RleVersion_2,
      *scopedPool,
      true /* doesn't matter */,
      INT_BYTE_SIZE /* doesn't matter */);

  std::vector<int64_t> data(N);
  rle->next(data.data(), N, nullptr);
  // check patched values
  for (size_t i = 0; i < P; ++i) {
    EXPECT_EQ(v[i], data[i + D]);
  }
};

TEST(RLEv1, simpleTest) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {
      0x61, 0xff, 0x64, 0xfb, 0x02, 0x03, 0x5, 0x7, 0xb};
  std::unique_ptr<IntDecoder<false>> rle = IntDecoder<false>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(
              buffer, VELOX_ARRAY_SIZE(buffer))),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(105);
  rle->next(data.data(), 105, nullptr);

  for (size_t i = 0; i < 100; ++i) {
    EXPECT_EQ(100 - i, data[i]) << "Output wrong at " << i;
  }
  EXPECT_EQ(2, data[100]);
  EXPECT_EQ(3, data[101]);
  EXPECT_EQ(5, data[102]);
  EXPECT_EQ(7, data[103]);
  EXPECT_EQ(11, data[104]);
};

TEST(RLEv1, signedNullLiteralTest) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {0xf8, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7};
  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(
              buffer, VELOX_ARRAY_SIZE(buffer))),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(8);
  std::vector<uint64_t> nulls(1, ~0);
  rle->next(data.data(), 8, nulls.data());

  for (size_t i = 0; i < 8; ++i) {
    EXPECT_EQ(i % 2 == 0 ? i / 2 : -((i + 1) / 2), data[i]);
  }
}

TEST(RLEv1, splitHeader) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {0x0, 0x00, 0xdc, 0xba, 0x98, 0x76};
  std::unique_ptr<IntDecoder<false>> rle = IntDecoder<false>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(
              buffer, VELOX_ARRAY_SIZE(buffer), 4)),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(200);
  rle->next(data.data(), 3, nullptr);

  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(247864668, data[i]) << "Output wrong at " << i;
  }
}

TEST(RLEv1, splitRuns) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {
      0x7d, 0x01, 0xff, 0x01, 0xfb, 0x01, 0x02, 0x03, 0x04, 0x05};
  dwio::common::SeekableInputStream* const stream =
      new dwio::common::SeekableArrayInputStream(
          buffer, VELOX_ARRAY_SIZE(buffer));
  std::unique_ptr<IntDecoder<false>> rle = IntDecoder<false>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(stream),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(200);
  for (size_t i = 0; i < 42; ++i) {
    rle->next(data.data(), 3, nullptr);
    for (size_t j = 0; j < 3; ++j) {
      EXPECT_EQ(255 + i * 3 + j, data[j])
          << "Wrong output at " << i << ", " << j;
    }
  }
  rle->next(data.data(), 3, nullptr);
  EXPECT_EQ(381, data[0]);
  EXPECT_EQ(382, data[1]);
  EXPECT_EQ(1, data[2]);
  rle->next(data.data(), 3, nullptr);
  EXPECT_EQ(2, data[0]);
  EXPECT_EQ(3, data[1]);
  EXPECT_EQ(4, data[2]);
  rle->next(data.data(), 1, nullptr);
  EXPECT_EQ(5, data[0]);
}

TEST(RLEv1, testSigned) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {0x7f, 0xff, 0x20};
  dwio::common::SeekableInputStream* const stream =
      new dwio::common::SeekableArrayInputStream(
          buffer, VELOX_ARRAY_SIZE(buffer));
  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(stream),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(100);
  rle->next(data.data(), data.size(), nullptr);
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(16 - i, data[i]) << "Wrong output at " << i;
  }
  rle->next(data.data(), 30, nullptr);
  for (size_t i = 0; i < 30; ++i) {
    EXPECT_EQ(16 - 100 - static_cast<long>(i), data[i])
        << "Wrong output at " << (i + 100);
  }
}

TEST(RLEv1, testNull) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {0x75, 0x02, 0x00};
  dwio::common::SeekableInputStream* const stream =
      new dwio::common::SeekableArrayInputStream(
          buffer, VELOX_ARRAY_SIZE(buffer));
  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(stream),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(24);
  uint64_t nulls[1] = {bits::kNotNull64};
  for (size_t i = 0; i < data.size(); ++i) {
    bits::setNull(nulls, i, i % 2);
  }
  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < data.size(); ++j) {
      data[j] = -1;
    }
    rle->next(data.data(), 24, nulls);
    for (size_t j = 0; j < 24; ++j) {
      if (!bits::isBitNull(nulls, j)) {
        EXPECT_EQ(i * 24 + j, data[j]);
      } else {
        EXPECT_EQ(-1, data[j]);
      }
    }
  }
}

TEST(RLEv1, testAllNulls) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {0xf0, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
                                  0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                                  0x0d, 0x0e, 0x0f, 0x3d, 0x00, 0x12};
  dwio::common::SeekableInputStream* const stream =
      new dwio::common::SeekableArrayInputStream(
          buffer, VELOX_ARRAY_SIZE(buffer));
  std::unique_ptr<IntDecoder<false>> rle = IntDecoder<false>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(stream),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(16, -1);
  std::vector<uint64_t> allNull(1, bits::kNull64);
  std::vector<uint64_t> noNull(1, bits::kNotNull64);
  rle->next(data.data(), 16, allNull.data());
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(-1, data[i]) << "Output wrong at " << i;
  }
  rle->next(data.data(), data.size(), noNull.data());
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(i, data[i]) << "Output wrong at " << i;
    data[i] = -1;
  }
  rle->next(data.data(), data.size(), allNull.data());
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(-1, data[i]) << "Output wrong at " << i;
  }
  for (size_t i = 0; i < 4; ++i) {
    rle->next(data.data(), data.size(), noNull.data());
    for (size_t j = 0; j < data.size(); ++j) {
      EXPECT_EQ(18, data[j]) << "Output wrong at " << i;
    }
  }
  rle->next(data.data(), data.size(), allNull.data());
}

TEST(RLEv1, skipTest) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  // Create the RLE stream from Java's TestRunLengthIntegerEncoding.testSkips
  // for (size_t i = 0; i < 1024; ++i)
  //   out.write(i);
  // for (size_t i = 1024; i < 2048; ++i)
  //   out.write(i * 256);
  // This causes the first half to be delta encoded and the second half to
  // be literal encoded.
  const unsigned char buffer[] = {
      127, 1,   0,   127, 1,   132, 2,   127, 1,   136, 4,   127, 1,   140, 6,
      127, 1,   144, 8,   127, 1,   148, 10,  127, 1,   152, 12,  111, 1,   156,
      14,  128, 128, 128, 32,  128, 132, 32,  128, 136, 32,  128, 140, 32,  128,
      144, 32,  128, 148, 32,  128, 152, 32,  128, 156, 32,  128, 160, 32,  128,
      164, 32,  128, 168, 32,  128, 172, 32,  128, 176, 32,  128, 180, 32,  128,
      184, 32,  128, 188, 32,  128, 192, 32,  128, 196, 32,  128, 200, 32,  128,
      204, 32,  128, 208, 32,  128, 212, 32,  128, 216, 32,  128, 220, 32,  128,
      224, 32,  128, 228, 32,  128, 232, 32,  128, 236, 32,  128, 240, 32,  128,
      244, 32,  128, 248, 32,  128, 252, 32,  128, 128, 33,  128, 132, 33,  128,
      136, 33,  128, 140, 33,  128, 144, 33,  128, 148, 33,  128, 152, 33,  128,
      156, 33,  128, 160, 33,  128, 164, 33,  128, 168, 33,  128, 172, 33,  128,
      176, 33,  128, 180, 33,  128, 184, 33,  128, 188, 33,  128, 192, 33,  128,
      196, 33,  128, 200, 33,  128, 204, 33,  128, 208, 33,  128, 212, 33,  128,
      216, 33,  128, 220, 33,  128, 224, 33,  128, 228, 33,  128, 232, 33,  128,
      236, 33,  128, 240, 33,  128, 244, 33,  128, 248, 33,  128, 252, 33,  128,
      128, 34,  128, 132, 34,  128, 136, 34,  128, 140, 34,  128, 144, 34,  128,
      148, 34,  128, 152, 34,  128, 156, 34,  128, 160, 34,  128, 164, 34,  128,
      168, 34,  128, 172, 34,  128, 176, 34,  128, 180, 34,  128, 184, 34,  128,
      188, 34,  128, 192, 34,  128, 196, 34,  128, 200, 34,  128, 204, 34,  128,
      208, 34,  128, 212, 34,  128, 216, 34,  128, 220, 34,  128, 224, 34,  128,
      228, 34,  128, 232, 34,  128, 236, 34,  128, 240, 34,  128, 244, 34,  128,
      248, 34,  128, 252, 34,  128, 128, 35,  128, 132, 35,  128, 136, 35,  128,
      140, 35,  128, 144, 35,  128, 148, 35,  128, 152, 35,  128, 156, 35,  128,
      160, 35,  128, 164, 35,  128, 168, 35,  128, 172, 35,  128, 176, 35,  128,
      180, 35,  128, 184, 35,  128, 188, 35,  128, 192, 35,  128, 196, 35,  128,
      200, 35,  128, 204, 35,  128, 208, 35,  128, 212, 35,  128, 216, 35,  128,
      220, 35,  128, 224, 35,  128, 228, 35,  128, 232, 35,  128, 236, 35,  128,
      240, 35,  128, 244, 35,  128, 248, 35,  128, 252, 35,  128, 128, 128, 36,
      128, 132, 36,  128, 136, 36,  128, 140, 36,  128, 144, 36,  128, 148, 36,
      128, 152, 36,  128, 156, 36,  128, 160, 36,  128, 164, 36,  128, 168, 36,
      128, 172, 36,  128, 176, 36,  128, 180, 36,  128, 184, 36,  128, 188, 36,
      128, 192, 36,  128, 196, 36,  128, 200, 36,  128, 204, 36,  128, 208, 36,
      128, 212, 36,  128, 216, 36,  128, 220, 36,  128, 224, 36,  128, 228, 36,
      128, 232, 36,  128, 236, 36,  128, 240, 36,  128, 244, 36,  128, 248, 36,
      128, 252, 36,  128, 128, 37,  128, 132, 37,  128, 136, 37,  128, 140, 37,
      128, 144, 37,  128, 148, 37,  128, 152, 37,  128, 156, 37,  128, 160, 37,
      128, 164, 37,  128, 168, 37,  128, 172, 37,  128, 176, 37,  128, 180, 37,
      128, 184, 37,  128, 188, 37,  128, 192, 37,  128, 196, 37,  128, 200, 37,
      128, 204, 37,  128, 208, 37,  128, 212, 37,  128, 216, 37,  128, 220, 37,
      128, 224, 37,  128, 228, 37,  128, 232, 37,  128, 236, 37,  128, 240, 37,
      128, 244, 37,  128, 248, 37,  128, 252, 37,  128, 128, 38,  128, 132, 38,
      128, 136, 38,  128, 140, 38,  128, 144, 38,  128, 148, 38,  128, 152, 38,
      128, 156, 38,  128, 160, 38,  128, 164, 38,  128, 168, 38,  128, 172, 38,
      128, 176, 38,  128, 180, 38,  128, 184, 38,  128, 188, 38,  128, 192, 38,
      128, 196, 38,  128, 200, 38,  128, 204, 38,  128, 208, 38,  128, 212, 38,
      128, 216, 38,  128, 220, 38,  128, 224, 38,  128, 228, 38,  128, 232, 38,
      128, 236, 38,  128, 240, 38,  128, 244, 38,  128, 248, 38,  128, 252, 38,
      128, 128, 39,  128, 132, 39,  128, 136, 39,  128, 140, 39,  128, 144, 39,
      128, 148, 39,  128, 152, 39,  128, 156, 39,  128, 160, 39,  128, 164, 39,
      128, 168, 39,  128, 172, 39,  128, 176, 39,  128, 180, 39,  128, 184, 39,
      128, 188, 39,  128, 192, 39,  128, 196, 39,  128, 200, 39,  128, 204, 39,
      128, 208, 39,  128, 212, 39,  128, 216, 39,  128, 220, 39,  128, 224, 39,
      128, 228, 39,  128, 232, 39,  128, 236, 39,  128, 240, 39,  128, 244, 39,
      128, 248, 39,  128, 252, 39,  128, 128, 128, 40,  128, 132, 40,  128, 136,
      40,  128, 140, 40,  128, 144, 40,  128, 148, 40,  128, 152, 40,  128, 156,
      40,  128, 160, 40,  128, 164, 40,  128, 168, 40,  128, 172, 40,  128, 176,
      40,  128, 180, 40,  128, 184, 40,  128, 188, 40,  128, 192, 40,  128, 196,
      40,  128, 200, 40,  128, 204, 40,  128, 208, 40,  128, 212, 40,  128, 216,
      40,  128, 220, 40,  128, 224, 40,  128, 228, 40,  128, 232, 40,  128, 236,
      40,  128, 240, 40,  128, 244, 40,  128, 248, 40,  128, 252, 40,  128, 128,
      41,  128, 132, 41,  128, 136, 41,  128, 140, 41,  128, 144, 41,  128, 148,
      41,  128, 152, 41,  128, 156, 41,  128, 160, 41,  128, 164, 41,  128, 168,
      41,  128, 172, 41,  128, 176, 41,  128, 180, 41,  128, 184, 41,  128, 188,
      41,  128, 192, 41,  128, 196, 41,  128, 200, 41,  128, 204, 41,  128, 208,
      41,  128, 212, 41,  128, 216, 41,  128, 220, 41,  128, 224, 41,  128, 228,
      41,  128, 232, 41,  128, 236, 41,  128, 240, 41,  128, 244, 41,  128, 248,
      41,  128, 252, 41,  128, 128, 42,  128, 132, 42,  128, 136, 42,  128, 140,
      42,  128, 144, 42,  128, 148, 42,  128, 152, 42,  128, 156, 42,  128, 160,
      42,  128, 164, 42,  128, 168, 42,  128, 172, 42,  128, 176, 42,  128, 180,
      42,  128, 184, 42,  128, 188, 42,  128, 192, 42,  128, 196, 42,  128, 200,
      42,  128, 204, 42,  128, 208, 42,  128, 212, 42,  128, 216, 42,  128, 220,
      42,  128, 224, 42,  128, 228, 42,  128, 232, 42,  128, 236, 42,  128, 240,
      42,  128, 244, 42,  128, 248, 42,  128, 252, 42,  128, 128, 43,  128, 132,
      43,  128, 136, 43,  128, 140, 43,  128, 144, 43,  128, 148, 43,  128, 152,
      43,  128, 156, 43,  128, 160, 43,  128, 164, 43,  128, 168, 43,  128, 172,
      43,  128, 176, 43,  128, 180, 43,  128, 184, 43,  128, 188, 43,  128, 192,
      43,  128, 196, 43,  128, 200, 43,  128, 204, 43,  128, 208, 43,  128, 212,
      43,  128, 216, 43,  128, 220, 43,  128, 224, 43,  128, 228, 43,  128, 232,
      43,  128, 236, 43,  128, 240, 43,  128, 244, 43,  128, 248, 43,  128, 252,
      43,  128, 128, 128, 44,  128, 132, 44,  128, 136, 44,  128, 140, 44,  128,
      144, 44,  128, 148, 44,  128, 152, 44,  128, 156, 44,  128, 160, 44,  128,
      164, 44,  128, 168, 44,  128, 172, 44,  128, 176, 44,  128, 180, 44,  128,
      184, 44,  128, 188, 44,  128, 192, 44,  128, 196, 44,  128, 200, 44,  128,
      204, 44,  128, 208, 44,  128, 212, 44,  128, 216, 44,  128, 220, 44,  128,
      224, 44,  128, 228, 44,  128, 232, 44,  128, 236, 44,  128, 240, 44,  128,
      244, 44,  128, 248, 44,  128, 252, 44,  128, 128, 45,  128, 132, 45,  128,
      136, 45,  128, 140, 45,  128, 144, 45,  128, 148, 45,  128, 152, 45,  128,
      156, 45,  128, 160, 45,  128, 164, 45,  128, 168, 45,  128, 172, 45,  128,
      176, 45,  128, 180, 45,  128, 184, 45,  128, 188, 45,  128, 192, 45,  128,
      196, 45,  128, 200, 45,  128, 204, 45,  128, 208, 45,  128, 212, 45,  128,
      216, 45,  128, 220, 45,  128, 224, 45,  128, 228, 45,  128, 232, 45,  128,
      236, 45,  128, 240, 45,  128, 244, 45,  128, 248, 45,  128, 252, 45,  128,
      128, 46,  128, 132, 46,  128, 136, 46,  128, 140, 46,  128, 144, 46,  128,
      148, 46,  128, 152, 46,  128, 156, 46,  128, 160, 46,  128, 164, 46,  128,
      168, 46,  128, 172, 46,  128, 176, 46,  128, 180, 46,  128, 184, 46,  128,
      188, 46,  128, 192, 46,  128, 196, 46,  128, 200, 46,  128, 204, 46,  128,
      208, 46,  128, 212, 46,  128, 216, 46,  128, 220, 46,  128, 224, 46,  128,
      228, 46,  128, 232, 46,  128, 236, 46,  128, 240, 46,  128, 244, 46,  128,
      248, 46,  128, 252, 46,  128, 128, 47,  128, 132, 47,  128, 136, 47,  128,
      140, 47,  128, 144, 47,  128, 148, 47,  128, 152, 47,  128, 156, 47,  128,
      160, 47,  128, 164, 47,  128, 168, 47,  128, 172, 47,  128, 176, 47,  128,
      180, 47,  128, 184, 47,  128, 188, 47,  128, 192, 47,  128, 196, 47,  128,
      200, 47,  128, 204, 47,  128, 208, 47,  128, 212, 47,  128, 216, 47,  128,
      220, 47,  128, 224, 47,  128, 228, 47,  128, 232, 47,  128, 236, 47,  128,
      240, 47,  128, 244, 47,  128, 248, 47,  128, 252, 47,  128, 128, 128, 48,
      128, 132, 48,  128, 136, 48,  128, 140, 48,  128, 144, 48,  128, 148, 48,
      128, 152, 48,  128, 156, 48,  128, 160, 48,  128, 164, 48,  128, 168, 48,
      128, 172, 48,  128, 176, 48,  128, 180, 48,  128, 184, 48,  128, 188, 48,
      128, 192, 48,  128, 196, 48,  128, 200, 48,  128, 204, 48,  128, 208, 48,
      128, 212, 48,  128, 216, 48,  128, 220, 48,  128, 224, 48,  128, 228, 48,
      128, 232, 48,  128, 236, 48,  128, 240, 48,  128, 244, 48,  128, 248, 48,
      128, 252, 48,  128, 128, 49,  128, 132, 49,  128, 136, 49,  128, 140, 49,
      128, 144, 49,  128, 148, 49,  128, 152, 49,  128, 156, 49,  128, 160, 49,
      128, 164, 49,  128, 168, 49,  128, 172, 49,  128, 176, 49,  128, 180, 49,
      128, 184, 49,  128, 188, 49,  128, 192, 49,  128, 196, 49,  128, 200, 49,
      128, 204, 49,  128, 208, 49,  128, 212, 49,  128, 216, 49,  128, 220, 49,
      128, 224, 49,  128, 228, 49,  128, 232, 49,  128, 236, 49,  128, 240, 49,
      128, 244, 49,  128, 248, 49,  128, 252, 49,  128, 128, 50,  128, 132, 50,
      128, 136, 50,  128, 140, 50,  128, 144, 50,  128, 148, 50,  128, 152, 50,
      128, 156, 50,  128, 160, 50,  128, 164, 50,  128, 168, 50,  128, 172, 50,
      128, 176, 50,  128, 180, 50,  128, 184, 50,  128, 188, 50,  128, 192, 50,
      128, 196, 50,  128, 200, 50,  128, 204, 50,  128, 208, 50,  128, 212, 50,
      128, 216, 50,  128, 220, 50,  128, 224, 50,  128, 228, 50,  128, 232, 50,
      128, 236, 50,  128, 240, 50,  128, 244, 50,  128, 248, 50,  128, 252, 50,
      128, 128, 51,  128, 132, 51,  128, 136, 51,  128, 140, 51,  128, 144, 51,
      128, 148, 51,  128, 152, 51,  128, 156, 51,  128, 160, 51,  128, 164, 51,
      128, 168, 51,  128, 172, 51,  128, 176, 51,  128, 180, 51,  128, 184, 51,
      128, 188, 51,  128, 192, 51,  128, 196, 51,  128, 200, 51,  128, 204, 51,
      128, 208, 51,  128, 212, 51,  128, 216, 51,  128, 220, 51,  128, 224, 51,
      128, 228, 51,  128, 232, 51,  128, 236, 51,  128, 240, 51,  128, 244, 51,
      128, 248, 51,  128, 252, 51,  128, 128, 128, 52,  128, 132, 52,  128, 136,
      52,  128, 140, 52,  128, 144, 52,  128, 148, 52,  128, 152, 52,  128, 156,
      52,  128, 160, 52,  128, 164, 52,  128, 168, 52,  128, 172, 52,  128, 176,
      52,  128, 180, 52,  128, 184, 52,  128, 188, 52,  128, 192, 52,  128, 196,
      52,  128, 200, 52,  128, 204, 52,  128, 208, 52,  128, 212, 52,  128, 216,
      52,  128, 220, 52,  128, 224, 52,  128, 228, 52,  128, 232, 52,  128, 236,
      52,  128, 240, 52,  128, 244, 52,  128, 248, 52,  128, 252, 52,  128, 128,
      53,  128, 132, 53,  128, 136, 53,  128, 140, 53,  128, 144, 53,  128, 148,
      53,  128, 152, 53,  128, 156, 53,  128, 160, 53,  128, 164, 53,  128, 168,
      53,  128, 172, 53,  128, 176, 53,  128, 180, 53,  128, 184, 53,  128, 188,
      53,  128, 192, 53,  128, 196, 53,  128, 200, 53,  128, 204, 53,  128, 208,
      53,  128, 212, 53,  128, 216, 53,  128, 220, 53,  128, 224, 53,  128, 228,
      53,  128, 232, 53,  128, 236, 53,  128, 240, 53,  128, 244, 53,  128, 248,
      53,  128, 252, 53,  128, 128, 54,  128, 132, 54,  128, 136, 54,  128, 140,
      54,  128, 144, 54,  128, 148, 54,  128, 152, 54,  128, 156, 54,  128, 160,
      54,  128, 164, 54,  128, 168, 54,  128, 172, 54,  128, 176, 54,  128, 180,
      54,  128, 184, 54,  128, 188, 54,  128, 192, 54,  128, 196, 54,  128, 200,
      54,  128, 204, 54,  128, 208, 54,  128, 212, 54,  128, 216, 54,  128, 220,
      54,  128, 224, 54,  128, 228, 54,  128, 232, 54,  128, 236, 54,  128, 240,
      54,  128, 244, 54,  128, 248, 54,  128, 252, 54,  128, 128, 55,  128, 132,
      55,  128, 136, 55,  128, 140, 55,  128, 144, 55,  128, 148, 55,  128, 152,
      55,  128, 156, 55,  128, 160, 55,  128, 164, 55,  128, 168, 55,  128, 172,
      55,  128, 176, 55,  128, 180, 55,  128, 184, 55,  128, 188, 55,  128, 192,
      55,  128, 196, 55,  128, 200, 55,  128, 204, 55,  128, 208, 55,  128, 212,
      55,  128, 216, 55,  128, 220, 55,  128, 224, 55,  128, 228, 55,  128, 232,
      55,  128, 236, 55,  128, 240, 55,  128, 244, 55,  128, 248, 55,  128, 252,
      55,  128, 128, 128, 56,  128, 132, 56,  128, 136, 56,  128, 140, 56,  128,
      144, 56,  128, 148, 56,  128, 152, 56,  128, 156, 56,  128, 160, 56,  128,
      164, 56,  128, 168, 56,  128, 172, 56,  128, 176, 56,  128, 180, 56,  128,
      184, 56,  128, 188, 56,  128, 192, 56,  128, 196, 56,  128, 200, 56,  128,
      204, 56,  128, 208, 56,  128, 212, 56,  128, 216, 56,  128, 220, 56,  128,
      224, 56,  128, 228, 56,  128, 232, 56,  128, 236, 56,  128, 240, 56,  128,
      244, 56,  128, 248, 56,  128, 252, 56,  128, 128, 57,  128, 132, 57,  128,
      136, 57,  128, 140, 57,  128, 144, 57,  128, 148, 57,  128, 152, 57,  128,
      156, 57,  128, 160, 57,  128, 164, 57,  128, 168, 57,  128, 172, 57,  128,
      176, 57,  128, 180, 57,  128, 184, 57,  128, 188, 57,  128, 192, 57,  128,
      196, 57,  128, 200, 57,  128, 204, 57,  128, 208, 57,  128, 212, 57,  128,
      216, 57,  128, 220, 57,  128, 224, 57,  128, 228, 57,  128, 232, 57,  128,
      236, 57,  128, 240, 57,  128, 244, 57,  128, 248, 57,  128, 252, 57,  128,
      128, 58,  128, 132, 58,  128, 136, 58,  128, 140, 58,  128, 144, 58,  128,
      148, 58,  128, 152, 58,  128, 156, 58,  128, 160, 58,  128, 164, 58,  128,
      168, 58,  128, 172, 58,  128, 176, 58,  128, 180, 58,  128, 184, 58,  128,
      188, 58,  128, 192, 58,  128, 196, 58,  128, 200, 58,  128, 204, 58,  128,
      208, 58,  128, 212, 58,  128, 216, 58,  128, 220, 58,  128, 224, 58,  128,
      228, 58,  128, 232, 58,  128, 236, 58,  128, 240, 58,  128, 244, 58,  128,
      248, 58,  128, 252, 58,  128, 128, 59,  128, 132, 59,  128, 136, 59,  128,
      140, 59,  128, 144, 59,  128, 148, 59,  128, 152, 59,  128, 156, 59,  128,
      160, 59,  128, 164, 59,  128, 168, 59,  128, 172, 59,  128, 176, 59,  128,
      180, 59,  128, 184, 59,  128, 188, 59,  128, 192, 59,  128, 196, 59,  128,
      200, 59,  128, 204, 59,  128, 208, 59,  128, 212, 59,  128, 216, 59,  128,
      220, 59,  128, 224, 59,  128, 228, 59,  128, 232, 59,  128, 236, 59,  128,
      240, 59,  128, 244, 59,  128, 248, 59,  128, 252, 59,  128, 128, 128, 60,
      128, 132, 60,  128, 136, 60,  128, 140, 60,  128, 144, 60,  128, 148, 60,
      128, 152, 60,  128, 156, 60,  128, 160, 60,  128, 164, 60,  128, 168, 60,
      128, 172, 60,  128, 176, 60,  128, 180, 60,  128, 184, 60,  128, 188, 60,
      128, 192, 60,  128, 196, 60,  128, 200, 60,  128, 204, 60,  128, 208, 60,
      128, 212, 60,  128, 216, 60,  128, 220, 60,  128, 224, 60,  128, 228, 60,
      128, 232, 60,  128, 236, 60,  128, 240, 60,  128, 244, 60,  128, 248, 60,
      128, 252, 60,  128, 128, 61,  128, 132, 61,  128, 136, 61,  128, 140, 61,
      128, 144, 61,  128, 148, 61,  128, 152, 61,  128, 156, 61,  128, 160, 61,
      128, 164, 61,  128, 168, 61,  128, 172, 61,  128, 176, 61,  128, 180, 61,
      128, 184, 61,  128, 188, 61,  128, 192, 61,  128, 196, 61,  128, 200, 61,
      128, 204, 61,  128, 208, 61,  128, 212, 61,  128, 216, 61,  128, 220, 61,
      128, 224, 61,  128, 228, 61,  128, 232, 61,  128, 236, 61,  128, 240, 61,
      128, 244, 61,  128, 248, 61,  128, 252, 61,  128, 128, 62,  128, 132, 62,
      128, 136, 62,  128, 140, 62,  128, 144, 62,  128, 148, 62,  128, 152, 62,
      128, 156, 62,  128, 160, 62,  128, 164, 62,  128, 168, 62,  128, 172, 62,
      128, 176, 62,  128, 180, 62,  128, 184, 62,  128, 188, 62,  128, 192, 62,
      128, 196, 62,  128, 200, 62,  128, 204, 62,  128, 208, 62,  128, 212, 62,
      128, 216, 62,  128, 220, 62,  128, 224, 62,  128, 228, 62,  128, 232, 62,
      128, 236, 62,  128, 240, 62,  128, 244, 62,  128, 248, 62,  128, 252, 62,
      128, 128, 63,  128, 132, 63,  128, 136, 63,  128, 140, 63,  128, 144, 63,
      128, 148, 63,  128, 152, 63,  128, 156, 63,  128, 160, 63,  128, 164, 63,
      128, 168, 63,  128, 172, 63,  128, 176, 63,  128, 180, 63,  128, 184, 63,
      128, 188, 63,  128, 192, 63,  128, 196, 63,  128, 200, 63,  128, 204, 63,
      128, 208, 63,  128, 212, 63,  128, 216, 63,  128, 220, 63,  128, 224, 63,
      128, 228, 63,  128, 232, 63,  128, 236, 63,  128, 240, 63,  128, 244, 63,
      128, 248, 63,  128, 252, 63};
  dwio::common::SeekableInputStream* const stream =
      new dwio::common::SeekableArrayInputStream(
          buffer, VELOX_ARRAY_SIZE(buffer));
  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(stream),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(1);
  for (size_t i = 0; i < 2048; i += 10) {
    rle->next(data.data(), 1, nullptr);
    if (i < 1024) {
      EXPECT_EQ(i, data[0]) << "Wrong output at " << i;
    } else {
      EXPECT_EQ(256 * i, data[0]) << "Wrong output at " << i;
    }
    if (i < 2038) {
      rle->skip(9);
    }
    rle->skip(0);
  }
}

TEST(RLEv1, seekTest) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  // Create the RLE stream from Java's
  // TestRunLengthIntegerEncoding.testUncompressedSeek
  // for (size_t i = 0; i < 1024; ++i)
  //   out.write(i / 4);
  // for (size_t i = 1024; i < 2048; ++i)
  //   out.write(2 * i);
  // for (size_t i = 0; i < 2048; ++i)
  //   out.write(junk[i]);
  // This causes the first half to be delta encoded and the second half to
  // be literal encoded.
  const unsigned char buffer[] = {
      1,   0,   0,   1,   0,   2,   1,   0,   4,   1,   0,   6,   1,   0,   8,
      1,   0,   10,  1,   0,   12,  1,   0,   14,  1,   0,   16,  1,   0,   18,
      1,   0,   20,  1,   0,   22,  1,   0,   24,  1,   0,   26,  1,   0,   28,
      1,   0,   30,  1,   0,   32,  1,   0,   34,  1,   0,   36,  1,   0,   38,
      1,   0,   40,  1,   0,   42,  1,   0,   44,  1,   0,   46,  1,   0,   48,
      1,   0,   50,  1,   0,   52,  1,   0,   54,  1,   0,   56,  1,   0,   58,
      1,   0,   60,  1,   0,   62,  1,   0,   64,  1,   0,   66,  1,   0,   68,
      1,   0,   70,  1,   0,   72,  1,   0,   74,  1,   0,   76,  1,   0,   78,
      1,   0,   80,  1,   0,   82,  1,   0,   84,  1,   0,   86,  1,   0,   88,
      1,   0,   90,  1,   0,   92,  1,   0,   94,  1,   0,   96,  1,   0,   98,
      1,   0,   100, 1,   0,   102, 1,   0,   104, 1,   0,   106, 1,   0,   108,
      1,   0,   110, 1,   0,   112, 1,   0,   114, 1,   0,   116, 1,   0,   118,
      1,   0,   120, 1,   0,   122, 1,   0,   124, 1,   0,   126, 1,   0,   128,
      1,   1,   0,   130, 1,   1,   0,   132, 1,   1,   0,   134, 1,   1,   0,
      136, 1,   1,   0,   138, 1,   1,   0,   140, 1,   1,   0,   142, 1,   1,
      0,   144, 1,   1,   0,   146, 1,   1,   0,   148, 1,   1,   0,   150, 1,
      1,   0,   152, 1,   1,   0,   154, 1,   1,   0,   156, 1,   1,   0,   158,
      1,   1,   0,   160, 1,   1,   0,   162, 1,   1,   0,   164, 1,   1,   0,
      166, 1,   1,   0,   168, 1,   1,   0,   170, 1,   1,   0,   172, 1,   1,
      0,   174, 1,   1,   0,   176, 1,   1,   0,   178, 1,   1,   0,   180, 1,
      1,   0,   182, 1,   1,   0,   184, 1,   1,   0,   186, 1,   1,   0,   188,
      1,   1,   0,   190, 1,   1,   0,   192, 1,   1,   0,   194, 1,   1,   0,
      196, 1,   1,   0,   198, 1,   1,   0,   200, 1,   1,   0,   202, 1,   1,
      0,   204, 1,   1,   0,   206, 1,   1,   0,   208, 1,   1,   0,   210, 1,
      1,   0,   212, 1,   1,   0,   214, 1,   1,   0,   216, 1,   1,   0,   218,
      1,   1,   0,   220, 1,   1,   0,   222, 1,   1,   0,   224, 1,   1,   0,
      226, 1,   1,   0,   228, 1,   1,   0,   230, 1,   1,   0,   232, 1,   1,
      0,   234, 1,   1,   0,   236, 1,   1,   0,   238, 1,   1,   0,   240, 1,
      1,   0,   242, 1,   1,   0,   244, 1,   1,   0,   246, 1,   1,   0,   248,
      1,   1,   0,   250, 1,   1,   0,   252, 1,   1,   0,   254, 1,   1,   0,
      128, 2,   1,   0,   130, 2,   1,   0,   132, 2,   1,   0,   134, 2,   1,
      0,   136, 2,   1,   0,   138, 2,   1,   0,   140, 2,   1,   0,   142, 2,
      1,   0,   144, 2,   1,   0,   146, 2,   1,   0,   148, 2,   1,   0,   150,
      2,   1,   0,   152, 2,   1,   0,   154, 2,   1,   0,   156, 2,   1,   0,
      158, 2,   1,   0,   160, 2,   1,   0,   162, 2,   1,   0,   164, 2,   1,
      0,   166, 2,   1,   0,   168, 2,   1,   0,   170, 2,   1,   0,   172, 2,
      1,   0,   174, 2,   1,   0,   176, 2,   1,   0,   178, 2,   1,   0,   180,
      2,   1,   0,   182, 2,   1,   0,   184, 2,   1,   0,   186, 2,   1,   0,
      188, 2,   1,   0,   190, 2,   1,   0,   192, 2,   1,   0,   194, 2,   1,
      0,   196, 2,   1,   0,   198, 2,   1,   0,   200, 2,   1,   0,   202, 2,
      1,   0,   204, 2,   1,   0,   206, 2,   1,   0,   208, 2,   1,   0,   210,
      2,   1,   0,   212, 2,   1,   0,   214, 2,   1,   0,   216, 2,   1,   0,
      218, 2,   1,   0,   220, 2,   1,   0,   222, 2,   1,   0,   224, 2,   1,
      0,   226, 2,   1,   0,   228, 2,   1,   0,   230, 2,   1,   0,   232, 2,
      1,   0,   234, 2,   1,   0,   236, 2,   1,   0,   238, 2,   1,   0,   240,
      2,   1,   0,   242, 2,   1,   0,   244, 2,   1,   0,   246, 2,   1,   0,
      248, 2,   1,   0,   250, 2,   1,   0,   252, 2,   1,   0,   254, 2,   1,
      0,   128, 3,   1,   0,   130, 3,   1,   0,   132, 3,   1,   0,   134, 3,
      1,   0,   136, 3,   1,   0,   138, 3,   1,   0,   140, 3,   1,   0,   142,
      3,   1,   0,   144, 3,   1,   0,   146, 3,   1,   0,   148, 3,   1,   0,
      150, 3,   1,   0,   152, 3,   1,   0,   154, 3,   1,   0,   156, 3,   1,
      0,   158, 3,   1,   0,   160, 3,   1,   0,   162, 3,   1,   0,   164, 3,
      1,   0,   166, 3,   1,   0,   168, 3,   1,   0,   170, 3,   1,   0,   172,
      3,   1,   0,   174, 3,   1,   0,   176, 3,   1,   0,   178, 3,   1,   0,
      180, 3,   1,   0,   182, 3,   1,   0,   184, 3,   1,   0,   186, 3,   1,
      0,   188, 3,   1,   0,   190, 3,   1,   0,   192, 3,   1,   0,   194, 3,
      1,   0,   196, 3,   1,   0,   198, 3,   1,   0,   200, 3,   1,   0,   202,
      3,   1,   0,   204, 3,   1,   0,   206, 3,   1,   0,   208, 3,   1,   0,
      210, 3,   1,   0,   212, 3,   1,   0,   214, 3,   1,   0,   216, 3,   1,
      0,   218, 3,   1,   0,   220, 3,   1,   0,   222, 3,   1,   0,   224, 3,
      1,   0,   226, 3,   1,   0,   228, 3,   1,   0,   230, 3,   1,   0,   232,
      3,   1,   0,   234, 3,   1,   0,   236, 3,   1,   0,   238, 3,   1,   0,
      240, 3,   1,   0,   242, 3,   1,   0,   244, 3,   1,   0,   246, 3,   1,
      0,   248, 3,   1,   0,   250, 3,   1,   0,   252, 3,   1,   0,   254, 3,
      127, 2,   128, 32,  127, 2,   136, 36,  127, 2,   144, 40,  127, 2,   152,
      44,  127, 2,   160, 48,  127, 2,   168, 52,  127, 2,   176, 56,  111, 2,
      184, 60,  128, 147, 150, 232, 240, 8,   168, 134, 179, 187, 12,  246, 145,
      173, 142, 11,  241, 162, 190, 162, 9,   239, 218, 128, 243, 5,   202, 175,
      131, 196, 12,  151, 253, 204, 160, 4,   229, 167, 247, 255, 12,  255, 177,
      140, 184, 7,   188, 145, 181, 229, 1,   178, 190, 158, 163, 8,   147, 179,
      151, 132, 8,   150, 133, 222, 129, 11,  193, 218, 187, 242, 14,  181, 177,
      154, 155, 9,   150, 145, 194, 135, 8,   186, 222, 142, 242, 10,  140, 195,
      254, 237, 11,  141, 189, 143, 198, 14,  229, 146, 237, 203, 8,   251, 162,
      179, 211, 3,   222, 237, 175, 145, 13,  221, 178, 163, 162, 3,   211, 192,
      165, 189, 14,  230, 228, 168, 250, 4,   141, 140, 247, 178, 7,   143, 164,
      170, 152, 2,   131, 166, 136, 26,  171, 143, 232, 134, 12,  158, 239, 246,
      204, 11,  133, 128, 213, 223, 14,  255, 213, 190, 250, 15,  143, 162, 252,
      157, 4,   204, 181, 135, 245, 7,   206, 241, 254, 136, 4,   184, 182, 211,
      190, 15,  172, 156, 202, 135, 10,  249, 180, 139, 131, 4,   202, 128, 204,
      221, 9,   131, 247, 166, 249, 8,   141, 236, 241, 185, 3,   128, 229, 150,
      186, 2,   237, 189, 141, 218, 9,   193, 240, 241, 156, 3,   210, 142, 198,
      202, 10,  227, 241, 194, 234, 7,   145, 180, 228, 254, 6,   171, 249, 185,
      188, 11,  215, 135, 224, 219, 4,   133, 132, 178, 165, 7,   205, 180, 133,
      209, 11,  198, 253, 246, 145, 12,  190, 194, 153, 146, 8,   139, 220, 235,
      249, 1,   170, 203, 205, 159, 6,   136, 130, 154, 166, 14,  250, 189, 153,
      191, 7,   178, 163, 191, 158, 12,  251, 138, 135, 245, 10,  175, 249, 219,
      164, 14,  136, 185, 220, 188, 7,   170, 135, 221, 146, 7,   209, 224, 204,
      171, 11,  216, 144, 236, 172, 1,   133, 205, 202, 170, 6,   215, 250, 133,
      181, 3,   181, 133, 142, 158, 5,   166, 192, 134, 238, 13,  246, 243, 233,
      218, 12,  163, 202, 238, 241, 14,  241, 214, 224, 215, 2,   212, 192, 237,
      243, 10,  163, 165, 163, 206, 6,   159, 161, 227, 152, 14,  209, 234, 225,
      249, 13,  167, 206, 188, 161, 3,   143, 209, 188, 214, 11,  184, 224, 210,
      200, 10,  185, 171, 199, 183, 3,   177, 229, 245, 86,  255, 183, 178, 142,
      9,   232, 209, 135, 151, 8,   191, 153, 174, 175, 7,   190, 245, 224, 174,
      9,   243, 165, 145, 169, 1,   145, 161, 221, 249, 13,  195, 221, 244, 240,
      5,   157, 156, 217, 237, 15,  143, 201, 155, 207, 5,   169, 136, 192, 238,
      12,  135, 223, 244, 200, 2,   137, 228, 167, 187, 1,   134, 212, 158, 155,
      15,  186, 224, 212, 214, 7,   193, 141, 216, 241, 2,   246, 159, 138, 117,
      216, 230, 215, 29,  204, 178, 147, 255, 8,   195, 140, 136, 164, 11,  234,
      204, 155, 222, 10,  193, 156, 138, 187, 8,   161, 161, 184, 212, 1,   128,
      141, 162, 133, 13,  180, 211, 132, 210, 9,   239, 203, 201, 177, 5,   236,
      191, 140, 207, 13,  173, 205, 192, 186, 7,   179, 214, 222, 136, 8,   189,
      142, 204, 152, 5,   221, 176, 135, 241, 1,   223, 146, 195, 166, 11,  146,
      133, 226, 137, 6,   150, 243, 247, 1,   153, 246, 184, 42,  234, 194, 229,
      98,  237, 144, 253, 133, 11,  196, 131, 158, 244, 6,   218, 149, 253, 221,
      7,   219, 180, 234, 156, 10,  179, 255, 197, 218, 13,  150, 137, 240, 204,
      9,   240, 185, 181, 203, 2,   160, 194, 146, 246, 5,   131, 168, 191, 138,
      4,   158, 245, 240, 150, 15,  157, 202, 136, 14,  135, 154, 226, 240, 5,
      153, 168, 212, 222, 8,   128, 218, 198, 244, 133, 13,  183, 245, 153, 118,
      139, 141, 238, 141, 1,   235, 193, 197, 5,   169, 141, 210, 62,  231, 186,
      238, 219, 6,   141, 243, 204, 242, 12,  172, 165, 150, 187, 13,  163, 254,
      250, 230, 12,  203, 166, 166, 223, 3,   177, 155, 168, 182, 4,   213, 130,
      148, 221, 3,   150, 178, 146, 235, 6,   149, 226, 237, 225, 2,   177, 149,
      218, 10,  205, 241, 161, 21,  186, 239, 197, 189, 15,  132, 249, 249, 171,
      5,   130, 223, 220, 167, 5,   171, 235, 129, 84,  207, 145, 246, 231, 2,
      183, 176, 230, 148, 11,  180, 142, 254, 128, 1,   171, 251, 177, 177, 1,
      188, 190, 157, 222, 11,  140, 195, 192, 141, 10,  200, 139, 160, 247, 9,
      139, 247, 194, 144, 1,   160, 160, 234, 208, 11,  174, 210, 150, 196, 15,
      209, 201, 176, 208, 14,  199, 183, 218, 132, 8,   175, 143, 188, 168, 7,
      172, 234, 158, 248, 11,  192, 223, 160, 152, 7,   178, 134, 130, 235, 3,
      243, 134, 181, 181, 4,   225, 135, 251, 236, 7,   203, 166, 149, 169, 10,
      181, 213, 156, 193, 12,  239, 138, 235, 252, 2,   183, 243, 201, 133, 10,
      137, 186, 227, 237, 13,  255, 188, 221, 148, 14,  188, 156, 198, 143, 15,
      223, 224, 252, 208, 9,   160, 241, 190, 221, 13,  195, 241, 163, 241, 9,
      199, 253, 138, 163, 12,  173, 251, 143, 133, 12,  167, 246, 153, 247, 14,
      237, 223, 140, 174, 14,  219, 229, 138, 242, 2,   200, 163, 210, 86,  197,
      251, 199, 241, 9,   243, 211, 209, 132, 3,   178, 176, 152, 224, 13,  195,
      131, 248, 159, 5,   194, 255, 160, 171, 14,  145, 243, 143, 173, 3,   222,
      168, 246, 134, 2,   178, 145, 204, 240, 1,   176, 240, 236, 165, 14,  254,
      145, 162, 165, 8,   243, 173, 131, 238, 3,   247, 192, 235, 163, 4,   244,
      239, 180, 203, 15,  214, 167, 152, 233, 13,  176, 158, 206, 235, 9,   252,
      150, 228, 160, 13,  148, 243, 234, 239, 2,   225, 152, 250, 167, 5,   252,
      143, 229, 254, 4,   184, 202, 161, 157, 14,  233, 190, 185, 195, 9,   159,
      223, 240, 216, 11,  132, 172, 243, 200, 6,   212, 182, 191, 194, 13,  230,
      245, 240, 130, 12,  189, 146, 233, 239, 2,   155, 190, 214, 183, 15,  159,
      222, 148, 155, 13,  195, 158, 248, 112, 224, 219, 145, 234, 12,  145, 169,
      172, 135, 10,  234, 184, 245, 220, 4,   138, 150, 232, 212, 5,   132, 195,
      135, 214, 5,   181, 247, 216, 205, 12,  239, 160, 183, 178, 9,   161, 143,
      210, 206, 11,  248, 209, 207, 94,  166, 178, 165, 97,  133, 162, 246, 212,
      9,   206, 240, 235, 156, 1,   200, 228, 176, 252, 12,  163, 215, 219, 141,
      1,   236, 133, 216, 202, 9,   220, 170, 222, 242, 10,  239, 203, 197, 220,
      11,  148, 218, 209, 161, 7,   185, 175, 210, 171, 15,  153, 213, 208, 214,
      15,  188, 239, 128, 244, 13,  141, 220, 136, 166, 12,  150, 148, 250, 175,
      13,  130, 145, 226, 216, 1,   216, 204, 215, 193, 9,   191, 211, 181, 229,
      14,  233, 168, 165, 9,   240, 188, 146, 132, 12,  173, 220, 201, 244, 4,
      140, 147, 190, 199, 15,  190, 213, 175, 213, 1,   254, 212, 239, 171, 10,
      200, 161, 168, 144, 10,  161, 188, 230, 163, 6,   192, 198, 213, 167, 3,
      240, 251, 180, 243, 5,   202, 165, 247, 147, 7,   173, 191, 133, 228, 3,
      229, 139, 154, 210, 7,   147, 254, 164, 236, 13,  162, 214, 180, 128, 8,
      202, 176, 252, 143, 13,  154, 179, 169, 149, 3,   169, 156, 168, 229, 1,
      164, 128, 214, 138, 15,  128, 239, 253, 160, 181, 2,   232, 203, 196, 235,
      11,  181, 153, 131, 240, 12,  145, 178, 179, 206, 12,  134, 244, 215, 141,
      10,  138, 228, 171, 244, 7,   246, 160, 221, 177, 14,  176, 231, 208, 135,
      9,   194, 210, 159, 234, 2,   238, 250, 139, 146, 10,  249, 191, 224, 241,
      10,  250, 140, 140, 147, 5,   190, 185, 216, 220, 15,  248, 131, 153, 236,
      9,   140, 219, 183, 252, 14,  254, 184, 223, 216, 14,  253, 211, 235, 254,
      14,  252, 180, 147, 152, 9,   147, 221, 188, 174, 1,   222, 219, 180, 185,
      12,  185, 175, 244, 136, 9,   214, 147, 217, 182, 4,   191, 193, 233, 157,
      2,   238, 191, 156, 211, 14,  229, 221, 129, 224, 2,   230, 212, 248, 128,
      3,   186, 165, 136, 84,  129, 216, 148, 139, 15,  150, 231, 196, 184, 8,
      160, 156, 253, 171, 2,   156, 198, 161, 183, 11,  164, 181, 155, 137, 8,
      133, 196, 192, 213, 6,   140, 174, 143, 152, 12,  142, 202, 143, 192, 9,
      128, 167, 234, 152, 13,  214, 131, 156, 246, 14,  167, 223, 250, 135, 4,
      233, 185, 236, 128, 1,   138, 131, 251, 181, 9,   184, 141, 213, 136, 15,
      171, 224, 222, 192, 12,  244, 168, 162, 144, 1,   212, 183, 184, 200, 9,
      177, 193, 168, 174, 14,  249, 175, 129, 197, 1,   142, 181, 130, 162, 10,
      214, 197, 196, 214, 4,   148, 146, 228, 202, 13,  213, 154, 241, 127, 165,
      166, 144, 164, 4,   205, 251, 139, 128, 13,  244, 188, 143, 236, 12,  190,
      247, 138, 217, 8,   185, 201, 217, 187, 4,   130, 142, 167, 137, 4,   139,
      185, 215, 95,  136, 170, 224, 218, 9,   154, 158, 177, 200, 15,  227, 154,
      189, 136, 15,  224, 233, 220, 179, 3,   227, 203, 160, 188, 7,   236, 228,
      239, 162, 15,  214, 227, 159, 242, 4,   151, 252, 232, 42,  151, 166, 168,
      245, 3,   135, 180, 250, 243, 15,  167, 254, 137, 160, 13,  214, 240, 225,
      152, 8,   190, 229, 204, 136, 13,  150, 219, 186, 10,  163, 249, 225, 249,
      6,   215, 233, 254, 162, 9,   171, 204, 237, 189, 5,   229, 137, 174, 157,
      6,   135, 205, 140, 164, 10,  189, 136, 130, 244, 1,   210, 222, 223, 247,
      1,   189, 128, 142, 203, 12,  232, 241, 180, 195, 12,  237, 228, 243, 183,
      7,   218, 155, 204, 158, 14,  235, 167, 134, 183, 6,   171, 218, 141, 128,
      3,   184, 152, 251, 187, 10,  138, 217, 169, 182, 2,   210, 140, 240, 138,
      7,   150, 156, 232, 128, 9,   209, 231, 181, 174, 14,  243, 210, 173, 34,
      220, 254, 188, 199, 14,  245, 195, 226, 124, 141, 228, 248, 228, 15,  158,
      166, 194, 150, 6,   152, 220, 238, 252, 13,  179, 132, 217, 220, 15,  213,
      168, 186, 245, 4,   241, 243, 200, 226, 10,  216, 178, 141, 137, 13,  134,
      176, 169, 179, 6,   212, 242, 197, 75,  175, 222, 238, 237, 10,  185, 143,
      171, 166, 6,   180, 198, 129, 170, 5,   159, 129, 176, 134, 11,  130, 248,
      213, 183, 12,  204, 162, 169, 238, 8,   139, 139, 145, 227, 15,  232, 239,
      206, 163, 3,   145, 157, 143, 183, 10,  250, 190, 179, 189, 3,   185, 138,
      211, 215, 3,   179, 147, 158, 165, 13,  231, 226, 199, 245, 11,  147, 179,
      178, 190, 1,   208, 217, 154, 195, 14,  226, 194, 229, 142, 8,   198, 175,
      184, 231, 4,   199, 198, 191, 24,  184, 134, 226, 231, 10,  152, 208, 222,
      254, 1,   134, 167, 234, 69,  175, 214, 177, 218, 3,   218, 234, 128, 162,
      3,   160, 177, 187, 166, 3,   201, 210, 191, 159, 13,  240, 152, 160, 250,
      6,   235, 130, 214, 240, 11,  128, 237, 251, 245, 225, 3,   245, 237, 174,
      230, 9,   252, 148, 229, 201, 7,   152, 148, 165, 153, 7,   223, 238, 242,
      16,  156, 212, 237, 228, 7,   139, 153, 178, 37,  219, 217, 217, 172, 15,
      178, 168, 128, 199, 9,   236, 189, 144, 226, 12,  214, 248, 134, 230, 13,
      163, 252, 247, 55,  239, 252, 149, 196, 3,   230, 159, 214, 139, 6,   132,
      200, 241, 154, 2,   129, 231, 153, 173, 12,  235, 131, 255, 157, 2,   246,
      190, 145, 55,  205, 201, 240, 141, 9,   188, 202, 199, 189, 6,   196, 235,
      245, 205, 11,  249, 253, 241, 223, 6,   187, 250, 137, 241, 9,   133, 135,
      168, 146, 8,   132, 248, 219, 156, 8,   132, 241, 185, 4,   198, 209, 147,
      129, 11,  229, 192, 218, 178, 4,   199, 210, 138, 166, 13,  244, 148, 172,
      141, 2,   194, 215, 171, 220, 1,   192, 248, 230, 128, 2,   238, 167, 209,
      222, 11,  240, 200, 227, 150, 11,  182, 217, 170, 158, 14,  223, 223, 254,
      201, 10,  140, 164, 245, 175, 2,   178, 140, 153, 102, 139, 145, 181, 242,
      8,   188, 154, 214, 154, 15,  149, 187, 204, 192, 2,   223, 153, 219, 51,
      245, 236, 130, 133, 5,   197, 138, 169, 80,  243, 162, 164, 167, 1,   206,
      232, 180, 137, 12,  180, 191, 164, 226, 8,   162, 180, 231, 222, 13,  184,
      143, 156, 74,  134, 230, 248, 219, 10,  203, 156, 149, 205, 1,   219, 205,
      173, 167, 10,  174, 146, 180, 141, 7,   214, 231, 229, 231, 10,  181, 246,
      174, 180, 15,  236, 175, 222, 241, 7,   191, 150, 253, 209, 8,   233, 139,
      167, 149, 13,  142, 249, 150, 223, 10,  220, 151, 135, 222, 5,   138, 228,
      133, 131, 4,   232, 183, 160, 245, 3,   157, 219, 209, 200, 5,   159, 242,
      142, 148, 13,  241, 207, 248, 177, 11,  179, 226, 169, 150, 13,  169, 201,
      212, 218, 8,   172, 214, 220, 31,  155, 173, 251, 231, 12,  221, 150, 137,
      174, 15,  146, 137, 251, 255, 14,  245, 216, 203, 138, 1,   163, 170, 194,
      133, 12,  205, 157, 188, 131, 12,  184, 220, 161, 97,  162, 240, 190, 243,
      2,   213, 134, 147, 251, 3,   178, 160, 193, 188, 14,  214, 153, 226, 140,
      12,  191, 208, 235, 174, 13,  138, 188, 204, 236, 11,  214, 135, 129, 235,
      10,  198, 242, 226, 128, 11,  154, 219, 163, 144, 7,   236, 134, 217, 197,
      2,   181, 248, 144, 157, 8,   150, 174, 195, 224, 12,  156, 247, 234, 192,
      7,   156, 206, 174, 246, 2,   181, 214, 138, 155, 1,   246, 242, 141, 152,
      9,   207, 157, 139, 243, 1,   153, 135, 158, 249, 6,   162, 129, 144, 170,
      13,  227, 162, 245, 246, 1,   130, 237, 192, 208, 13,  187, 165, 153, 215,
      8,   178, 141, 203, 163, 15,  172, 179, 180, 172, 10,  206, 200, 237, 194,
      12,  129, 235, 165, 143, 7,   129, 230, 217, 244, 8,   223, 249, 152, 233,
      2,   160, 224, 204, 187, 10,  167, 211, 138, 247, 7,   207, 204, 131, 200,
      1,   207, 240, 161, 219, 9,   219, 213, 129, 183, 11,  186, 163, 243, 198,
      13,  217, 197, 175, 218, 8,   195, 228, 209, 137, 1,   149, 253, 193, 190,
      8,   216, 231, 225, 190, 15,  244, 168, 191, 152, 6,   180, 210, 162, 198,
      9,   172, 159, 195, 158, 9,   173, 151, 226, 34,  143, 231, 162, 212, 6,
      250, 171, 192, 187, 11,  229, 212, 155, 156, 9,   234, 159, 165, 254, 8,
      180, 154, 227, 197, 3,   175, 158, 214, 235, 8,   164, 157, 160, 130, 4,
      158, 223, 243, 254, 10,  178, 236, 213, 212, 12,  194, 173, 185, 159, 6,
      184, 214, 195, 172, 5,   128, 161, 203, 183, 194, 10,  207, 218, 209, 222,
      12,  136, 166, 226, 224, 3,   148, 153, 145, 214, 4,   164, 178, 253, 243,
      4,   173, 162, 237, 129, 4,   236, 134, 193, 169, 14,  140, 234, 164, 190,
      7,   211, 148, 252, 223, 8,   213, 149, 180, 170, 12,  194, 182, 191, 205,
      15,  206, 233, 190, 211, 2,   241, 136, 223, 152, 12,  184, 185, 231, 176,
      10,  201, 166, 182, 211, 4,   209, 201, 205, 235, 1,   141, 184, 205, 173,
      15,  244, 222, 218, 113, 175, 190, 179, 140, 4,   234, 232, 231, 183, 8,
      174, 167, 140, 130, 9,   169, 157, 136, 196, 14,  187, 244, 242, 135, 7,
      248, 183, 178, 253, 10,  135, 216, 152, 153, 15,  226, 223, 172, 161, 11,
      236, 183, 231, 216, 3,   183, 169, 209, 137, 13,  130, 219, 233, 167, 4,
      168, 132, 197, 161, 7,   164, 146, 152, 207, 4,   239, 229, 147, 130, 2,
      172, 156, 244, 148, 6,   171, 253, 185, 213, 4,   184, 181, 241, 207, 1,
      144, 250, 219, 222, 1,   213, 189, 209, 177, 10,  207, 252, 251, 239, 9,
      181, 132, 203, 147, 6,   159, 135, 181, 18,  215, 252, 202, 234, 7,   207,
      215, 210, 222, 12,  195, 211, 185, 171, 14,  178, 132, 165, 140, 9,   139,
      160, 171, 250, 1,   248, 176, 203, 170, 14,  148, 184, 131, 141, 4,   158,
      226, 204, 197, 3,   215, 157, 148, 219, 15,  228, 206, 156, 132, 3,   234,
      206, 202, 231, 8,   232, 177, 135, 215, 10,  173, 253, 176, 172, 5,   144,
      188, 170, 229, 14,  200, 165, 144, 50,  198, 153, 206, 184, 3,   150, 128,
      128, 141, 14,  155, 221, 221, 199, 12,  229, 199, 160, 156, 3,   176, 172,
      200, 97,  222, 255, 134, 158, 9,   233, 155, 199, 193, 14,  146, 216, 186,
      250, 13,  156, 152, 194, 212, 8,   254, 190, 240, 232, 2,   178, 210, 194,
      160, 3,   142, 216, 141, 184, 10,  173, 210, 214, 187, 2,   161, 211, 201,
      143, 5,   213, 149, 210, 222, 15,  134, 165, 184, 171, 9,   211, 175, 153,
      241, 9,   227, 201, 184, 213, 1,   173, 225, 213, 176, 13,  143, 228, 200,
      151, 12,  224, 224, 224, 186, 8,   188, 153, 234, 254, 7,   137, 188, 238,
      186, 8,   166, 236, 135, 180, 13,  202, 174, 133, 194, 13,  179, 243, 158,
      193, 13,  210, 173, 128, 149, 2,   208, 216, 158, 168, 13,  205, 251, 152,
      230, 3,   245, 245, 254, 163, 9,   211, 243, 234, 164, 9,   173, 221, 221,
      215, 4,   146, 220, 209, 198, 1,   235, 237, 170, 130, 7,   181, 227, 149,
      141, 2,   170, 245, 149, 217, 5,   153, 179, 215, 195, 14,  249, 206, 140,
      148, 1,   247, 200, 219, 152, 15,  165, 228, 197, 152, 11,  234, 192, 242,
      244, 6,   217, 229, 173, 147, 3,   216, 209, 206, 189, 7,   165, 171, 221,
      214, 2,   151, 250, 211, 138, 2,   144, 169, 182, 176, 13,  179, 254, 191,
      225, 3,   244, 147, 218, 212, 3,   129, 187, 183, 253, 10,  218, 149, 188,
      168, 10,  223, 241, 149, 129, 8,   209, 128, 150, 126, 153, 139, 195, 131,
      6,   201, 208, 246, 221, 1,   194, 165, 175, 173, 5,   197, 133, 207, 196,
      2,   192, 211, 129, 210, 7,   211, 147, 163, 220, 9,   173, 191, 188, 152,
      1,   169, 242, 205, 20,  167, 133, 213, 211, 2,   213, 226, 129, 166, 12,
      186, 202, 155, 203, 5,   180, 251, 220, 174, 12,  145, 228, 247, 146, 12,
      196, 151, 247, 184, 10,  217, 233, 238, 147, 6,   149, 174, 181, 128, 13,
      128, 246, 173, 207, 15,  200, 162, 139, 103, 237, 199, 220, 252, 7,   208,
      201, 133, 231, 3,   140, 148, 223, 137, 5,   128, 242, 251, 140, 228, 11,
      214, 205, 158, 228, 2,   147, 190, 212, 138, 4,   228, 228, 253, 154, 9,
      146, 191, 248, 187, 8,   168, 200, 246, 160, 4,   224, 168, 147, 211, 11,
      153, 197, 133, 229, 5,   176, 131, 167, 203, 6,   213, 183, 189, 178, 10,
      185, 222, 229, 183, 5,   171, 185, 208, 162, 15,  203, 130, 137, 201, 6,
      236, 152, 138, 176, 1,   221, 200, 169, 183, 11,  237, 230, 219, 108, 152,
      247, 239, 145, 14,  242, 220, 245, 148, 6,   183, 147, 218, 144, 11,  236,
      190, 230, 197, 1,   253, 147, 205, 165, 10,  181, 130, 138, 249, 10,  193,
      135, 148, 142, 10,  232, 132, 254, 163, 4,   244, 153, 241, 197, 13,  251,
      150, 230, 242, 10,  211, 255, 182, 243, 3,   247, 137, 150, 236, 5,   137,
      168, 208, 161, 10,  192, 178, 137, 210, 13,  192, 158, 177, 203, 7,   237,
      221, 208, 153, 4,   180, 129, 195, 139, 4,   195, 220, 254, 129, 8,   235,
      249, 252, 142, 2,   171, 195, 208, 162, 12,  205, 185, 192, 166, 9,   208,
      205, 169, 160, 10,  156, 148, 150, 185, 2,   246, 165, 207, 129, 12,  145,
      207, 129, 130, 15,  253, 209, 184, 133, 11,  247, 226, 200, 185, 9,   193,
      147, 150, 128, 8,   251, 208, 155, 45,  251, 142, 248, 144, 15,  174, 199,
      157, 236, 12,  206, 215, 156, 131, 14,  224, 242, 193, 145, 9,   194, 231,
      136, 243, 7,   135, 188, 221, 220, 10,  252, 138, 172, 180, 15,  222, 245,
      235, 161, 2,   147, 195, 191, 195, 7,   191, 205, 163, 247, 3,   237, 172,
      239, 187, 6,   137, 141, 231, 233, 10,  246, 253, 140, 184, 5,   191, 252,
      199, 190, 13,  235, 212, 206, 220, 8,   163, 219, 233, 232, 13,  166, 129,
      242, 168, 12,  131, 217, 184, 209, 7,   138, 139, 223, 216, 8,   186, 152,
      149, 207, 6,   229, 191, 144, 149, 8,   223, 167, 204, 251, 1,   181, 240,
      166, 200, 9,   194, 230, 150, 122, 210, 176, 221, 179, 5,   137, 169, 225,
      196, 2,   190, 138, 243, 173, 10,  155, 224, 148, 154, 15,  180, 176, 218,
      153, 2,   194, 220, 179, 239, 3,   209, 243, 151, 171, 1,   135, 192, 192,
      129, 3,   154, 145, 158, 166, 8,   174, 159, 201, 207, 1,   134, 247, 247,
      152, 5,   169, 139, 159, 171, 3,   173, 170, 159, 244, 15,  201, 205, 215,
      223, 9,   227, 214, 226, 134, 14,  237, 245, 216, 153, 1,   207, 208, 244,
      63,  136, 146, 237, 215, 2,   131, 173, 129, 187, 4,   150, 204, 222, 185,
      6,   243, 177, 246, 252, 5,   246, 173, 234, 215, 14,  207, 252, 211, 199,
      3,   177, 211, 230, 228, 5,   208, 143, 209, 191, 13,  173, 192, 232, 246,
      12,  132, 255, 207, 139, 14,  171, 129, 141, 173, 7,   255, 222, 227, 255,
      12,  155, 193, 184, 244, 14,  171, 144, 214, 163, 1,   241, 232, 221, 228,
      15,  188, 160, 210, 226, 13,  189, 190, 189, 5,   204, 252, 250, 234, 10,
      228, 161, 153, 190, 9,   210, 208, 187, 214, 7,   198, 154, 214, 242, 9,
      197, 163, 254, 27,  220, 251, 130, 172, 2,   193, 147, 157, 255, 14,  242,
      131, 138, 180, 14,  200, 239, 175, 239, 5,   181, 157, 238, 152, 1,   203,
      211, 156, 220, 10,  210, 166, 223, 241, 2,   214, 243, 250, 244, 10,  238,
      200, 226, 216, 9,   168, 140, 235, 228, 14,  149, 176, 161, 188, 9,   180,
      224, 247, 138, 11,  168, 159, 157, 226, 7,   216, 226, 212, 131, 5,   158,
      162, 174, 190, 2,   147, 131, 155, 194, 4,   227, 156, 248, 169, 14,  210,
      216, 130, 142, 14,  233, 234, 248, 230, 13,  146, 190, 216, 248, 9,   128,
      173, 190, 149, 182, 11,  254, 210, 132, 152, 8,   211, 239, 231, 248, 9,
      132, 255, 247, 168, 7,   149, 224, 145, 136, 14,  162, 220, 148, 134, 6,
      204, 244, 192, 159, 8,   178, 160, 245, 237, 15,  193, 167, 249, 251, 5,
      238, 159, 153, 199, 9,   228, 225, 136, 225, 9,   147, 221, 134, 220, 7,
      249, 129, 250, 131, 5,   255, 249, 227, 129, 15,  183, 246, 177, 190, 10,
      217, 182, 196, 128, 6,   136, 242, 159, 173, 1,   244, 128, 137, 210, 10,
      154, 223, 230, 173, 7,   193, 171, 203, 220, 9,   193, 222, 146, 129, 2,
      159, 229, 247, 153, 1,   205, 139, 189, 204, 13,  181, 152, 211, 186, 3,
      252, 181, 234, 182, 4,   230, 212, 233, 169, 13,  134, 211, 157, 165, 1,
      218, 165, 218, 239, 4,   148, 140, 245, 130, 11,  197, 152, 165, 199, 2,
      235, 219, 158, 232, 9,   187, 231, 171, 149, 12,  134, 191, 248, 157, 3,
      219, 140, 128, 208, 1,   181, 140, 225, 226, 15,  234, 239, 208, 170, 10,
      166, 152, 192, 138, 15,  237, 204, 242, 197, 12,  230, 224, 210, 68,  128,
      170, 249, 251, 10,  193, 202, 171, 142, 7,   235, 192, 224, 175, 14,  147,
      243, 214, 94,  165, 202, 243, 157, 6,   192, 178, 204, 211, 8,   242, 240,
      207, 231, 4,   251, 234, 238, 218, 1,   207, 227, 224, 149, 4,   155, 215,
      210, 203, 2,   164, 248, 235, 166, 6,   226, 234, 165, 222, 13,  228, 197,
      249, 231, 14,  169, 172, 201, 163, 14,  149, 206, 208, 159, 15,  178, 216,
      205, 227, 15,  210, 228, 223, 220, 5,   161, 214, 153, 136, 11,  181, 178,
      246, 212, 7,   128, 131, 238, 218, 13,  138, 156, 141, 139, 15,  134, 187,
      137, 234, 4,   152, 215, 181, 142, 6,   160, 185, 166, 193, 13,  213, 145,
      204, 240, 13,  190, 164, 216, 231, 13,  251, 208, 176, 231, 4,   243, 160,
      187, 150, 5,   235, 251, 246, 205, 3,   142, 232, 229, 222, 5,   227, 251,
      238, 161, 12,  224, 198, 250, 176, 3,   187, 162, 200, 223, 5,   199, 133,
      234, 181, 3,   167, 160, 247, 232, 4,   174, 198, 216, 180, 15,  144, 251,
      131, 187, 10,  161, 171, 169, 190, 9,   223, 175, 171, 171, 4,   141, 165,
      211, 128, 5,   139, 239, 131, 173, 3,   211, 163, 253, 45,  212, 199, 216,
      226, 11,  137, 216, 228, 198, 3,   216, 209, 199, 233, 3,   249, 144, 225,
      146, 1,   216, 184, 225, 218, 9,   197, 219, 219, 247, 12,  214, 227, 243,
      240, 14,  221, 155, 244, 141, 4,   239, 249, 179, 130, 4,   161, 187, 191,
      135, 3,   245, 241, 237, 241, 12,  194, 211, 209, 238, 5,   252, 210, 135,
      149, 1,   134, 241, 220, 170, 12,  175, 208, 242, 229, 9,   181, 144, 172,
      202, 7,   170, 195, 174, 180, 5,   198, 153, 178, 158, 6,   146, 142, 204,
      119, 137, 185, 250, 204, 10,  208, 190, 240, 166, 1,   138, 183, 212, 226,
      3,   241, 240, 245, 140, 15,  250, 184, 161, 117, 198, 194, 173, 133, 15,
      135, 247, 179, 180, 11,  158, 233, 195, 162, 2,   209, 143, 142, 203, 13,
      156, 215, 224, 192, 5,   228, 223, 167, 163, 6,   253, 160, 223, 182, 5,
      178, 178, 223, 147, 5,   150, 180, 221, 189, 10,  168, 197, 173, 169, 6,
      166, 146, 252, 254, 15,  154, 211, 198, 238, 6,   182, 166, 227, 223, 3,
      152, 209, 173, 192, 3,   147, 255, 130, 153, 9,   152, 159, 128, 195, 7,
      204, 199, 174, 227, 8,   149, 133, 142, 33,  236, 185, 160, 136, 14,  154,
      137, 143, 236, 7,   246, 149, 237, 166, 3,   150, 184, 224, 232, 3,   204,
      220, 171, 245, 15,  128, 131, 146, 236, 219, 10,  168, 253, 226, 198, 3,
      196, 185, 159, 245, 14,  246, 239, 172, 207, 7,   172, 188, 238, 233, 13,
      193, 158, 247, 192, 10,  178, 146, 230, 233, 8,   143, 221, 252, 145, 5,
      169, 173, 160, 149, 7,   141, 199, 235, 35,  225, 224, 227, 213, 7,   233,
      249, 164, 132, 11,  255, 158, 248, 254, 2,   248, 200, 154, 176, 3,   168,
      248, 134, 165, 8,   145, 177, 231, 188, 10,  189, 223, 182, 129, 7,   246,
      146, 219, 62,  185, 190, 133, 217, 3,   228, 177, 227, 170, 1,   230, 175,
      223, 120, 150, 130, 206, 166, 5,   223, 216, 157, 168, 1,   225, 151, 175,
      248, 5,   140, 228, 227, 235, 7,   243, 148, 219, 250, 3,   250, 215, 234,
      130, 1,   191, 146, 221, 133, 8,   220, 223, 135, 100, 233, 148, 197, 224,
      11,  164, 203, 178, 134, 9,   170, 133, 159, 133, 8,   162, 189, 239, 68,
      144, 186, 204, 211, 6,   167, 218, 219, 144, 2,   208, 155, 181, 237, 2,
      253, 223, 151, 180, 15,  137, 132, 173, 135, 7,   172, 137, 239, 146, 13,
      250, 140, 255, 211, 11,  231, 134, 228, 145, 3,   149, 220, 253, 168, 10,
      236, 163, 149, 221, 10,  247, 151, 236, 190, 6,   166, 210, 238, 52,  192,
      248, 168, 229, 9,   237, 182, 227, 199, 12,  189, 199, 195, 216, 12,  178,
      236, 220, 158, 2,   247, 182, 235, 221, 14,  219, 148, 216, 159, 15,  158,
      234, 200, 167, 2,   184, 132, 251, 232, 2,   138, 227, 158, 204, 14,  225,
      192, 227, 165, 8,   130, 214, 149, 173, 13,  210, 140, 161, 181, 9,   222,
      217, 168, 158, 10,  220, 222, 238, 137, 10,  237, 248, 184, 57,  167, 213,
      169, 132, 5,   236, 173, 141, 25,  131, 201, 181, 180, 4,   133, 182, 179,
      134, 14,  243, 180, 195, 169, 11,  145, 153, 139, 242, 14,  210, 148, 136,
      230, 2,   174, 147, 246, 185, 7,   185, 230, 252, 230, 10,  247, 210, 139,
      242, 13,  187, 227, 199, 158, 14,  186, 209, 178, 166, 8,   148, 174, 212,
      154, 6,   193, 139, 246, 160, 4,   180, 129, 135, 190, 7,   253, 202, 252,
      194, 1,   145, 192, 198, 192, 2,   136, 201, 194, 165, 5,   238, 198, 216,
      222, 8,   148, 132, 194, 231, 2,   179, 212, 226, 152, 13,  216, 203, 190,
      81,  241, 158, 205, 205, 3,   153, 250, 248, 251, 11,  157, 223, 163, 229,
      11,  160, 240, 198, 156, 13,  155, 254, 151, 138, 14,  219, 233, 172, 254,
      4,   186, 194, 189, 227, 4,   169, 243, 181, 201, 14,  161, 158, 146, 201,
      3,   135, 139, 242, 206, 4,   222, 141, 186, 201, 11,  247, 182, 166, 198,
      12,  141, 168, 155, 172, 4,   206, 218, 254, 175, 4,   140, 213, 159, 204,
      7,   214, 128, 160, 215, 9,   253, 242, 237, 147, 8,   162, 233, 151, 181,
      5,   183, 223, 151, 21,  132, 164, 206, 242, 1,   179, 227, 155, 165, 11,
      189, 251, 195, 212, 3,   154, 195, 137, 190, 6,   129, 212, 227, 177, 4,
      185, 141, 235, 183, 7,   233, 220, 229, 174, 4,   215, 138, 248, 25,  161,
      210, 193, 241, 14,  239, 201, 231, 152, 12,  240, 169, 204, 169, 14,  228,
      195, 196, 225, 6,   250, 159, 144, 234, 1,   167, 238, 191, 142, 11,  202,
      222, 151, 207, 9,   205, 219, 185, 142, 3,   230, 224, 187, 235, 5,   194,
      167, 210, 173, 7,   235, 250, 253, 178, 12,  239, 128, 215, 198, 13,  130,
      141, 191, 238, 3,   173, 252, 172, 217, 14,  129, 203, 164, 16,  191, 131,
      153, 141, 8,   133, 200, 131, 240, 15,  173, 165, 172, 11,  182, 247, 244,
      165, 9,   128, 238, 232, 219, 37,  214, 148, 220, 206, 10,  199, 154, 167,
      130, 1,   188, 191, 233, 235, 9,   167, 131, 215, 154, 5,   133, 224, 241,
      202, 1,   237, 213, 192, 223, 4,   160, 202, 178, 132, 10,  248, 217, 142,
      133, 12,  199, 164, 231, 189, 5,   240, 129, 134, 189, 6,   173, 135, 204,
      176, 15,  164, 142, 214, 137, 8,   208, 169, 163, 251, 15,  196, 171, 247,
      187, 14,  230, 177, 251, 130, 13,  200, 234, 146, 173, 4,   252, 218, 210,
      212, 10,  206, 187, 236, 129, 5,   165, 161, 220, 171, 11,  135, 129, 179,
      205, 2,   240, 251, 134, 254, 3,   136, 185, 186, 220, 10,  230, 142, 156,
      211, 1,   215, 243, 241, 179, 12,  141, 140, 140, 166, 5,   136, 183, 213,
      220, 14,  182, 213, 134, 202, 10,  177, 197, 170, 230, 6,   210, 133, 203,
      128, 14,  145, 196, 176, 139, 5,   191, 143, 140, 133, 11,  247, 155, 221,
      233, 10,  131, 192, 238, 143, 3,   194, 196, 146, 129, 9,   245, 183, 142,
      133, 6,   200, 197, 143, 185, 2,   133, 144, 194, 144, 4,   149, 202, 240,
      36,  230, 214, 182, 211, 5,   254, 227, 217, 246, 2,   128, 164, 220, 255,
      5,   132, 138, 149, 153, 6,   200, 139, 167, 97,  203, 137, 179, 195, 2,
      141, 176, 199, 134, 9,   165, 244, 225, 254, 3,   136, 180, 252, 193, 3,
      200, 165, 159, 207, 12,  147, 222, 142, 148, 5,   191, 146, 228, 191, 9,
      213, 255, 236, 152, 13,  132, 240, 164, 174, 2,   204, 152, 214, 3,   251,
      240, 222, 248, 10,  219, 208, 211, 189, 15,  175, 252, 221, 88,  182, 234,
      154, 107, 208, 190, 199, 159, 2,   209, 139, 150, 182, 13,  212, 219, 146,
      154, 15,  221, 178, 221, 188, 11,  148, 200, 197, 17,  129, 218, 170, 253,
      11,  164, 244, 228, 252, 2,   220, 175, 146, 195, 15,  141, 223, 154, 232,
      9,   227, 186, 130, 220, 8,   153, 157, 145, 139, 12,  233, 140, 173, 183,
      12,  223, 255, 155, 139, 13,  162, 238, 129, 242, 11,  252, 162, 211, 191,
      2,   228, 182, 210, 101, 171, 202, 191, 167, 11,  247, 189, 170, 255, 3,
      217, 150, 238, 215, 10,  173, 188, 234, 177, 5,   166, 139, 147, 132, 12,
      230, 216, 153, 200, 3,   182, 202, 167, 210, 12,  222, 169, 137, 180, 7,
      253, 249, 181, 197, 2,   198, 205, 156, 192, 12,  168, 135, 243, 185, 2,
      138, 158, 139, 159, 11,  138, 210, 248, 255, 14,  157, 141, 161, 207, 9,
      218, 206, 244, 191, 4,   222, 169, 188, 238, 5,   133, 211, 152, 218, 14,
      248, 191, 242, 250, 13,  217, 188, 239, 231, 14,  137, 198, 135, 144, 1,
      231, 227, 214, 168, 7,   128, 136, 152, 103, 150, 151, 161, 171, 12,  251,
      222, 212, 229, 4,   154, 193, 182, 62,  251, 246, 205, 142, 3,   132, 140,
      242, 166, 14,  165, 231, 192, 250, 6,   136, 154, 230, 163, 1,   230, 228,
      246, 182, 3,   187, 215, 217, 177, 8,   137, 171, 251, 15,  211, 128, 230,
      244, 15,  160, 146, 188, 255, 4,   204, 242, 150, 194, 1,   128, 184, 177,
      139, 14,  139, 209, 245, 134, 11,  241, 167, 181, 139, 5,   159, 129, 160,
      74,  159, 200, 133, 222, 5,   157, 204, 165, 199, 10,  193, 159, 169, 151,
      11,  205, 219, 226, 134, 9,   197, 252, 179, 128, 14,  230, 250, 244, 215,
      5,   207, 138, 239, 212, 14,  237, 216, 191, 199, 15,  250, 250, 198, 148,
      9,   212, 228, 174, 146, 15,  221, 137, 207, 196, 3,   146, 165, 245, 220,
      13,  157, 249, 149, 228, 5,   185, 219, 188, 185, 8,   212, 150, 240, 218,
      15,  128, 211, 229, 202, 129, 14,  132, 225, 178, 226, 1,   251, 195, 132,
      66,  210, 245, 154, 234, 5,   145, 183, 146, 177, 9,   218, 223, 128, 170,
      13,  238, 227, 168, 197, 11,  189, 225, 206, 179, 6,   221, 169, 239, 193,
      4,   194, 207, 170, 203, 7,   163, 206, 232, 197, 1,   160, 130, 131, 160,
      4,   139, 146, 149, 173, 10,  140, 240, 243, 180, 4,   231, 180, 202, 245,
      9,   146, 250, 195, 157, 1,   233, 199, 188, 210, 15,  253, 222, 137, 142,
      10,  174, 245, 231, 20,  219, 156, 185, 201, 5,   139, 137, 230, 135, 1,
      236, 207, 146, 138, 4,   149, 174, 164, 221, 4,   158, 227, 224, 210, 7,
      206, 150, 186, 244, 9,   156, 183, 159, 142, 13,  176, 152, 163, 193, 8,
      190, 229, 232, 155, 7,   234, 132, 236, 132, 9,   242, 254, 204, 134, 14,
      143, 226, 253, 180, 2,   138, 226, 214, 218, 2,   199, 228, 210, 186, 12,
      147, 179, 230, 254, 5,   249, 135, 247, 147, 10,  148, 253, 186, 214, 12,
      250, 240, 173, 159, 14,  162, 215, 177, 42,  162, 142, 248, 135, 3,   196,
      143, 150, 150, 10,  236, 221, 178, 147, 7,   165, 248, 197, 136, 7,   199,
      152, 158, 228, 13,  229, 215, 242, 194, 7,   145, 249, 246, 181, 13,  134,
      191, 196, 245, 3,   161, 251, 235, 200, 14,  255, 232, 248, 228, 10,  170,
      188, 227, 177, 14,  212, 202, 144, 143, 13,  199, 230, 234, 155, 10,  247,
      239, 142, 167, 6,   197, 129, 192, 235, 2,   207, 229, 194, 237, 12,  228,
      239, 211, 136, 3,   199, 135, 194, 244, 4,   167, 137, 158, 132, 15,  208,
      199, 176, 183, 2,   161, 181, 218, 155, 11,  218, 235, 160, 207, 5,   250,
      181, 244, 252, 9,   197, 130, 193, 168, 1,   153, 235, 181, 253, 2,   203,
      245, 229, 255, 11,  134, 136, 148, 249, 8,   179, 174, 133, 187, 8,   145,
      212, 156, 196, 7,   163, 222, 227, 236, 11,  242, 171, 200, 143, 12,  185,
      225, 231, 211, 15,  135, 230, 213, 153, 6,   254, 187, 227, 167, 2,   147,
      191, 160, 185, 12,  177, 145, 137, 133, 1,   241, 244, 217, 231, 3,   225,
      213, 246, 253, 11,  138, 185, 169, 229, 8,   129, 248, 228, 155, 4,   150,
      208, 194, 129, 13,  149, 233, 140, 159, 7,   149, 223, 199, 33,  153, 214,
      176, 117, 175, 193, 163, 144, 9,   135, 207, 150, 12,  216, 138, 151, 55,
      233, 245, 225, 219, 8,   215, 194, 201, 214, 6,   235, 254, 134, 70,  251,
      142, 174, 209, 12,  215, 218, 132, 174, 4,   209, 177, 189, 144, 3,   247,
      136, 205, 212, 8,   152, 220, 178, 208, 5,   183, 146, 202, 149, 6,   248,
      229, 196, 211, 12,  226, 191, 237, 227, 7,   234, 157, 195, 196, 4,   203,
      147, 213, 156, 1,   245, 161, 241, 97,  186, 245, 223, 246, 8,   170, 241,
      234, 188, 8,   171, 155, 201, 168, 8,   193, 168, 145, 142, 10,  254, 183,
      192, 202, 14,  137, 175, 147, 223, 9,   176, 133, 131, 166, 12,  211, 168,
      155, 225, 4,   197, 193, 255, 204, 8,   154, 208, 144, 165, 1,   134, 190,
      143, 217, 11,  148, 242, 203, 237, 11,  161, 142, 172, 215, 3,   166, 203,
      240, 162, 6,   200, 195, 186, 162, 7,   198, 211, 223, 252, 15,  132, 160,
      226, 204, 15,  158, 187, 167, 222, 6,   174, 214, 139, 220, 9,   130, 243,
      221, 206, 6,   190, 217, 211, 145, 4,   160, 255, 142, 201, 5,   201, 166,
      217, 174, 7,   240, 197, 130, 214, 7,   216, 133, 220, 184, 3,   241, 148,
      192, 185, 6,   213, 181, 240, 210, 2,   137, 194, 206, 172, 5,   221, 189,
      134, 241, 10,  128, 180, 234, 178, 219, 13,  203, 213, 182, 247, 10,  172,
      229, 222, 178, 15,  188, 154, 206, 196, 12,  240, 136, 172, 156, 11,  165,
      151, 164, 200, 7,   189, 152, 225, 146, 7,   214, 167, 205, 147, 4,   216,
      175, 130, 230, 10,  243, 162, 145, 154, 7,   155, 169, 190, 182, 6,   255,
      212, 152, 251, 6,   147, 152, 160, 237, 2,   170, 228, 233, 210, 13,  166,
      255, 247, 207, 14,  238, 175, 242, 171, 9,   174, 241, 193, 193, 4,   245,
      210, 147, 167, 14,  151, 233, 199, 154, 1,   193, 184, 194, 249, 9,   216,
      255, 201, 246, 10,  138, 198, 240, 208, 5,   187, 230, 137, 145, 1,   200,
      237, 144, 115, 131, 149, 167, 201, 15,  249, 130, 240, 202, 5,   141, 220,
      198, 233, 3,   216, 165, 204, 210, 12,  176, 166, 249, 207, 4,   244, 158,
      162, 140, 8,   174, 153, 181, 253, 14,  249, 157, 148, 130, 10,  178, 203,
      201, 162, 4,   161, 215, 176, 137, 3,   164, 232, 198, 200, 15,  141, 189,
      153, 206, 6,   148, 138, 219, 252, 12,  147, 134, 206, 210, 9,   214, 186,
      141, 183, 10,  235, 192, 204, 245, 10,  155, 177, 148, 174, 7,   246, 150,
      200, 167, 15,  134, 228, 212, 210, 7,   128, 198, 173, 133, 10,  173, 148,
      155, 170, 4,   131, 242, 205, 148, 14,  154, 220, 156, 236, 11,  213, 150,
      219, 145, 4,   171, 231, 199, 224, 12,  190, 139, 161, 155, 3,   136, 151,
      199, 129, 9,   182, 161, 156, 237, 1,   218, 151, 248, 132, 13,  201, 207,
      164, 115, 190, 137, 205, 255, 11,  191, 198, 251, 165, 10,  234, 205, 249,
      181, 3,   172, 185, 218, 244, 14,  134, 171, 214, 151, 9,   152, 245, 182,
      215, 10,  204, 161, 209, 196, 14,  180, 134, 204, 240, 4,   242, 196, 170,
      185, 13,  156, 255, 134, 178, 14,  203, 145, 211, 216, 3,   190, 148, 160,
      180, 14,  189, 162, 214, 209, 10,  238, 176, 239, 248, 15,  151, 163, 176,
      168, 5,   152, 247, 207, 238, 14,  181, 238, 168, 251, 8,   181, 189, 202,
      33,  232, 239, 229, 226, 5,   133, 156, 212, 180, 4,   224, 169, 249, 216,
      4,   198, 245, 205, 147, 8,   231, 232, 149, 230, 8,   243, 161, 191, 162,
      3,   194, 189, 237, 227, 15,  223, 185, 161, 232, 4,   153, 233, 249, 155,
      8,   240, 147, 199, 249, 5,   135, 205, 250, 160, 11,  252, 183, 238, 210,
      10,  244, 146, 156, 160, 5,   196, 252, 142, 22,  191, 148, 222, 231, 11,
      182, 201, 163, 219, 13,  199, 238, 233, 179, 4,   180, 199, 255, 249, 4,
      254, 237, 180, 213, 2,   211, 221, 157, 151, 7,   178, 192, 158, 241, 13,
      133, 212, 252, 51,  146, 221, 241, 177, 7,   137, 246, 204, 171, 5,   138,
      209, 144, 231, 2,   153, 213, 230, 179, 4,   171, 244, 213, 172, 3,   191,
      201, 249, 129, 3,   184, 184, 186, 243, 11,  145, 176, 183, 103, 145, 131,
      206, 147, 14,  136, 134, 191, 173, 9,   180, 164, 241, 245, 5,   172, 133,
      212, 167, 8,   198, 162, 158, 244, 4,   232, 175, 222, 231, 12,  146, 246,
      134, 196, 8,   147, 248, 177, 230, 8,   145, 216, 180, 139, 12,  224, 254,
      191, 222, 1,   182, 145, 213, 232, 10,  178, 139, 143, 237, 9,   253, 230,
      172, 181, 13,  225, 218, 252, 132, 6,   141, 175, 159, 197, 14,  185, 222,
      237, 246, 3,   154, 184, 245, 228, 11,  193, 198, 235, 204, 10,  182, 239,
      253, 136, 15,  205, 143, 161, 211, 7,   164, 207, 235, 220, 4,   158, 235,
      183, 187, 9,   203, 201, 147, 139, 3,   169, 181, 153, 201, 11,  222, 206,
      192, 251, 12,  221, 253, 242, 152, 11,  128, 249, 190, 248, 152, 5,   151,
      199, 221, 227, 14,  209, 246, 133, 200, 7,   246, 181, 176, 131, 9,   165,
      219, 139, 171, 4,   254, 130, 187, 208, 8,   144, 221, 189, 192, 10,  163,
      146, 139, 166, 12,  231, 177, 223, 205, 9,   229, 179, 214, 227, 2,   132,
      153, 150, 154, 5,   242, 250, 159, 171, 9,   144, 228, 238, 120, 168, 206,
      130, 107, 145, 144, 235, 248, 1,   254, 218, 166, 129, 4,   237, 129, 235,
      7,   150, 199, 251, 175, 9,   252, 199, 200, 168, 9,   172, 147, 153, 151,
      5,   168, 129, 129, 188, 13,  200, 166, 192, 192, 8,   154, 184, 218, 232,
      2,   155, 202, 193, 156, 12,  182, 241, 250, 153, 13,  180, 141, 206, 141,
      6,   206, 129, 157, 153, 12,  132, 158, 212, 247, 14,  160, 135, 203, 238,
      12,  216, 173, 204, 156, 9,   166, 214, 242, 138, 14,  178, 248, 246, 135,
      1,   244, 219, 210, 155, 3,   208, 155, 189, 180, 3,   156, 189, 171, 174,
      13,  162, 161, 233, 108, 231, 134, 177, 255, 11,  216, 159, 226, 244, 2,
      130, 227, 211, 185, 6,   169, 146, 187, 143, 2,   153, 225, 150, 187, 9,
      210, 153, 211, 181, 13,  147, 216, 152, 173, 3,   246, 236, 142, 33,  222,
      140, 194, 241, 10,  171, 251, 248, 210, 11,  239, 197, 137, 242, 8,   132,
      179, 189, 209, 6,   181, 221, 179, 161, 2,   168, 172, 241, 163, 2,   203,
      241, 250, 226, 12,  184, 188, 237, 210, 8,   228, 163, 153, 234, 5,   222,
      162, 216, 142, 13,  235, 251, 186, 239, 1,   199, 133, 166, 158, 9,   134,
      241, 161, 174, 3,   183, 248, 214, 158, 7,   181, 248, 184, 143, 11,  234,
      152, 151, 169, 9,   198, 134, 159, 251, 7,   144, 176, 211, 121, 199, 255,
      166, 132, 5,   201, 243, 215, 189, 14,  213, 240, 205, 223, 10,  205, 191,
      234, 185, 9,   240, 221, 255, 234, 5,   210, 250, 179, 148, 7,   185, 162,
      155, 243, 3,   140, 197, 165, 222, 12,  150, 143, 215, 241, 13,  138, 138,
      246, 30,  236, 151, 243, 235, 12,  232, 222, 197, 223, 5,   177, 198, 228,
      194, 4,   130, 172, 242, 221, 11,  208, 235, 221, 161, 1,   254, 141, 148,
      144, 14,  168, 251, 185, 179, 9,   247, 144, 244, 178, 12,  209, 235, 151,
      183, 9,   131, 208, 184, 182, 13,  135, 245, 255, 250, 2,   173, 149, 179,
      144, 12,  135, 248, 137, 220, 10,  233, 194, 242, 248, 6,   212, 132, 219,
      149, 12,  204, 211, 128, 213, 2,   137, 232, 221, 213, 9,   253, 167, 158,
      148, 3,   230, 179, 147, 176, 13,  224, 201, 179, 191, 9,   135, 168, 142,
      253, 13,  241, 194, 141, 216, 6,   153, 214, 245, 216, 11,  195, 145, 195,
      142, 15,  242, 152, 180, 191, 11,  229, 219, 238, 220, 11,  248, 241, 183,
      229, 15,  231, 171, 213, 81,  205, 182, 151, 253, 6,   170, 162, 168, 177,
      10,  231, 205, 251, 209, 3,   253, 168, 199, 198, 12,  252, 138, 233, 210,
      8,   234, 156, 212, 168, 11,  250, 136, 144, 228, 7,   168, 238, 236, 143,
      1,   180, 204, 171, 173, 13,  180, 221, 130, 239, 4,   253, 135, 233, 166,
      9,   233, 248, 248, 182, 1,   239, 198, 243, 139, 14,  160, 215, 214, 199,
      9,   229, 211, 167, 193, 9,   247, 135, 221, 142, 9,   205, 178, 155, 150,
      3,   254, 172, 151, 215, 14,  170, 242, 195, 176, 5,   207, 226, 194, 155,
      11,  216, 223, 149, 43,  240, 135, 144, 187, 13,  139, 215, 216, 182, 15,
      135, 209, 192, 226, 6,   251, 144, 191, 169, 8,   226, 207, 136, 188, 2,
      255, 128, 205, 245, 4,   214, 222, 198, 178, 4,   128, 160, 134, 134, 201,
      1,   227, 171, 159, 179, 3,   247, 175, 155, 247, 11,  130, 208, 142, 189,
      1,   209, 251, 137, 239, 14,  196, 246, 217, 190, 2,   216, 236, 193, 250,
      4,   171, 135, 202, 174, 13,  157, 230, 183, 194, 15,  151, 155, 192, 234,
      5,   192, 160, 198, 226, 7,   246, 249, 139, 215, 1,   163, 181, 142, 210,
      4,   138, 246, 219, 179, 4,   143, 187, 253, 153, 10,  190, 131, 161, 171,
      8,   193, 185, 156, 210, 9,   221, 200, 245, 253, 2,   234, 176, 164, 194,
      4,   234, 206, 138, 90,  226, 227, 130, 184, 6,   213, 198, 190, 208, 10,
      234, 213, 248, 154, 8,   242, 195, 155, 149, 9,   248, 145, 209, 218, 3,
      162, 176, 130, 131, 9,   187, 166, 140, 162, 11,  189, 169, 188, 197, 7,
      197, 240, 176, 226, 3,   158, 243, 236, 114, 214, 252, 228, 253, 15,  237,
      210, 163, 153, 11,  253, 202, 188, 196, 3,   198, 237, 141, 147, 3,   145,
      225, 201, 203, 8,   162, 160, 216, 149, 7,   136, 166, 225, 139, 2,   230,
      144, 134, 245, 8,   213, 208, 144, 236, 5,   140, 163, 160, 219, 8,   244,
      181, 176, 132, 13,  141, 168, 184, 252, 11,  238, 218, 178, 216, 9,   254,
      164, 216, 173, 8,   233, 173, 221, 183, 10,  241, 148, 151, 179, 5,   192,
      198, 255, 228, 7,   237, 131, 167, 203, 14,  136, 194, 238, 162, 4,   233,
      138, 144, 191, 13,  184, 167, 194, 138, 5,   243, 150, 162, 137, 1,   216,
      138, 143, 176, 2,   168, 185, 251, 188, 1,   226, 135, 148, 99,  221, 188,
      199, 199, 12,  172, 200, 179, 146, 2,   215, 133, 179, 248, 1,   166, 136,
      133, 29,  179, 161, 179, 46,  149, 179, 140, 236, 9,   206, 194, 254, 173,
      9,   199, 144, 152, 197, 13,  130, 186, 169, 197, 14,  234, 210, 198, 186,
      11,  166, 157, 141, 196, 8,   135, 171, 179, 243, 10,  136, 135, 199, 243,
      1,   239, 233, 248, 191, 6,   162, 208, 228, 225, 7,   218, 186, 182, 162,
      9,   197, 229, 188, 155, 7,   252, 160, 180, 95,  152, 167, 185, 156, 15,
      169, 157, 242, 208, 6,   206, 213, 229, 223, 4,   205, 225, 209, 237, 9,
      208, 223, 194, 178, 6,   185, 206, 145, 140, 11,  162, 153, 203, 219, 11,
      191, 254, 170, 128, 15,  138, 198, 198, 231, 12,  171, 145, 216, 178, 11,
      250, 208, 197, 186, 14,  230, 209, 184, 146, 15,  136, 148, 233, 177, 2,
      224, 176, 162, 142, 12,  171, 216, 173, 150, 5,   190, 236, 246, 207, 8,
      146, 159, 218, 24,  149, 184, 172, 144, 12,  215, 138, 134, 222, 8,   230,
      138, 234, 150, 10,  132, 233, 180, 129, 7,   246, 243, 136, 179, 15,  180,
      245, 254, 253, 8,   217, 162, 135, 169, 14,  175, 223, 178, 21,  248, 184,
      135, 155, 3,   194, 214, 241, 156, 10,  150, 140, 157, 20,  245, 219, 214,
      189, 9,   195, 224, 165, 187, 6,   143, 205, 236, 165, 14,  177, 147, 215,
      253, 3,   149, 236, 255, 166, 10,  183, 205, 220, 209, 12,  135, 254, 156,
      236, 12,  253, 196, 175, 55,  159, 249, 156, 132, 7,   206, 138, 221, 129,
      1,   131, 237, 190, 202, 4,   203, 213, 202, 160, 13,  142, 239, 143, 188,
      13,  140, 181, 178, 132, 5,   196, 160, 202, 171, 13,  165, 231, 144, 248,
      3,   218, 192, 242, 222, 6,   182, 201, 241, 138, 7,   146, 141, 216, 156,
      9,   253, 199, 128, 8,   143, 152, 133, 227, 10,  161, 133, 237, 138, 5,
      155, 167, 242, 192, 11,  131, 131, 221, 252, 10,  173, 222, 208, 175, 4,
      222, 246, 234, 182, 15,  186, 223, 226, 234, 4,   128, 134, 162, 161, 166,
      8,   173, 187, 185, 226, 15,  193, 158, 170, 192, 15,  157, 143, 170, 233,
      7,   236, 143, 129, 250, 7,   170, 139, 148, 165, 15,  227, 193, 248, 149,
      1,   193, 175, 193, 161, 3,   201, 133, 138, 248, 1,   248, 238, 181, 148,
      4,   148, 149, 163, 224, 5,   140, 176, 170, 226, 1,   210, 131, 226, 211,
      11,  177, 220, 204, 252, 6,   172, 166, 215, 221, 7,   207, 206, 190, 142,
      9,   180, 178, 244, 139, 13,  205, 186, 224, 193, 5,   203, 134, 137, 186,
      9,   131, 254, 156, 161, 2,   251, 240, 204, 196, 9,   174, 198, 211, 220,
      6,   203, 229, 158, 140, 7,   141, 224, 133, 196, 2,   185, 203, 211, 149,
      14,  212, 173, 245, 172, 10,  171, 172, 253, 175, 15,  146, 215, 253, 240,
      10,  129, 217, 236, 156, 12,  196, 183, 197, 250, 9,   189, 203, 169, 148,
      11,  221, 247, 223, 173, 14,  218, 190, 182, 170, 9,   188, 230, 139, 223,
      2,   152, 181, 134, 241, 11,  137, 184, 151, 151, 11,  224, 248, 137, 176,
      12,  233, 234, 254, 228, 13,  202, 199, 164, 253, 15,  205, 152, 196, 208,
      8,   245, 131, 154, 210, 13,  173, 230, 205, 208, 6,   138, 165, 240, 198,
      14,  231, 137, 175, 129, 13,  246, 163, 168, 158, 1,   213, 246, 226, 226,
      7,   211, 214, 201, 234, 15,  173, 179, 224, 157, 10,  146, 223, 141, 141,
      14,  249, 209, 212, 241, 7,   152, 138, 146, 237, 1,   178, 175, 134, 132,
      10,  201, 203, 154, 188, 14,  148, 188, 172, 196, 4,   170, 188, 178, 162,
      9,   255, 227, 197, 169, 15,  163, 196, 216, 154, 13,  202, 217, 150, 190,
      1,   156, 213, 189, 194, 11,  192, 206, 130, 164, 3,   197, 163, 251, 148,
      5,   145, 250, 232, 230, 5,   141, 198, 246, 236, 6,   254, 143, 215, 185,
      10,  139, 223, 210, 246, 12,  158, 243, 217, 139, 12,  218, 234, 151, 205,
      5,   239, 161, 218, 141, 11,  189, 145, 169, 172, 10,  218, 224, 248, 247,
      6,   229, 195, 222, 160, 15,  220, 251, 218, 156, 8,   235, 140, 138, 160,
      5,   138, 133, 155, 195, 5,   248, 204, 241, 223, 7,   174, 250, 182, 238,
      12,  190, 151, 165, 58,  132, 155, 154, 219, 10,  160, 136, 241, 163, 8,
      145, 179, 243, 103, 156, 198, 227, 136, 12,  154, 158, 219, 228, 6,   138,
      134, 248, 146, 4,   139, 141, 198, 253, 5,   193, 167, 232, 162, 7,   227,
      182, 238, 134, 13,  153, 232, 167, 238, 13,  179, 178, 133, 162, 1,   144,
      247, 180, 57,  228, 228, 193, 222, 6,   249, 173, 245, 228, 1,   128, 173,
      168, 233, 7,   128, 193, 229, 199, 11,  222, 247, 155, 204, 8,   193, 156,
      248, 222, 5,   161, 217, 242, 184, 6,   221, 249, 182, 235, 11,  136, 232,
      167, 217, 2,   175, 247, 247, 133, 6,   190, 197, 160, 205, 12,  228, 232,
      159, 194, 14,  138, 157, 191, 164, 12,  165, 147, 184, 78,  175, 222, 170,
      114, 215, 206, 218, 235, 9,   172, 178, 244, 228, 9,   207, 184, 158, 134,
      15,  220, 242, 151, 210, 5,   135, 214, 152, 169, 10,  147, 180, 187, 157,
      15,  169, 248, 147, 144, 8,   148, 170, 166, 139, 7,   221, 178, 241, 141,
      12,  129, 229, 166, 231, 10,  167, 164, 187, 169, 3,   210, 166, 162, 164,
      9,   145, 229, 163, 141, 3,   169, 219, 228, 254, 13,  195, 154, 185, 239,
      2,   232, 144, 143, 245, 1,   182, 224, 131, 205, 4,   203, 167, 128, 129,
      2,   157, 147, 212, 142, 2,   157, 143, 162, 249, 10,  223, 183, 231, 195,
      7,   162, 136, 180, 151, 14,  244, 221, 234, 162, 7,   175, 243, 194, 204,
      12,  128, 201, 131, 148, 223, 15,  245, 173, 201, 153, 15,  179, 154, 219,
      169, 10,  146, 160, 147, 195, 12,  235, 208, 250, 180, 3,   180, 137, 229,
      254, 5,   167, 134, 245, 142, 10,  231, 130, 163, 232, 15,  235, 167, 185,
      137, 12,  179, 205, 207, 135, 8,   130, 159, 158, 216, 6,   203, 218, 229,
      194, 2,   244, 246, 217, 133, 3,   242, 158, 230, 208, 2,   221, 195, 182,
      229, 4,   214, 236, 135, 214, 10,  137, 214, 209, 246, 7,   189, 198, 158,
      200, 9,   214, 201, 139, 246, 11,  222, 180, 147, 211, 14,  146, 235, 149,
      248, 11,  173, 234, 137, 145, 14,  135, 210, 187, 187, 3,   155, 174, 179,
      178, 14,  158, 193, 253, 239, 12,  185, 178, 240, 176, 7,   129, 232, 208,
      205, 1,   244, 167, 133, 23,  236, 181, 197, 142, 8,   177, 189, 200, 176,
      12,  151, 218, 206, 138, 3,   153, 238, 153, 179, 7,   252, 224, 230, 173,
      13,  201, 177, 222, 166, 13,  158, 168, 219, 249, 3,   227, 223, 144, 175,
      8,   162, 217, 224, 176, 11,  153, 139, 169, 180, 7,   230, 211, 215, 224,
      14,  181, 189, 210, 219, 5,   170, 217, 206, 227, 3,   246, 166, 191, 147,
      2,   193, 209, 134, 215, 8,   187, 151, 160, 226, 10,  253, 149, 235, 174,
      15,  168, 167, 137, 177, 14,  175, 245, 198, 225, 10,  189, 211, 166, 129,
      4,   155, 158, 199, 212, 15,  246, 158, 210, 172, 12,  208, 222, 167, 183,
      4,   168, 151, 160, 228, 4,   254, 136, 200, 147, 3,   162, 243, 242, 217,
      11,  236, 201, 184, 252, 15,  177, 243, 230, 233, 11,  148, 179, 150, 255,
      6,   174, 237, 246, 167, 3,   209, 233, 193, 130, 12,  200, 240, 131, 186,
      2,   178, 157, 168, 198, 6,   143, 190, 249, 232, 1,   230, 135, 144, 204,
      1,   174, 150, 157, 136, 15,  183, 158, 183, 162, 6,   231, 245, 145, 241,
      14,  233, 184, 247, 249, 9,   234, 135, 237, 186, 2,   159, 177, 236, 132,
      6,   180, 190, 237, 183, 3,   171, 196, 155, 188, 2,   242, 181, 155, 167,
      8,   199, 137, 219, 202, 13,  190, 192, 159, 241, 10,  180, 160, 136, 200,
      5,   225, 221, 234, 202, 13,  139, 193, 211, 222, 12,  229, 247, 247, 254,
      7,   178, 167, 200, 189, 9,   236, 145, 224, 209, 12,  189, 181, 181, 142,
      6,   211, 255, 195, 137, 10,  231, 134, 244, 223, 2,   192, 216, 170, 244,
      3,   128, 132, 231, 150, 12,  210, 164, 150, 104, 232, 225, 252, 210, 4,
      199, 178, 132, 135, 12,  147, 224, 226, 167, 14,  135, 143, 239, 248, 3,
      190, 250, 237, 208, 10,  147, 135, 174, 171, 6,   189, 157, 158, 198, 10,
      152, 177, 153, 236, 6,   191, 223, 190, 248, 14,  186, 138, 156, 164, 2,
      149, 132, 208, 195, 5,   194, 194, 155, 218, 7,   253, 246, 182, 184, 11,
      140, 169, 156, 177, 11,  227, 184, 167, 231, 4,   172, 213, 253, 232, 8,
      240, 193, 129, 233, 10,  223, 212, 219, 155, 2,   142, 193, 243, 221, 2,
      176, 192, 237, 244, 4,   178, 239, 243, 212, 8,   210, 200, 130, 32,  231,
      254, 253, 141, 6,   218, 244, 156, 198, 11,  152, 151, 178, 192, 13,  222,
      212, 142, 204, 7,   218, 137, 247, 160, 8,   143, 221, 142, 144, 14,  129,
      184, 237, 128, 10,  203, 237, 139, 172, 3,   149, 196, 216, 182, 11,  246,
      160, 169, 231, 15,  186, 238, 171, 161, 1,   176, 176, 172, 169, 7,   137,
      191, 244, 226, 13,  184, 161, 159, 26,  128, 167, 143, 134, 3,   242, 254,
      138, 225, 13,  238, 184, 199, 235, 10,  214, 203, 144, 197, 11,  198, 220,
      212, 219, 7,   229, 145, 174, 165, 5,   128, 209, 130, 231, 167, 13,  198,
      196, 179, 211, 7,   182, 132, 201, 165, 10,  235, 153, 140, 252, 3,   217,
      181, 237, 175, 7,   161, 199, 151, 225, 11,  142, 199, 232, 132, 4,   153,
      249, 128, 186, 1,   131, 229, 136, 172, 12,  159, 136, 140, 195, 12,  243,
      153, 183, 233, 5,   145, 158, 229, 145, 2,   193, 243, 236, 167, 14,  133,
      208, 141, 205, 14,  210, 174, 175, 132, 2,   172, 189, 148, 149, 10,  255,
      234, 231, 149, 9,   214, 219, 150, 166, 5,   173, 182, 178, 187, 3,   148,
      239, 172, 131, 4,   245, 159, 177, 254, 15,  154, 239, 200, 245, 15,  182,
      184, 242, 193, 13,  183, 158, 219, 253, 3,   129, 224, 170, 184, 1,   252,
      215, 217, 217, 2,   194, 206, 254, 158, 5,   232, 208, 253, 206, 11,  132,
      164, 179, 184, 14,  186, 220, 196, 189, 10,  185, 135, 128, 185, 2,   163,
      174, 141, 221, 3,   151, 252, 208, 215, 12,  176, 237, 217, 182, 6,   129,
      224, 235, 156, 7,   146, 177, 225, 230, 3,   208, 175, 157, 130, 8,   218,
      247, 170, 228, 14,  250, 251, 140, 220, 14,  212, 188, 171, 195, 1,   212,
      181, 207, 205, 2,   163, 184, 196, 185, 5,   142, 159, 199, 176, 1,   130,
      132, 140, 203, 9,   199, 243, 181, 168, 15,  247, 242, 204, 164, 11,  174,
      146, 198, 218, 3,   230, 163, 238, 136, 12,  130, 178, 158, 249, 10,  149,
      240, 204, 191, 9,   228, 204, 214, 170, 4,   242, 168, 132, 63,  207, 143,
      253, 240, 10,  210, 166, 246, 186, 2,   168, 216, 222, 162, 5,   241, 226,
      208, 203, 10,  216, 150, 185, 141, 12,  173, 130, 130, 204, 5,   167, 140,
      140, 158, 7,   224, 204, 187, 243, 4,   212, 247, 211, 212, 5,   171, 193,
      169, 172, 8,   148, 129, 238, 88,  239, 170, 248, 251, 3,   230, 252, 155,
      197, 3,   148, 170, 220, 211, 8,   172, 146, 230, 175, 6,   235, 243, 165,
      186, 4,   230, 158, 157, 187, 3,   207, 206, 145, 175, 14,  136, 225, 235,
      190, 15,  135, 175, 253, 169, 1,   155, 237, 177, 179, 5,   180, 161, 155,
      160, 15,  138, 133, 179, 167, 12,  221, 170, 176, 202, 6,   171, 131, 178,
      247, 8,   146, 239, 153, 251, 11,  128, 183, 154, 182, 3,   154, 240, 245,
      209, 6,   207, 137, 159, 171, 9,   192, 180, 191, 246, 11,  253, 143, 225,
      81,  222, 137, 185, 155, 8,   174, 188, 154, 243, 4,   170, 238, 166, 178,
      10,  226, 182, 176, 168, 7,   148, 175, 247, 131, 14,  173, 177, 239, 236,
      12,  177, 244, 159, 183, 7,   241, 150, 173, 185, 13,  232, 224, 169, 137,
      5,   207, 168, 131, 230, 1,   203, 162, 193, 202, 3,   131, 173, 240, 55,
      183, 215, 128, 137, 3,   133, 218, 167, 144, 14,  129, 136, 189, 243, 6,
      239, 157, 134, 248, 9,   172, 137, 234, 168, 2,   193, 240, 176, 235, 11,
      242, 148, 152, 217, 6,   253, 184, 164, 173, 11,  228, 215, 198, 128, 8,
      129, 252, 183, 6,   137, 250, 183, 210, 7,   208, 210, 163, 181, 6,   167,
      244, 197, 183, 15,  157, 215, 173, 242, 8,   211, 182, 254, 181, 12,  235,
      158, 194, 212, 9,   218, 154, 242, 147, 11,  220, 199, 237, 141, 12,  155,
      177, 201, 103, 254, 161, 191, 215, 8,   230, 235, 168, 49,  208, 227, 236,
      235, 12,  201, 130, 172, 253, 5,   180, 140, 169, 224, 15,  234, 243, 153,
      151, 12,  193, 190, 224, 143, 9,   129, 245, 133, 204, 8,   182, 209, 250,
      178, 8,   148, 139, 144, 193, 11,  230, 182, 245, 164, 7,   149, 204, 161,
      226, 14,  175, 229, 148, 166, 13,  148, 140, 189, 216, 3};
  dwio::common::SeekableInputStream* const stream =
      new dwio::common::SeekableArrayInputStream(
          buffer, VELOX_ARRAY_SIZE(buffer));
  const long junk[] = {
      -1192035722, 1672896916,  1491444859,  -1244121273, -791680696,
      1681943525,  -571055948,  -1744759283, -998345856,  240559198,
      1110691737,  -1078127818, 1478213963,  -1999074977, -1236487259,
      1081623627,  1461835677,  1591726278,  -1952575303, -1153279155,
      -490105022,  1763048303,  -438594735,  -1943318570, 665131315,
      -992928519,  -293947656,  -27330946,   -1617757142, 1557060559,
      -1979359235, -2141705600, -568297608,  1062268262,  546298983,
      2078961052,  1350125334,  -540110141,  1306099749,  -1200938434,
      -463354631,  329439552,   -1302441847, -432946209,  1420346281,
      -1051221106, -938249481,  -1539784278, -633078252,  -978731267,
      -1561373991, 1629413219,  1092825247,  -261977862,  838447829,
      1919107204,  1005793149,  1642588377,  -1464918718, -1917550168,
      1003195972,  959160789,   -1522112553, 181240876,   -849957699,
      -458276524,  -702660955,  1860227091,  1705852155,  -1998443154,
      -360453561,  1463660586,  -887384402,  -1905027152, -1872509609,
      -437752724,  -1567069256, 1418352668,  -460909277,  -91142489,
      -1223052800, 1097921652,  -989185632,  1256987999,  -177351034,
      -1872472137, -789485410,  -2128291599, -754152008,  -1726480917,
      -344889284,  -196409605,  2041828611,  1030395933,  -387646305,
      122767355,   31127980,    1207069862,  -1514210082, 1440969525,
      -1135691553, -222758993,  1750352704,  1293980890,  -723071736,
      1827770358,  -1000870743, -1082906010, -696877983,  -252767279,
      -1516790960, 815546697,   2030795,     -44506509,   103592117,
      -1482662967, 927187170,   1038067053,  -1372409134, -1839775706,
      1288569419,  347516536,   794972304,   -547875330,  2037259599,
      -14750351,   -789333636,  -1172998669, 1751028141,  -123944284,
      -148751174,  -5812342,    -65684309,   -901631668,  -1730780359,
      1806879062,  -1718574994, -502581670,  -593823449,  -500334763,
      917654667,   -371046539,  -11224409,   -22297703,   2077801437,
      717176386,   712742849,   -88095446,   -377406568,  -1498205212,
      135250842,   -186007254,  1575202718,  1356337350,  1333002980,
      -151543238,  1561151504,  2084754583,  -1963332201, -1078676964,
      -981959640,  1602476694,  964958176,   514867609,   -592880058,
      -1053778417, -1385343398, -1679005019, -399336120,  -1348025564,
      -1859939973, -1900785472, 2029569822,  -1292867632, 1842863184,
      -1326742626, -1647402852, -1615986391, -2004041108, -1927387127,
      -388061550,  90851556,    -1327038179, -407516410,  1845693465,
      -704577762,  1924407265,  -449969353,  275696175,   252281945,
      1918737432,  1112818815,  -518024058,  -574451772,  2092342266,
      1855130091,  1320798104,  1779205566,  385703114,   -712984113,
      669819902,   1909731996,  -1278685109, -1569593296, 881748738,
      1814556074,  1613634931,  -385688735,  -2071646094, -1773311888,
      -118425506,  1721906928,  -1349880393, 634302005,   760022405,
      761327810,   -1692081627, -1260841016, -1558856657, 99218556,
      102018195,   -1297008771, 164461607,   1741035812,  -148600274,
      1286275446,  1462487726,  -1573434104, 974796426,   -2059029469,
      -2104104269, 1866472414,  -1650530055, 1795114251,  227296321,
      1276834604,  -1985393888, -9742901,    1614958392,  -659109655,
      2088223942,  223737183,   1388180799,  1359284324,  -842845969,
      444248480,   792108792,   960424293,   -507555799,  -1025721075,
      -1858379658, 1074173329,  1761578021,  425012429,   -240453397,
      2024456210,  -324280184,  1589154548,  -1728079451, -1692822665,
      1356528899,  1061517573,  1931192379,  1215961560,  379843745,
      1361149623,  -1461456893, 691110717,   2110459487,  1321410812,
      2009528006,  1972104767,  -2012050687, 1233284414,  -182949706,
      1670813423,  -1217301469, 594224363,   -299708512,  1966313463,
      -369112947,  403641651,   88148317,    -2024969729, 1133025739,
      314550032,   1534341518,  1083403602,  -894963971,  1635904390,
      1275196039,  1770867136,  2003009771,  -545216468,  -135106165,
      1264541893,  2022351708,  -1678497814, 151276090,   1283919338,
      -1927614553, -206580733,  1377848647,  627609963,   1823245450,
      -134096555,  -574753171,  -1744928487, 1723985722,  1167154655,
      -599470685,  546628481,   -100331078,  1303120516,  2089166733,
      -2022155954, 456890992,   -1002705650, 2049833270,  656668907,
      -44900108,   -525666700,  -2134854916, -1778466708, 1099709483,
      1753848159,  10966731,    -932986450,  -1244650092, -735949590,
      -836092531,  -1380029252, -255869471,  259782569,   -1689370655,
      1681300596,  -998144311,  1911129837,  -863029750,  -402765462,
      1405052444,  325400133,   950928169,   1208813323,  -1927723497,
      -36025530,   1953996718,  -130830587,  -2119112967, 828918159,
      1875760908,  -2110464282, -660032043,  -1445534969, 1754377388,
      859122691,   79215786,    -1457379224, -845505501,  715141530,
      -1483079760, 1668988417,  1189423270,  -2117214918, 439999476,
      -1399973705, 467038141,   -494559901,  -1783874778, -1599666356,
      -199642314,  1949521512,  1089253553,  645336035,   -25686436,
      1450983836,  267113484,   73222595,    -497431960,  438311597,
      442985552,   -1777857701, 933496376,   -1594540214, -505331447,
      -1315298171, 1016898878,  966042892,   -17718192,   1045280014,
      -39208518,   -2060138094, 1282411033,  1713508214,  1851842091,
      -58654482,   -474136376,  817547251,   296628738,   -1658010049,
      -299884790,  57814971,    -1222513255, 869855902,   1558100706,
      -905854845,  -1326530206, -1092944323, 1103855106,  4668482,
      1477604451,  -590041139,  -1784763556, 282428730,   231044577,
      269278752,   1575627255,  1500279352,  1910855259,  -1419761648,
      318679302,   107160345,   -1193714758, 2041235102,  -336170699,
      -54224496,   -676354875,  -84222627,   -175409338,  1620482599,
      1176801242,  1844243729,  77824988,    1438587267,  -215131942,
      -1383445358, 953582743,   1451014635,  -2068176283, 1058786294,
      -1159701920, -1767170805, 1441979975,  769713646,   540064005,
      525602292,   -747255503,  -1765923984, -1528763385, -1768241306,
      -1168806485, 33265046,    -1719626574, -2061575599, 2013225545,
      -145323579,  -1616398994, -1614251879, 101988124,   389536785,
      -531784107,  1942497305,  1624000107,  -1793946656, 1590267653,
      1454383595,  1477205155,  956593869,   341516726,   -1104289307,
      1711827851,  1007508942,  392549262,   -162616731,  1233239227,
      -254895976,  -932430285,  1789001809,  -258910386,  1829247809,
      -1165175134, 2050581337,  1388743894,  1680716327,  -955562689,
      -1196112257, -378740336,  1404672016,  -1064391892, -209744680,
      -1303657512, -1534080366, 1819175133,  -1168503149, -144324898,
      -1139294027, 2079078892,  830990906,   1281643674,  1239967702,
      -36455895,   -893671880,  1538788093,  -1237546291, 1206167541,
      475817626,   -1186645912, 539232082,   1475246031,  1699396377,
      838282081,   717780380,   -1411838673, -1709848232, 504121732,
      627189322,   658484370,   -538814615,  1922572726,  1004837510,
      -1174373674, -1655080299, 2094525857,  355981927,   -1636557369,
      1393356380,  -624347557,  -247050857,  -2061086215, 119232442,
      -549875608,  1132263989,  1210157527,  -1950418773, -947805470,
      1473662460,  -2039682564, 1511364593,  495775222,   -1754933852,
      578631361,   974692628,   619906194,   -270694776,  827229974,
      -626474838,  217984348,   233537160,   -1394225003, -1325367080,
      -825843995,  -19309008,   -1051287340, -1709856232, -1924609250,
      1220845849,  -262498310,  1923705916,  550530570,   475633807,
      -2108852076, 407081906,   1182356405,  1433463924,  -717627223,
      1985302280,  52562276,    462014051,   1892679691,  -1685829454,
      -432280051,  102304536,   1239474159,  -1947789045, 1873237513,
      1162364430,  378408895,   436753561,   1401009671,  -331011223,
      -687420625,  -2112505195, 1253509443,  -1326656490, -223810162,
      -1795864663, -1635326216, 1135351856,  1072514654,  -1135464197,
      1799420691,  1814080421,  -1813241050, 290458473,   1787024936,
      -509812455,  -1245699451, -1246584042, -628864855,  208287497,
      -941972342,  -282245339,  764591445,   -1950018765, -155292605,
      -2039181884, -1502132499, 927879221,   -422951277,  1004131436,
      -359377619,  -279608972,  1795607112,  -504889242,  491472122,
      -1473703617, 1384613229,  -1074969712, -132300841,  -809001677,
      -232707109,  718661985,   -340386147,  1025520864,  -1304716522,
      -159879127,  -21609621,   -356163924,  -1650473131, 749957789,
      1659608794,  -1630468361, 1401873890,  -826137197,  -1745267595,
      2096479616,  108095652,   -1070305783, 510702184,   681305350,
      1581358841,  373543787,   -548048778,  1236252978,  1136594889,
      571396628,   1563585072,  -777040205,  884269272,   -1395109355,
      -729593757,  -2049576534, -881926310,  184632886,   -1534407215,
      -113998263,  1897790924,  827242297,   -1493910748, 207409078,
      -1381606655, -1469137051, -1357021665, 574603572,   1818109562,
      -1462551998, -523689962,  -784515708,  -1377438213, 1830890656,
      1018570656,  -563746679,  548954202,   -1075828514, -284139126,
      -1646923990, -1248333415, 1376072552,  328385806,   1612310907,
      -2015376329, -1482101887, -1268324540, -1073923297, -47412286,
      -2031027134, 1724101079,  1882428903,  1226325168,  1060182497,
      -1439411972, 2068153022,  303922543,   -1010299082, -527725408,
      -868084535,  -1453122373, 729915259,   -1810431776, -1170855222,
      -1854748370, 1653489747,  -1024923202, 1166795461,  888317469,
      -1095897075, -263817712,  -1283775515, 128113057,   725330985,
      -340535877,  1390305951,  -2040698894, 295390234,   519468833,
      -179502313,  -404230148,  1113834573,  217655255,   697236931,
      -447996629,  -2135157399, -1308291941, -1886148018, -161160567,
      -67015720,   360555652,   -598747970,  865850123,   -802081914,
      1971145595,  -477789992,  -776787161,  1811555304,  -1735200791,
      1891237826,  -986816598,  -1744598976, -2001145934, -171623446,
      -2118892089, 1848264734,  -5746591,    1454333734,  1273178226,
      1030190121,  1328203427,  -29346019,   314597102,   -2012456161,
      1933656313,  787872740,   -160286555,  -1438880998, 387705257,
      1464818923,  1301041719,  1984783124,  -1271147531, 1487861786,
      1042524116,  674928812,   333826191,   -606298314,  -1923024690,
      1893750313,  -1852775093, 1334513545,  -1533194135, 1098945727,
      -1334639594, 982450114,   -1887582219, 811767569,   1106779430,
      2128521241,  -801057249,  1282615287,  1309743218,  -1036048202,
      -675233917,  -2015133312, -1407597980, -805866925,  181664900,
      1428234298,  987551693,   -1305045729, -269637537,  -161413456,
      -1825022695, -464152091,  594365822,   1788687667,  173257923,
      654002541,   1479451402,  -343189027,  -1317263094, -1632991710,
      434048963,   -218104622,  -2116821787, 1386879989,  2024277523,
      -1683903287, 71981107,    1472146048,  -954561185,  -1929121846,
      -99278026,   -836661907,  1161399456,  645528633,   -229497534,
      -559683816,  -347755982,  846036498,   1843706545,  1988047218,
      -1916349205, -2046432139, 2117711385,  768342313,   -1484993937,
      -1028574363, 1840103616,  2024908549,  648097475,   820426188,
      1813302864,  -1862894699, 1853557023,  -645272638,  -694642746,
      -484368118,  770488839,   -1646124786, 453988784,   -771295390,
      -459096420,  -646899732,  2068517271,  1404075720,  -1273309905,
      -582314992,  -671770951,  -449870790,  -48212202,   1579880938,
      -476878341,  513340524,   -153887805,  1303129644,  -1736144611,
      1997437163,  -551454447,  -539393656,  -410513105,  -1730002043,
      787100897,   156300478,   1655413827,  -1314804760, -1017480219,
      725995733,   837174883,   125404041,   -1422872133, 174985128,
      506105285,   -2026814521, 122957373,   2018881699,  -1531346372,
      304642639,   -1823589353, 738989518,   842332146,   -728492095,
      691793049,   1406905611,  848671060,   2146403475,  921228493,
      503081371,   470135884,   -1234198474, 1009780684,  1177932262,
      -34718027,   1887702646,  1052893773,  443393403,   512495115,
      2136307494,  -1438483586, 476864340,   2001989218,  1022729211,
      1855835926,  -1410262945, 1184679065,  -689936200,  -961809237,
      -37581255,   -1029470257, -1480892021, -401541056,  453202492,
      1112595988,  -1405938761, -941021151,  65758395,    -496021405,
      179072114,   126610419,   711573643,   -176404016,  -797304305,
      1052539142,  -531326266,  137188861,   -1079747744, 104921070,
      -1577624885, 1214665426,  1079238997,  72216401,    892964488,
      -285963924,  383166184,   -2067986431, -947233029,  1764614742,
      1564468029,  -421298612,  -1385150219, 1439869174,  -871204348,
      55432339,    1314201120,  -1685876151, -1703440863, 300653337,
      -1977445820, -2046493998, 309926543,   378495260,   1958992069,
      -1113354289, 1792193921,  1263805225,  1373967983,  1352521646,
      -60235319,   -675624276,  26323830,    -591835714,  -1885760899,
      -1519938874, -1998677577, 375457065,   1000260823,  -1450154397,
      -1864463548, -1911093470, 1114002525,  833260426,   -571392737,
      1004593242,  -204444351,  -336121865,  710431300,   1173033399,
      376979722,   -1770804506, 85447404,    -484026297,  -1606360717,
      -1582593999, 1774771216,  -1889730446, -669358702,  641183901,
      -1956035797, -479348625,  -619594436,  1553417071,  -1684327868,
      -583232007,  587191975,   1019475270,  1299447851,  -1094565055,
      726858321,   -22214620,   254396674,   -1515419866, -491290335,
      870396109,   -589067521,  -998073181,  -585938741,  -27198124,
      -1998075025, -1636627064, 1922665080,  907579634,   245499901,
      -1491598228, 1290991525,  -417806055,  783775795,   987384289,
      -1664073398, -1818943544, 518513473,   -1972739863, -17076929,
      -1087578336, -2130735619, -11897175,   1247714779,  39549495,
      1424721195,  -136636068,  1321021406,  -699064532,  -212744195,
      -637015415,  1346785936,  1615976060,  -735897892,  869318776,
      -2064220631, 1083884434,  2142530152,  1941891810,  1747938419,
      584211108,   1430935230,  673025767,   -1522239571, -349593668,
      534830840,   1439125060,  221479859,   -1665023212, -711033607,
      1976217028,  1419826523,  -912609625,  1879662953,  -683020553,
      -1481737184, -1453041404, -419287042,  1209159969,  -810667515,
      328331620,   -554189827,  -38670987,   758568371,   392902911,
      805013760,   831693442,   102032100,   -339108454,  -1214835719,
      -535575827,  471829764,   1693706596,  -692180874,  -1274840224,
      -1770889195, 316972034,   3851814,     -1468783678, -2077914158,
      -93044504,   112417435,   301526952,   -1801634537, 2040682218,
      -1540074671, 18395658,    -1607816833, 399285522,   2083671022,
      -1317230535, -1170230962, -1622288205, -1668653877, -1756594160,
      1595947921,  335177918,   106581426,   -1517810326, -536170364,
      -1434305965, -723341079,  1614963411,  478361139,   1696920219,
      994126447,   -341229183,  1677955939,  329146836,   1508992901,
      2013205637,  -1291068239, 603886509,   786926191,   -1973621955,
      1873694716,  -1987964717, -151056773,  -982178036,  108200448,
      1655973323,  -643471294,  65458253,    -417971646,  1919828738,
      -933763539,  171755140,   460249395,   -1125856734, -16739013,
      -2135736362, 670532752,   203611302,   1890987520,  -1483650118,
      -683059705,  -77856848,   -769700368,  -1416934159, -1500850145,
      -1215059687, -1879473955, 763272883,   -1968038568, -2088236599,
      1229512381,  2032523562,  -474604143,  1842260297,  -776126031,
      -1134008029, 2108556714,  -1880709482, 237393986,   -69243134,
      782458217,   -1259490761, 1788876781,  1549080823,  -859428959,
      -605940335,  1018516449,  -207426450,  570450064,   -1389536390,
      592346118,   -1331252532, 165183113,   -2099745269, -1356937151,
      21822807,    -748103470,  -142393926,  547509238,   -634686347,
      1026300111,  1330070951,  1759768014,  1142187544,  968694111,
      1213038901,  1885970361,  -323991688,  363518085,   -1672108324,
      -804048074,  -1363075581, 1701273418,  1911929917,  44447185,
      410977169,   1365427170,  959862646,   -948485651,  -1849935396,
      -1009669619, -1801379401, 525897667,   -1955430097, -1448024640,
      1931243285,  1760694954,  -1371363748, -846322684,  -381157475,
      -1725454696, 411728882,   -659046884,  -2017706580, 326504936,
      -1505447249, 754195181,   1338936701,  -176693411,  -399948493,
      -1610399078, 1200783875,  -1135651738, -1011062025, -1590458258,
      1626934009,  -2101147741, -832223620,  310144767,   -1670647754,
      -139535449,  -511393081,  -1608439153, 1179987525,  -566009345,
      1746424843,  -972134987,  -35190731,   -123082125,  -1225027672,
      -12768196,   57860780,    -1169964405, -896086188,  -73457590,
      -1695925182, -585143980,  -419933289,  -1162453564, 755390220,
      -827933852,  1698208124,  1044230129,  608724853,   -164275430,
      -102639739,  1198259549,  1137531989,  -1116284630, -1356999201,
      1957170687,  -1307732933, 1650483544,  -638806570,  -1154478179,
      173151245,   1569845123,  1591311498,  -494240657,  841880275,
      975655140,   2144072931,  2093762562,  904195791,   1304524183,
      887864513,   555382367,   747757520,   -988490149,  1029722488,
      462127468,   -865600825,  -355339627,  -717869189,  -1460719471,
      1840667290,  -1467405670, 2066471254,  1682556574,  1506116152,
      -1015317971, -959194655,  557427179,   1449151468,  -966928570,
      -862440014,  -934483264,  -382993930,  1831680277,  1962868691,
      1253985271,  605568087,   -1920103611, -162069068,  -1335381537,
      1466515436,  755896709,   -152123806,  120724324,   -2090132802,
      -749600957,  -513332999,  1697220972,  620702104,   1086605242,
      2010555991,  -1344440189, 573125337,   -412489169,  2089343506,
      -887304007,  1741382282,  -1294582154, 1399959211,  -1465487414,
      -987925582,  2054751675,  1026201859,  1347793280,  -581133591,
      -1900657794, 1589876493,  -555443627,  -1711864278, 431235807,
      1209591236,  248744027,   1750009325,  -120886245,  1610195551,
      -1381986720, 459223925,   2001423958,  1232784067,  1433853260,
      1951017062,  654934426,   1804947769,  1931534286,  -495608934,
      1933837599,  -1427818655, 2140007479,  -713427148,  1995046348,
      -1203051419, -35213147,   774683636,   -592086787,  630139504,
      1094303075,  -1180875316, -438823034,  2117971809,  -646196848,
      -1103051341, 798549240,   -1510953796, 1429065214,  704873658,
      23191330,    -1585169696, 1840542299,  -591215524,  664793562,
      357997439,   -963884906,  1863569433,  -54498563,   991835977,
      -716807557,  376575045,   -591189325,  -449494294,  -404697696,
      1597459996,  -108456969,  -1899610313, 1255661956,  794700058,
      1115324758,  658753699,   1719389172,  1145101705,  -1181105674,
      -1622578697, 233308080,   1451926619,  1322377945,  -1800772031,
      -810522289,  -1951656903, -527284125,  1582214669,  -1422750113,
      2022685659,  -1026827239, 634221522,   1270282959,  -414347878,
      -1553149269, 1740116911,  -1502502767, -697241533,  -1983623628,
      -1015070121, 1211501947,  -582055635,  1158111423,  1409791816,
      -1650549906, -1289481332, -372952307,  698533442,   1253310137,
      126736648,   112219028,   -260924425,  538236607,   -8216695,
      1258254795,  1250497022,  695411926,   1807753300,  1141377444,
      378228237,   -1640510094, 1772051547,  819577690,   1637064807,
      2004518786,  1726570960,  1237945196,  1890473363,  142532121,
      431642362,   457680616,   1793421134,  114108497,   -1609966004,
      390875116,   865761473,   -284648597,  -1270011981, 1801086569,
      -450041354,  34724667,    1461207855,  -1563369174, -1193357688,
      890744002,   -303462235,  306064148,   -1714379878, 1160621852,
      782444786,   1760233647,  -251092726,  -1239728484, 451165251,
      -971693596,  -1492590107, 1251141173,  1068753315,  127560712,
      -675602404,  -1943731429, -1442429995, -1268600807, 783284088,
      960921257,   -523462813,  1709486406,  1864033227,  32424581,
      1723753974,  771274676,   -606900633,  1574849281,  169589480,
      1895990143,  1261911764,  -1663992892, -1265826537, -1801917442,
      -397409604,  -1627809111, -1438727684, -932073653,  1633378602,
      357569766,   -1297857029, -423873023,  1795321075,  1274442352,
      -1876019716, -897691833,  -1569633677, -2028495970, 1542882873,
      -1573770995, 2119629948,  -85633780,   -936570279,  1393887381,
      -488600436,  -1684597311, 1160585918,  1519028021,  1044513341,
      150838164,   1792373530,  653285210,   -1248666111, -191831605,
      -1891529144, 1283118544,  -1276441843, -1223401980, -425946279,
      1970465599,  721976469,   -1505253544, 45266924,    1806828024,
      -2070615494, -908596292,  -1117250622, 331420657,   -660185152,
      589879211,   210813328,   -456387314,  -1601399804, 198300673,
      -1995521769, 334183842,   665336620,   -1793671638, -2082929039,
      -782763724,  1042860064,  225541755,   -622972242,  591101317,
      -1369419464, 1119101151,  -1294175841, -400470575,  606374965,
      94458805,    864049393,   -1426575787, 1101993333,  1230205177,
      497689724,   1211124753,  -1512147358, -1012370015, -505814051,
      120429775,   2145165099,  -1502901431, -474452671,  422689635,
      -1152989257, 962267153,   280766852,   1196475443,  -784471083,
      1169426630,  1749421434,  -1606879751, 1300649655,  1121651007,
      -1400613749, -724755769,  1045426592,  -1958011127, 573427844,
      -1811022517, 682117596,   -143934906,  318890668,   198143572,
      103973361,   -1685647151, 287732246,   -260465004,  30450195,
      -48654426,   -1321307339, 1256181927,  -1817379876, 1951739521,
      1537791157,  1145153363,  -1463184068, 255386052,   -872356472,
      1042060305,  1244057261,  -968333667,  100042814,   2043095500,
      -890128213,  637318503,   -1322924135, 858281960,   -1489122205,
      1572431441,  -2013618080, 1719193989,  -1529545814, 1940436029,
      2032604275,  320677124,   1625574448,  -694531606,  1157552927,
      25905097,    -1627753995, -1172357804, 1366114995,  941005378,
      2066816251,  1205853530,  -1922099373, -22435800,   431025724,
      1372468641,  21209867,    -1272633083, -867481634,  -1918735176,
      -534439129,  -1383070475, -1696306012, -1724096388, -58061119,
      -943955536,  136028839,   -614980418,  -1778996582, 1807875015,
      675695942,   1790527522,  -528619987,  904810541,   950940251,
      1238041417,  -8393215,    -1446028808, -682467665,  -1544440270,
      -1472962754, -586815383,  2070764975,  648828893,   1113860227,
      -2116497111, -2080720801, -1049969615, 1067459574,  2052227797,
      -157225074,  -437791713,  -260129125,  558283708,   772040010,
      237325318,   1564229865,  -935958297,  1037756822,  -1223152552,
      1757318298,  -740036263,  -1268851110, -303275906,  -1279892542,
      902459799,   -952359270,  -339785735,  -1901753053, 1389276010,
      -2063575830, 1460647369,  -1640863297, 1336454626,  -1497707231,
      -1927020015, 1252446125,  368146846,   1594936652,  -1500704261,
      1661025840,  -1850727093, 2144637413,  -1158186535, -1831026939,
      -889829783,  1953368389,  -1746264692, 166004987,   -1043094955,
      -2125018538, -1373375703, 1892792265,  -1058706557, 248660620,
      1346423769,  -1942180581, 608538378,   1244024597,  -2056829184,
      -1772818706, 199415397,   1546106190,  440423328,   -693070051,
      -778903177,  -919523719,  1402659839,  -1735022534, 1622883535,
      752024237,   -1490765944, -1388651615, 931076141,   -2047594739,
      1103847150,  -704725814,  741564741,   1040069436,  1726406295,
      61122015,    1437812418,  1111368208,  -108948681,  1619816846,
      910911373,   556728709,   -802734918,  -976030177,  -1752026546,
      -1860499981, -169913498,  60202440,    904411442,   -240036733,
      1049955136,  1551675456,  1153662447,  -770639649,  -864966225,
      -1589042799, 362084868,   -811531736,  1691619679,  1948514866,
      1648879429,  -82248915,   -119887768,  -1320899500, 1313770646,
      -2019806760, 757267630,   -1385371012, -2044161290, -1090682389,
      951372426,   -1625173167, -1450498369, -446130452,  1245989289,
      -416577865,  -1877776085, -385296034,  257025076,   617642011,
      -269486566,  -283804879,  -1469334479, -1010626032, 1903591953,
      976050042,   -1690852568, -2113044709, -2040081275, -1385916058,
      1681025033,  -458183734,  804037210,   -1357816212, -2122604724,
      -1620519414, -1081733978, 897828801,   -338474662,  408632762,
      353159097,   -643223791,  1432419115,  -1063925125, -1283707295,
      1600221803,  1966239023,  1602403017,  -1896954519, -465007748,
      -1931897742, 1728032847,  -990776477,  -215620097,  24160762,
      1088990582,  -1661538137, -413783692,  -993213325,  1792858174,
      -1785449573, 530278927,   -1123162098, 1527518801,  -994386637,
      1980429555,  -767184731,  507106901,   288876987,   -1165022305,
      -1445201374, -2062378367, 1930504660,  -1444470104, -538236127,
      -2101929870, 1657423803,  594868136,   641992148,   423166527,
      1570659537,  2143752822,  -1587338457, 938659018,   444521303,
      -1613249129, 329284644,   879036249,   -244264840,  214041075,
      2021893527,  -841410460,  -1997684084, -1335815733, 330146293,
      -810388560,  461221786,   -331575574,  1114860921,  -1823171172,
      1460924447,  746653722,   -1823299441, -1709862982, -1072627187,
      1272515033,  1696334966,  -820424031,  -1352171498, -369000884,
      524637728,   1634525440,  109234473,   623876212,   -1617988772,
      -1920751626, -529392580,  1426964127,  -850772426,  -1415825247,
      918760524,   -2005391328, 306414237,   -741998859,  1034121377,
      -1535565247, 1528007238,  -645197362,  1183823190,  1452290168,
      -297497904,  366899271,   659402776,   1162771417,  33575465,
      -819969972,  1550032173,  1812350412,  1019335983,  1108271725,
      -1895946056, -1343073793, -448887654,  -1533743371, 2121607227,
      169180061,   982879256,   -1848545221, 27519068,    409070016,
      1846632377,  1454960183,  1548882667,  1035638563,  -710263923,
      -1786568873, 1026978083,  1381572891,  -532776566,  -989703533,
      -1578299857, 541921735,   -195042893,  -1656822082, -1680966160,
      -781641338,  -287090569,  -1920834785, -1959900163, 273017769,
      1364365142,  -1230830272, 711120619,   -464932247,  540384202,
      -2145789947, 2136546253,  1813925403,  -534472604,  -193288193,
      362493438,   703583137,   1559213108,  1938188546,  1406703389,
      -328204765,  -500280210,  -1702502156, 862665560,   -969766913,
      510405705,   1076079592,  1984257517,  1975623421,  204828458,
      349826410,   -731418130,  185132999,   1286701313,  -2055650532,
      -1514773692, 497599639,   1619904755,  1469303937,  -1274649611,
      581620530,   66095673,    -1460642792, 330221993,   707515924,
      -1421482169, 1624712620,  -750796951,  -971080468,  657945392,
      759856618,   -1120219222, 93175882,    -532613816,  475234099,
      1161529994,  855426198,   -597998838,  464758707,   -1928475560,
      2079160388,  -178236356,  -724974414,  2047043674,  1651925317,
      -883296943,  -1198932182, 1605581769,  459492800,   891206669,
      -1253302888, 1600646432,  -85730303,   1102520943,  657674007,
      1394924437,  981863857,   1883171786,  -1724771415, -997457177,
      -1804969401, 680867892,   -241199656,  -480782502,  -58592066,
      -412095964,  -1896150659, -926392833,  -1333839736, 311247446,
      -1588993057, 898827577,   -1523879487, 1074320882,  -6749953,
      -1025965701, 861172904,   -2071510292, -1193653711, -1667222954,
      -1296582582, 1497253549,  1625141742,  -108604494,  1165486207,
      51714803,    1723701480,  -802521253,  2114265882,  1634942197,
      -1224478625, -1153482049, 1127175259,  1544684234,  978234803,
      -1982083851, -1784846680, 495428362};
  const unsigned long fileLoc[] = {
      0,     0,     0,     0,     0,     3,     3,     3,     3,     6,
      6,     6,     6,     9,     9,     9,     9,     12,    12,    12,
      12,    15,    15,    15,    15,    18,    18,    18,    18,    21,
      21,    21,    21,    24,    24,    24,    24,    27,    27,    27,
      27,    30,    30,    30,    30,    33,    33,    33,    33,    36,
      36,    36,    36,    39,    39,    39,    39,    42,    42,    42,
      42,    45,    45,    45,    45,    48,    48,    48,    48,    51,
      51,    51,    51,    54,    54,    54,    54,    57,    57,    57,
      57,    60,    60,    60,    60,    63,    63,    63,    63,    66,
      66,    66,    66,    69,    69,    69,    69,    72,    72,    72,
      72,    75,    75,    75,    75,    78,    78,    78,    78,    81,
      81,    81,    81,    84,    84,    84,    84,    87,    87,    87,
      87,    90,    90,    90,    90,    93,    93,    93,    93,    96,
      96,    96,    96,    99,    99,    99,    99,    102,   102,   102,
      102,   105,   105,   105,   105,   108,   108,   108,   108,   111,
      111,   111,   111,   114,   114,   114,   114,   117,   117,   117,
      117,   120,   120,   120,   120,   123,   123,   123,   123,   126,
      126,   126,   126,   129,   129,   129,   129,   132,   132,   132,
      132,   135,   135,   135,   135,   138,   138,   138,   138,   141,
      141,   141,   141,   144,   144,   144,   144,   147,   147,   147,
      147,   150,   150,   150,   150,   153,   153,   153,   153,   156,
      156,   156,   156,   159,   159,   159,   159,   162,   162,   162,
      162,   165,   165,   165,   165,   168,   168,   168,   168,   171,
      171,   171,   171,   174,   174,   174,   174,   177,   177,   177,
      177,   180,   180,   180,   180,   183,   183,   183,   183,   186,
      186,   186,   186,   189,   189,   189,   189,   192,   192,   192,
      192,   196,   196,   196,   196,   200,   200,   200,   200,   204,
      204,   204,   204,   208,   208,   208,   208,   212,   212,   212,
      212,   216,   216,   216,   216,   220,   220,   220,   220,   224,
      224,   224,   224,   228,   228,   228,   228,   232,   232,   232,
      232,   236,   236,   236,   236,   240,   240,   240,   240,   244,
      244,   244,   244,   248,   248,   248,   248,   252,   252,   252,
      252,   256,   256,   256,   256,   260,   260,   260,   260,   264,
      264,   264,   264,   268,   268,   268,   268,   272,   272,   272,
      272,   276,   276,   276,   276,   280,   280,   280,   280,   284,
      284,   284,   284,   288,   288,   288,   288,   292,   292,   292,
      292,   296,   296,   296,   296,   300,   300,   300,   300,   304,
      304,   304,   304,   308,   308,   308,   308,   312,   312,   312,
      312,   316,   316,   316,   316,   320,   320,   320,   320,   324,
      324,   324,   324,   328,   328,   328,   328,   332,   332,   332,
      332,   336,   336,   336,   336,   340,   340,   340,   340,   344,
      344,   344,   344,   348,   348,   348,   348,   352,   352,   352,
      352,   356,   356,   356,   356,   360,   360,   360,   360,   364,
      364,   364,   364,   368,   368,   368,   368,   372,   372,   372,
      372,   376,   376,   376,   376,   380,   380,   380,   380,   384,
      384,   384,   384,   388,   388,   388,   388,   392,   392,   392,
      392,   396,   396,   396,   396,   400,   400,   400,   400,   404,
      404,   404,   404,   408,   408,   408,   408,   412,   412,   412,
      412,   416,   416,   416,   416,   420,   420,   420,   420,   424,
      424,   424,   424,   428,   428,   428,   428,   432,   432,   432,
      432,   436,   436,   436,   436,   440,   440,   440,   440,   444,
      444,   444,   444,   448,   448,   448,   448,   452,   452,   452,
      452,   456,   456,   456,   456,   460,   460,   460,   460,   464,
      464,   464,   464,   468,   468,   468,   468,   472,   472,   472,
      472,   476,   476,   476,   476,   480,   480,   480,   480,   484,
      484,   484,   484,   488,   488,   488,   488,   492,   492,   492,
      492,   496,   496,   496,   496,   500,   500,   500,   500,   504,
      504,   504,   504,   508,   508,   508,   508,   512,   512,   512,
      512,   516,   516,   516,   516,   520,   520,   520,   520,   524,
      524,   524,   524,   528,   528,   528,   528,   532,   532,   532,
      532,   536,   536,   536,   536,   540,   540,   540,   540,   544,
      544,   544,   544,   548,   548,   548,   548,   552,   552,   552,
      552,   556,   556,   556,   556,   560,   560,   560,   560,   564,
      564,   564,   564,   568,   568,   568,   568,   572,   572,   572,
      572,   576,   576,   576,   576,   580,   580,   580,   580,   584,
      584,   584,   584,   588,   588,   588,   588,   592,   592,   592,
      592,   596,   596,   596,   596,   600,   600,   600,   600,   604,
      604,   604,   604,   608,   608,   608,   608,   612,   612,   612,
      612,   616,   616,   616,   616,   620,   620,   620,   620,   624,
      624,   624,   624,   628,   628,   628,   628,   632,   632,   632,
      632,   636,   636,   636,   636,   640,   640,   640,   640,   644,
      644,   644,   644,   648,   648,   648,   648,   652,   652,   652,
      652,   656,   656,   656,   656,   660,   660,   660,   660,   664,
      664,   664,   664,   668,   668,   668,   668,   672,   672,   672,
      672,   676,   676,   676,   676,   680,   680,   680,   680,   684,
      684,   684,   684,   688,   688,   688,   688,   692,   692,   692,
      692,   696,   696,   696,   696,   700,   700,   700,   700,   704,
      704,   704,   704,   708,   708,   708,   708,   712,   712,   712,
      712,   716,   716,   716,   716,   720,   720,   720,   720,   724,
      724,   724,   724,   728,   728,   728,   728,   732,   732,   732,
      732,   736,   736,   736,   736,   740,   740,   740,   740,   744,
      744,   744,   744,   748,   748,   748,   748,   752,   752,   752,
      752,   756,   756,   756,   756,   760,   760,   760,   760,   764,
      764,   764,   764,   768,   768,   768,   768,   772,   772,   772,
      772,   776,   776,   776,   776,   780,   780,   780,   780,   784,
      784,   784,   784,   788,   788,   788,   788,   792,   792,   792,
      792,   796,   796,   796,   796,   800,   800,   800,   800,   804,
      804,   804,   804,   808,   808,   808,   808,   812,   812,   812,
      812,   816,   816,   816,   816,   820,   820,   820,   820,   824,
      824,   824,   824,   828,   828,   828,   828,   832,   832,   832,
      832,   836,   836,   836,   836,   840,   840,   840,   840,   844,
      844,   844,   844,   848,   848,   848,   848,   852,   852,   852,
      852,   856,   856,   856,   856,   860,   860,   860,   860,   864,
      864,   864,   864,   868,   868,   868,   868,   872,   872,   872,
      872,   876,   876,   876,   876,   880,   880,   880,   880,   884,
      884,   884,   884,   888,   888,   888,   888,   892,   892,   892,
      892,   896,   896,   896,   896,   900,   900,   900,   900,   904,
      904,   904,   904,   908,   908,   908,   908,   912,   912,   912,
      912,   916,   916,   916,   916,   920,   920,   920,   920,   924,
      924,   924,   924,   928,   928,   928,   928,   932,   932,   932,
      932,   936,   936,   936,   936,   940,   940,   940,   940,   944,
      944,   944,   944,   948,   948,   948,   948,   952,   952,   952,
      952,   956,   956,   956,   956,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   960,   960,   960,   960,   960,   960,
      960,   960,   960,   960,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   964,   964,   964,   964,   964,   964,
      964,   964,   964,   964,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   968,   968,   968,   968,   968,   968,
      968,   968,   968,   968,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   972,   972,   972,   972,   972,   972,
      972,   972,   972,   972,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   976,   976,   976,   976,   976,   976,
      976,   976,   976,   976,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   980,   980,   980,   980,   980,   980,
      980,   980,   980,   980,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   984,   984,   984,   984,   984,   984,
      984,   984,   984,   984,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   988,
      988,   988,   988,   988,   988,   988,   988,   988,   988,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   992,   992,   992,   992,
      992,   992,   992,   992,   992,   992,   1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,  1625,
      1625,  1625,  1625,  1625,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,  2255,
      2255,  2255,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,  2886,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,
      3515,  3515,  3515,  3515,  3515,  3515,  3515,  3515,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,  4149,
      4149,  4149,  4149,  4149,  4149,  4149,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,  4784,
      4784,  4784,  4784,  4784,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,  5419,
      5419,  5419,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,  6047,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,
      6676,  6676,  6676,  6676,  6676,  6676,  6676,  6676,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,  7308,
      7308,  7308,  7308,  7308,  7308,  7308,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,  7943,
      7943,  7943,  7943,  7943,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,  8575,
      8575,  8575,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,  9205,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,
      9841,  9841,  9841,  9841,  9841,  9841,  9841,  9841,  10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478, 10478,
      10478, 10478, 10478, 10478, 10478, 10478};
  const unsigned long rleLoc[] = {
      0,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,
      3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,
      2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,
      1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,   4,   1,   2,   3,
      4,   1,   2,   3,   4,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,
      11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,
      26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,
      41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,
      56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,
      71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,
      86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100,
      101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
      116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 0,
      1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
      16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,
      31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,
      46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,
      61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,
      76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,
      91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105,
      106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
      121, 122, 123, 124, 125, 126, 127, 128, 129, 0,   1,   2,   3,   4,   5,
      6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,
      21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,
      36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,
      51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,
      66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,
      81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,
      96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
      111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
      126, 127, 128, 129, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,
      11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,
      26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,
      41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,
      56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,
      71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,
      86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100,
      101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
      116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 0,
      1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
      16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,
      31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,
      46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,
      61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,
      76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,
      91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105,
      106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
      121, 122, 123, 124, 125, 126, 127, 128, 129, 0,   1,   2,   3,   4,   5,
      6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,
      21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,
      36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,
      51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,
      66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,
      81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,
      96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
      111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
      126, 127, 128, 129, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,
      11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,
      26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,
      41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,
      56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,
      71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,
      86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100,
      101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
      116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 0,
      1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
      16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,
      31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,
      46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,
      61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,
      76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,
      91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105,
      106, 107, 108, 109, 110, 111, 112, 113, 114, 1,   2,   3,   4,   5,   6,
      7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,
      22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,
      37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,
      52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,
      67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,
      82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,
      97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
      112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126,
      127, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,
      14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,
      29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,
      44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,
      59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,
      74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,
      89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103,
      104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118,
      119, 120, 121, 122, 123, 124, 125, 126, 127, 0,   1,   2,   3,   4,   5,
      6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,
      21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,
      36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,
      51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,
      66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,
      81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,
      96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
      111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
      126, 127, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,
      13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,
      28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,
      43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,
      58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,
      73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,
      88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102,
      103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
      118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 0,   1,   2,   3,   4,
      5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,
      20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,
      35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,
      50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,
      65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,
      80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,
      95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
      110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124,
      125, 126, 127, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,
      12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,
      27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,
      42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,
      57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,
      72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,
      87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101,
      102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
      117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 0,   1,   2,   3,
      4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,
      19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,
      34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
      49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,
      64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,
      79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,
      94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108,
      109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123,
      124, 125, 126, 127, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,
      11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,
      26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,
      41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,
      56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,
      71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,
      86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100,
      101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
      116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 0,   1,   2,
      3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,
      18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,
      33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,
      48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,
      63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,
      78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,
      93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107,
      108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122,
      123, 124, 125, 126, 127, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
      10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
      25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,
      40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,
      55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,
      70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,
      85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,
      100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114,
      115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 0,   1,
      2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,
      17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,
      32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,
      47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,
      62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,
      77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,
      92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106,
      107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121,
      122, 123, 124, 125, 126, 127, 0,   1,   2,   3,   4,   5,   6,   7,   8,
      9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,
      24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,
      39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,
      54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,
      69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,
      84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,
      99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113,
      114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 0,
      1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
      16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,
      31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,
      46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,
      61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,
      76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,
      91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105,
      106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
      121, 122, 123, 124, 125, 126, 127, 0,   1,   2,   3,   4,   5,   6,   7,
      8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,
      23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,
      38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,
      53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,
      68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,
      83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,
      98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,
      113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127,
      0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,
      15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,
      30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,
      45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,
      60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,
      75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,
      90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104,
      105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
      120, 121, 122, 123, 124, 125, 126, 127, 0,   1,   2,   3,   4,   5,   6,
      7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,
      22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,
      37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,
      52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,
      67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,
      82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,
      97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
      112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126,
      127};
  std::vector<uint64_t> positions[4096];
  for (size_t i = 0; i < 4096; ++i) {
    positions[i].push_back(fileLoc[i]);
    positions[i].push_back(rleLoc[i]);
  }
  std::unique_ptr<IntDecoder<true>> rle = IntDecoder<true>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(stream),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(2048);
  rle->next(data.data(), data.size(), nullptr);
  for (size_t i = 0; i < data.size(); ++i) {
    if (i < 1024) {
      EXPECT_EQ(i / 4, data[i]) << "Wrong output at " << i;
    } else {
      EXPECT_EQ(2 * i, data[i]) << "Wrong output at " << i;
    }
  }
  rle->next(data.data(), data.size(), nullptr);
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(junk[i], data[i]) << "Wrong output at " << i;
  }
  size_t i = 4096;
  do {
    --i;
    dwio::common::PositionProvider location(positions[i]);
    rle->seekToRowGroup(location);
    rle->next(data.data(), 1, nullptr);
    if (i < 1024) {
      EXPECT_EQ(i / 4, data[0]) << "Wrong output at " << i;
    } else if (i < 2048) {
      EXPECT_EQ(2 * i, data[0]) << "Wrong output at " << i;
    } else {
      EXPECT_EQ(junk[i - 2048], data[0]) << "Wrong output at " << i;
    }
  } while (i != 0);

  // Seek to end
  std::vector<uint64_t> position;
  position.push_back(VELOX_ARRAY_SIZE(buffer));
  position.push_back(0);
  dwio::common::PositionProvider pp{position};
  rle->seekToRowGroup(pp);
  // Seek is fine, but read should fail
  EXPECT_THROW(rle->next(data.data(), 1, nullptr), VeloxException);

  // Seek to end + 1
  position.clear();
  position.push_back(VELOX_ARRAY_SIZE(buffer));
  position.push_back(1);
  dwio::common::PositionProvider pp2{position};
  EXPECT_THROW(rle->seekToRowGroup(pp2), VeloxException);
}

TEST(RLEv1, testLeadingNulls) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  const unsigned char buffer[] = {0xfb, 0x01, 0x02, 0x03, 0x04, 0x05};
  std::unique_ptr<IntDecoder<false>> rle = IntDecoder<false>::createRle(
      std::unique_ptr<dwio::common::SeekableInputStream>(
          new dwio::common::SeekableArrayInputStream(
              buffer, VELOX_ARRAY_SIZE(buffer))),
      RleVersion_1,
      *scopedPool,
      true,
      INT_BYTE_SIZE);
  std::vector<int64_t> data(10);
  uint64_t isNull[1] = {bits::kNotNull};
  for (int32_t i = 0; i < 10; i++) {
    bits::setNull(isNull, i, i < 5);
  }
  rle->next(data.data(), 10, isNull);

  for (size_t i = 5; i < 10; ++i) {
    EXPECT_EQ(i - 4, data[i]) << "Output wrong at " << i;
  }
}
