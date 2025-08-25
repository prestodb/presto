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

#include "velox/functions/lib/sfm/SfmSketch.h"
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::functions::sfm {

class SfmSketchTest : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

 protected:
  const int32_t numBuckets_ = 4096;
  const int32_t precision_ = 24;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  HashStringAllocator allocator_{pool_.get()};
};

TEST_F(SfmSketchTest, add) {
  SfmSketch a(&allocator_, 1);
  VELOX_ASSERT_THROW(a.add(1), "Sketch is not initialized.");

  a.initialize(numBuckets_, precision_);

  for (int32_t i = 0; i < 1000; i++) {
    a.add(i);
  }
  a.enablePrivacy(3.0);
  const auto cardinality = a.cardinality();

  // Add the same values through addIndexAndZeros, and check that the
  // cardinality is the same.
  auto computeIndexAndZeros = [&](int32_t value) {
    const auto hash = XXH64(&value, sizeof(value), 0);
    const int32_t kBitWidth = sizeof(uint64_t) * 8;
    const auto numIndexBits = static_cast<int32_t>(std::log2(numBuckets_));
    const uint64_t trailing = hash | (1ULL << (kBitWidth - numIndexBits));
    const auto index = static_cast<int32_t>(hash >> (kBitWidth - numIndexBits));
    const auto zeros = __builtin_ctzll(trailing);
    return std::make_pair(index, zeros);
  };

  SfmSketch b(&allocator_, 1);
  b.initialize(numBuckets_, precision_);

  for (int32_t i = 0; i < 1000; i++) {
    const auto [index, zeros] = computeIndexAndZeros(i);
    b.addIndexAndZeros(index, zeros);
  }

  b.enablePrivacy(3.0);
  ASSERT_EQ(b.cardinality(), cardinality);
}

TEST_F(SfmSketchTest, enablePrivacy) {
  SfmSketch sketch(&allocator_);
  VELOX_ASSERT_THROW(sketch.enablePrivacy(3.0), "Sketch is not initialized.");

  sketch.initialize(numBuckets_, precision_);
  ASSERT_FALSE(sketch.privacyEnabled());

  sketch.enablePrivacy(std::numeric_limits<double>::infinity());
  ASSERT_FALSE(sketch.privacyEnabled());

  sketch.enablePrivacy(1.0);
  ASSERT_TRUE(sketch.privacyEnabled());

  VELOX_ASSERT_THROW(sketch.enablePrivacy(1.0), "Privacy is already enabled.");
  VELOX_ASSERT_THROW(sketch.add(1), "Private sketch is immutable.");
}

TEST_F(SfmSketchTest, merge) {
  SfmSketch a(&allocator_);
  SfmSketch b(&allocator_);
  VELOX_ASSERT_THROW(a.mergeWith(b), "Sketch is not initialized.");

  a.initialize(numBuckets_, precision_);
  b.initialize(numBuckets_, precision_);

  a.add(1);
  b.add(1);

  a.mergeWith(b);

  // Merging two non-private sketches results in a non-private sketch.
  ASSERT_FALSE(a.privacyEnabled());

  b.enablePrivacy(3.0);
  a.mergeWith(b);

  // Merging a private sketch with a non-private sketch results in a private
  // sketch.
  ASSERT_TRUE(a.privacyEnabled());
}

TEST_F(SfmSketchTest, cardinality) {
  SfmSketch sketch(&allocator_, 3);
  VELOX_ASSERT_THROW(sketch.cardinality(), "Sketch is not initialized.");
  sketch.initialize(numBuckets_, precision_);

  // Non-private sketch should have 0 cardinality.
  ASSERT_EQ(sketch.cardinality(), 0);

  // Private sketch should have approximately 0 cardinality, but not exactly 0.
  sketch.enablePrivacy(3.0);
  ASSERT_EQ(sketch.cardinality(), 4);

  struct TestParams {
    int32_t numDistinct;
    double epsilon;
    int32_t expectedCardinality;
  };

  std::vector<TestParams> testParams = {
      {10000, 2.0, 9804},
      {10000, 4.0, 9898},
      {10000, std::numeric_limits<double>::infinity(), 9926},
      {100000, 2.0, 101130},
      {100000, 4.0, 100044},
      {100000, std::numeric_limits<double>::infinity(), 100332},
      {1000000, 2.0, 1017354},
      {1000000, 4.0, 1003236},
      {1000000, std::numeric_limits<double>::infinity(), 999603}};

  for (const auto& params : testParams) {
    SCOPED_TRACE(
        "numDistinct = " + std::to_string(params.numDistinct) +
        ", epsilon = " + std::to_string(params.epsilon));
    SfmSketch sketch(&allocator_, 1);
    sketch.initialize(numBuckets_, precision_);

    for (auto i = 0; i < params.numDistinct; i++) {
      sketch.add(i);
    }
    sketch.enablePrivacy(params.epsilon);

    ASSERT_EQ(sketch.cardinality(), params.expectedCardinality);
  }
}

TEST_F(SfmSketchTest, serializationRoundTrip) {
  SfmSketch sketch(&allocator_);
  sketch.initialize(numBuckets_, precision_);

  for (int32_t i = 0; i < 10000; i++) {
    sketch.add(i);
  }

  sketch.enablePrivacy(4.0);
  const auto originalCardinality = sketch.cardinality();

  // Allocate buffer for serialization.
  const auto serializedSize = sketch.serializedSize();
  std::vector<char> buffer(serializedSize);
  char* out = buffer.data();

  sketch.serialize(out);
  auto deserialized = SfmSketch::deserialize(out, &allocator_);

  // Test that the deserialized sketch is the same as the
  // original.
  ASSERT_EQ(deserialized.cardinality(), originalCardinality);
  ASSERT_TRUE(deserialized.privacyEnabled());
  ASSERT_EQ(sketch.numIndexBits(), deserialized.numIndexBits());
  ASSERT_EQ(sketch.precision(), deserialized.precision());
}

TEST_F(SfmSketchTest, cardinalityConsistency) {
  // This test validates the cardinality estimation functionality of SfmSketch
  // across multiple scenarios and data sizes:
  // 1. Duplicate handling
  // 2. Order independence
  // 3. Merge consistency
  // 4. Serialization integrity
  // 5. Privacy noise behavior

  std::vector<int32_t> dataSizes = {1000, 10000, 100000};

  auto testCardinalityConsistency = [&](int32_t dataSize) {
    SCOPED_TRACE("dataSize = " + std::to_string(dataSize));

    SfmSketch base(&allocator_, 1);
    base.initialize(numBuckets_, precision_);
    for (int32_t i = 0; i < dataSize; i++) {
      base.add(i);
    }
    base.enablePrivacy(8.0);

    // This is the baseCardinality other scenarios will be compared against.
    // baseCardinality should be approximately dataSize, with a relative error
    // tolerance.
    const auto baseCardinality = base.cardinality();
    ASSERT_NEAR(baseCardinality, dataSize, dataSize * 0.05);

    // Test duplicate handling.
    SfmSketch a(&allocator_, 1);
    a.initialize(numBuckets_, precision_);
    for (int j = 0; j < 10; j++) {
      for (int32_t i = 0; i < dataSize; i++) {
        a.add(i);
      }
    }
    a.enablePrivacy(8.0);
    ASSERT_EQ(baseCardinality, a.cardinality());

    // Test order independence.
    SfmSketch b(&allocator_, 1);
    b.initialize(numBuckets_, precision_);
    // We make sure that 0 - (dataSize-1) are all present in the sketch, but in
    // random order.
    std::vector<int32_t> values;
    values.reserve(dataSize);
    for (int32_t i = 0; i < dataSize; i++) {
      values.emplace_back(i);
    }
    std::shuffle(values.begin(), values.end(), folly::ThreadLocalPRNG());
    for (int32_t i = 0; i < dataSize; i++) {
      b.add(values[i]);
    }

    // Then we add random duplicate values to the sketch.
    int32_t duplicateCount = std::max(1, dataSize / 10);
    for (int32_t i = 0; i < duplicateCount; i++) {
      b.add(folly::Random::rand32(dataSize));
    }

    b.enablePrivacy(8.0);
    ASSERT_EQ(baseCardinality, b.cardinality());

    // Test merge consistency.
    SfmSketch c(&allocator_, 1);
    c.initialize(numBuckets_, precision_);
    SfmSketch d(&allocator_, 1);
    d.initialize(numBuckets_, precision_);
    SfmSketch e(&allocator_, 1);
    e.initialize(numBuckets_, precision_);
    for (int32_t i = 0; i < dataSize; i++) {
      if (i % 3 == 0) {
        c.add(i);
      } else if (i % 3 == 1) {
        d.add(i);
      } else {
        e.add(i);
      }
    }
    // Merge before enabling privacy.
    c.mergeWith(d);
    c.mergeWith(e);
    c.enablePrivacy(8.0);
    ASSERT_EQ(baseCardinality, c.cardinality());

    // Test serialization integrity.
    const auto serializedSize = c.serializedSize();
    std::vector<char> buffer(serializedSize);
    char* out = buffer.data();
    c.serialize(out);
    auto deserialized = SfmSketch::deserialize(out, &allocator_);
    ASSERT_EQ(baseCardinality, deserialized.cardinality());

    // Test privacy noise behavior.
    // Now enable privacy for d and e and merge them into c.
    // There should be noise added to the merged sketch.
    d.enablePrivacy(8.0);
    e.enablePrivacy(8.0);
    c.mergeWith(d);
    c.mergeWith(e);
    ASSERT_NE(baseCardinality, c.cardinality());
    ASSERT_NEAR(baseCardinality, c.cardinality(), baseCardinality * 0.05);
  };

  for (const auto dataSize : dataSizes) {
    testCardinalityConsistency(dataSize);
  }
}

TEST_F(SfmSketchTest, javaSerializationCompatibility) {
  // This test is to ensure that the C++ serialization format is compatible
  // with the Java serialization format.

  // The string is from a Java sketch serialized with the Java SfmSketch.
  std::string javaSketchBase64 =
      "BwgAAAAQAAAAw8JDHvpqkj//AQAA77//////////////v///////////3////7//"
      "////////////3/////////////////////////////////v//+/+ff/////////"
      "f///v//+//////////////////////7//////3////v//////3////v/f//////"
      "///3////v///////v/f///////////////////////////////f///////v/////"
      "////f///////7////+////9///////////////v//////////////////////////"
      "////////////f////////////3//////////////7////+////////////////////"
      "3///////////////////////9///33///v///3////f//f//9//+//////r/3///"
      "///9//Dfz/e/++vvfV3W//qtq33z//a7///+/m3/fz7/1///1v5hmo/ayTcv3l3Vv"
      "XmnMr+t/B72YloTNgmBy7+IKv+mIBmaBwSO4GwxBMCwWChIoYSVFTAAIIhLylIEj"
      "mJLamBAAooMYLQgikgBAUACEAQYJUAmxMGAhDAoBJGgRgAkBCCCAABAAkIIAAAAA"
      "EQAYEECAQYACgAACEAAQEBAACCAgAAAAQgAAAAAKgAoERABAMAAQbABAAgADEAAQ"
      "IABQ==";

  // Decode base64 to binary data.
  std::string decoded = encoding::Base64::decode(javaSketchBase64);

  auto sketch = SfmSketch::deserialize(decoded.c_str(), &allocator_);
  // Test that the deserialized sketch is the same as the original.
  ASSERT_EQ(sketch.cardinality(), 927499);
}
} // namespace facebook::velox::functions::sfm
