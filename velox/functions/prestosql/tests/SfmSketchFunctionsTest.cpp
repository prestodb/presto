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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/SfmSketchType.h"

namespace facebook::velox::functions::test {
using SfmSketch = facebook::velox::functions::sfm::SfmSketch;

class SfmSketchFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  HashStringAllocator allocator_{pool_.get()};
};

TEST_F(SfmSketchFunctionsTest, cardinalityTest) {
  const auto cardinality = [&](const std::optional<std::string>& input) {
    return evaluateOnce<int64_t>("cardinality(c0)", SFMSKETCH(), input);
  };

  // Test empty sketch.
  SfmSketch sketch{&allocator_, /* seed */ 1};
  sketch.initialize(4096, 24);

  const auto serializedSize = sketch.serializedSize();

  std::vector<char> buffer(serializedSize);
  sketch.serialize(buffer.data());
  std::string serializedEmpty(buffer.begin(), buffer.end());
  EXPECT_EQ(*cardinality(serializedEmpty), 0);

  // Add distinct elements
  const int64_t numElements = 100'000;
  for (auto i = 0; i < numElements; i++) {
    sketch.add(i);
  }
  // Add noise to the sketch.
  sketch.enablePrivacy(8.0);

  std::vector<char> buffer2(sketch.serializedSize());
  sketch.serialize(buffer2.data());
  std::string serialized2(buffer2.begin(), buffer2.end());
  const auto result = cardinality(serialized2);

  // SfmSketch is an approximate algorithm, typically we allow 25% error.
  // In the unit test, we are using deterministic seed.
  EXPECT_EQ(*result, 100'305);
}

TEST_F(SfmSketchFunctionsTest, cardinalityWithDuplicates) {
  const auto cardinality = [&](const std::optional<std::string>& input) {
    return evaluateOnce<int64_t>("cardinality(c0)", SFMSKETCH(), input);
  };

  SfmSketch sketch{&allocator_, 1};
  sketch.initialize(4096, 24);

  for (int i = 0; i < 100000; i++) {
    sketch.add(i % 100);
  }

  std::string buffer(sketch.serializedSize(), '\0');
  sketch.serialize(buffer.data());
  const auto result = cardinality(buffer);

  EXPECT_EQ(*result, 99);
}

TEST_F(SfmSketchFunctionsTest, noisyEmptyApproxSetSfm) {
  // Test with epsilon only.
  auto a = evaluateOnce<int64_t>(
      "cardinality(noisy_empty_approx_set_sfm(INFINITY()))",
      makeRowVector(ROW({}), 1));
  ASSERT_EQ(*a, 0);

  // Test with epsilon and buckets.
  auto b = evaluateOnce<int64_t>(
      "cardinality(noisy_empty_approx_set_sfm(INFINITY(), 1024))",
      makeRowVector(ROW({}), 1));
  ASSERT_EQ(*b, 0);

  // Test with epsilon, buckets, and precision.
  auto c = evaluateOnce<int64_t>(
      "cardinality(noisy_empty_approx_set_sfm(INFINITY(), 1024, 20))",
      makeRowVector(ROW({}), 1));
  ASSERT_EQ(*c, 0);
}

TEST_F(SfmSketchFunctionsTest, mergeSfmSketchArray) {
  // Create two sketches with overlapping elements.
  SfmSketch a{&allocator_};
  a.initialize(4096, 24);
  for (int i = 0; i < 50; i++) {
    a.add(i);
  }

  SfmSketch b{&allocator_};
  b.initialize(4096, 24);
  for (int i = 25; i < 75; i++) {
    b.add(i);
  }

  std::string aString(a.serializedSize(), '\0');
  a.serialize(aString.data());

  std::string bString(b.serializedSize(), '\0');
  b.serialize(bString.data());

  // Test merging array of sketches.
  const auto mergedResult = evaluateOnce<std::string>(
      "merge_sfm(c0)",
      makeRowVector(
          {makeArrayVector<std::string>({{aString, bString}}, SFMSKETCH())}));

  const auto mergedCardinality =
      evaluateOnce<int64_t>("cardinality(c0)", SFMSKETCH(), mergedResult);

  // There are 75 distinct elements in the sketch.
  ASSERT_EQ(mergedCardinality, 75);
}

TEST_F(SfmSketchFunctionsTest, mergeSfmSketchEmpty) {
  // Test merging empty array should return null.
  const auto result = evaluate(
      "merge_sfm(c0)",
      makeRowVector({makeArrayVector<std::string>({}, SFMSKETCH())}));

  EXPECT_TRUE(result->isNullAt(0));
}

TEST_F(SfmSketchFunctionsTest, mergeSfmSketchAllNulls) {
  // Test merging array with all null sketches should return null.
  const auto result = evaluate(
      "merge_sfm(c0)",
      makeRowVector({makeNullableArrayVector<std::string>(
          {std::nullopt, std::nullopt}, ARRAY(SFMSKETCH()))}));

  EXPECT_TRUE(result->isNullAt(0));
}

TEST_F(SfmSketchFunctionsTest, cardinalityNull) {
  // Test cardinality with null input.
  const auto result = evaluate(
      "cardinality(c0)",
      makeRowVector(
          {makeNullableFlatVector<std::string>({std::nullopt}, SFMSKETCH())}));

  EXPECT_TRUE(result->isNullAt(0));
}

} // namespace facebook::velox::functions::test
