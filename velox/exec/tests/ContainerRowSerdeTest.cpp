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

#include "velox/exec/ContainerRowSerde.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec {

namespace {

class ContainerRowSerdeTest : public testing::Test,
                              public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  // Writes all rows together and returns a position at the start of this
  // combined write.
  HashStringAllocator::Position serialize(
      const VectorPtr& data,
      bool isKey = true) {
    ByteOutputStream out(&allocator_);
    auto position = allocator_.newWrite(out);
    exec::ContainerRowSerdeOptions options{.isKey = isKey};
    for (auto i = 0; i < data->size(); ++i) {
      ContainerRowSerde::serialize(*data, i, out, options);
    }
    allocator_.finishWrite(out, 0);
    return position;
  }

  // Writes each row individually and returns positions for individual rows.
  std::vector<HashStringAllocator::Position> serializeWithPositions(
      const VectorPtr& data,
      bool isKey = true) {
    std::vector<HashStringAllocator::Position> positions;
    auto size = data->size();
    positions.reserve(size);
    exec::ContainerRowSerdeOptions options{.isKey = isKey};

    for (auto i = 0; i < size; ++i) {
      ByteOutputStream out(&allocator_);
      auto position = allocator_.newWrite(out);
      ContainerRowSerde::serialize(*data, i, out, options);
      allocator_.finishWrite(out, 0);
      positions.emplace_back(position);
    }

    return positions;
  }

  VectorPtr deserialize(
      HashStringAllocator::Position position,
      const TypePtr& type,
      vector_size_t numRows) {
    auto data = BaseVector::create(type, numRows, pool());
    // Set all rows in data to NULL to verify that deserialize can clear nulls
    // correctly.
    for (auto i = 0; i < numRows; ++i) {
      data->setNull(i, true);
    }

    auto in = HashStringAllocator::prepareRead(position.header);
    for (auto i = 0; i < numRows; ++i) {
      ContainerRowSerde::deserialize(in, i, data.get());
    }
    return data;
  }

  void testRoundTrip(const VectorPtr& data) {
    auto position = serialize(data);
    auto copy = deserialize(position, data->type(), data->size());
    test::assertEqualVectors(data, copy);

    allocator_.clear();
  }

  // If the mode is NullAsIndeterminate with equalsOnly is false, and expected
  // is kIndeterminate, then the test ensures that an exception is thrown with
  // the message "Ordering nulls is not supported".
  void testCompareWithNulls(
      const DecodedVector& decodedVector,
      const std::vector<HashStringAllocator::Position>& positions,
      const std::vector<std::optional<int32_t>>& expected,
      bool equalsOnly,
      CompareFlags::NullHandlingMode mode) {
    CompareFlags compareFlags{
        true, // nullsFirst
        true, // ascending
        equalsOnly,
        mode};

    for (auto i = 0; i < expected.size(); ++i) {
      auto stream = HashStringAllocator::prepareRead(positions.at(i).header);
      if (expected.at(i) == kIndeterminate &&
          mode == CompareFlags::NullHandlingMode::kNullAsIndeterminate &&
          !equalsOnly) {
        VELOX_ASSERT_THROW(
            ContainerRowSerde::compareWithNulls(
                stream, decodedVector, i, compareFlags),
            "Ordering nulls is not supported");
      } else {
        ASSERT_EQ(
            expected.at(i),
            ContainerRowSerde::compareWithNulls(
                stream, decodedVector, i, compareFlags));
      }
    }
  }

  // If the mode is NullAsIndeterminate with equalsOnly is false, and expected
  // is kIndeterminate, then the test ensures that an exception is thrown with
  // the message "Ordering nulls is not supported".
  void testCompareByteStreamWithNulls(
      const std::vector<HashStringAllocator::Position>& leftPositions,
      const std::vector<HashStringAllocator::Position>& rightPositions,
      const std::vector<std::optional<int32_t>>& expected,
      const TypePtr& type,
      bool equalsOnly,
      CompareFlags::NullHandlingMode mode) {
    CompareFlags compareFlags{
        true, // nullsFirst
        true, // ascending
        equalsOnly,
        mode};

    for (auto i = 0; i < expected.size(); ++i) {
      auto leftStream =
          HashStringAllocator::prepareRead(leftPositions.at(i).header);
      auto rightStream =
          HashStringAllocator::prepareRead(rightPositions.at(i).header);
      if (expected.at(i) == kIndeterminate &&
          mode == CompareFlags::NullHandlingMode::kNullAsIndeterminate &&
          !equalsOnly) {
        VELOX_ASSERT_THROW(
            ContainerRowSerde::compareWithNulls(
                leftStream, rightStream, type.get(), compareFlags),
            "Ordering nulls is not supported");
      } else {
        ASSERT_EQ(
            expected.at(i),
            ContainerRowSerde::compareWithNulls(
                leftStream, rightStream, type.get(), compareFlags));
      }
    }
  }

  void testCompare(const VectorPtr& vector) {
    auto positions = serializeWithPositions(vector);

    CompareFlags compareFlags =
        CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

    DecodedVector decodedVector(*vector);

    for (auto i = 0; i < positions.size(); ++i) {
      auto stream = HashStringAllocator::prepareRead(positions.at(i).header);
      ASSERT_EQ(
          0, ContainerRowSerde::compare(stream, decodedVector, i, compareFlags))
          << "at " << i << ": " << vector->toString(i);
    }
  }

  void assertNotEqualVectors(
      const VectorPtr& actual,
      const VectorPtr& expected) {
    ASSERT_EQ(actual->size(), expected->size());
    ASSERT_TRUE(actual->type()->equivalent(*expected->type()))
        << "Expected " << expected->type()->toString() << ", but got "
        << actual->type()->toString();
    for (auto i = 0; i < expected->size(); i++) {
      if (!actual->equalValueAt(expected.get(), i, i)) {
        return;
      }
    }
    FAIL() << "Expect two vectors are not equal.";
  }

  HashStringAllocator allocator_{pool()};
};

TEST_F(ContainerRowSerdeTest, bigint) {
  auto data = makeFlatVector<int64_t>({1, 2, 3, 4, 5});

  testRoundTrip(data);
}

TEST_F(ContainerRowSerdeTest, string) {
  auto data =
      makeFlatVector<std::string>({"a", "Abc", "Long test sentence.", "", "d"});

  testRoundTrip(data);
}

TEST_F(ContainerRowSerdeTest, arrayOfBigint) {
  auto data = makeArrayVector<int64_t>({
      {1, 2, 3},
      {4, 5},
      {6},
      {},
  });

  testRoundTrip(data);

  data = makeNullableArrayVector<int64_t>({
      {{{1, std::nullopt, 2, 3}}},
      {{{std::nullopt, 4, 5}}},
      {{{6, std::nullopt}}},
      {{std::vector<std::optional<int64_t>>({})}},
  });

  testRoundTrip(data);
}

TEST_F(ContainerRowSerdeTest, arrayOfString) {
  auto data = makeArrayVector<std::string>({
      {"a", "b", "Longer string ...."},
      {"c", "Abc", "Mountains and rivers"},
      {},
      {"Oceans and skies"},
  });

  testRoundTrip(data);

  data = makeNullableArrayVector<std::string>({
      {{{std::nullopt,
         std::nullopt,
         "a",
         std::nullopt,
         "b",
         "Longer string ...."}}},
      {{{"c", "Abc", std::nullopt, "Mountains and rivers"}}},
      {{std::vector<std::optional<std::string>>({})}},
      {{{"Oceans and skies"}}},
  });

  testRoundTrip(data);
}

TEST_F(ContainerRowSerdeTest, map) {
  auto data = makeMapVector<int64_t, int64_t>({
      {{2, 20}, {3, 30}, {1, 10}},
      {{4, 40}},
  });

  testRoundTrip(data);

  // When sortMap is not set, serialized Map has the same order of map entries
  // as the input.
  {
    auto position = serialize(data, false);
    auto deserialized = deserialize(position, data->type(), data->size());
    test::assertEqualVectors(
        data->mapKeys(), deserialized->as<MapVector>()->mapKeys());
  }

  // When sortMap is set, serialized Map has map entries sorted and thus in a
  // different order than the input.
  {
    auto position = serialize(data, true);
    auto deserialized = deserialize(position, data->type(), data->size());
    assertNotEqualVectors(
        data->mapKeys(), deserialized->as<MapVector>()->mapKeys());
  }
}

TEST_F(ContainerRowSerdeTest, nested) {
  auto data = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<std::string>({"a", "", "Long test sentence ......"}),
       makeNullableArrayVector<std::string>({{"a", "b", "c"}, {}, {"d"}})});

  testRoundTrip(data);

  auto nestedArray = makeNullableNestedArrayVector<std::string>(
      {{{{{"1", "2"}}, {{"3", "4"}}}}, {{}}, {{std::nullopt, {}}}});

  testRoundTrip(nestedArray);

  std::vector<std::pair<std::string, std::optional<int64_t>>> map{
      {"a", {1}}, {"b", {2}}, {"c", {3}}, {"d", {4}}};
  nestedArray = makeArrayOfMapVector<std::string, int64_t>(
      {{map, std::nullopt}, {std::nullopt}});

  testRoundTrip(nestedArray);
}

TEST_F(ContainerRowSerdeTest, compareNullsInArrayVector) {
  auto data = makeNullableArrayVector<int64_t>({
      {1, 2},
      {1, 5},
      {1, 3, 5},
      {1, 2, 3, 4},
      {1, 2, std::nullopt, 4},
      {1, std::nullopt, 5},
  });
  auto positions = serializeWithPositions(data);
  auto arrayVector = makeNullableArrayVector<int64_t>({
      {1, 2},
      {1, 3},
      {1, 5},
      {std::nullopt, 1},
      {1, 2, std::nullopt, 4},
      {1, 5},
  });
  DecodedVector decodedVector(*arrayVector);

  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {1}, {-1}, std::nullopt, std::nullopt, std::nullopt},
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {1}, {1}, {1}, std::nullopt, {1}},
      true,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {1}, {-1}, {1}, {0}, {-1}},
      false,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInMapVector) {
  auto data = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 10}, {4, 30}, {2, 3}}},
      {{{2, 20}}},
      {{{3, 50}}},
      {{{4, std::nullopt}}},
  });
  auto positions = serializeWithPositions(data);
  auto mapVector = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 10}, {3, 20}}},
      {{{2, 20}}},
      {{{3, 40}}},
      {{{4, std::nullopt}}},
  });
  DecodedVector decodedVector(*mapVector);

  testCompareWithNulls(
      decodedVector,
      positions,
      {{-1}, {0}, {1}, std::nullopt},
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{1}, {0}, {1}, std::nullopt},
      true,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{1}, {0}, {1}, {0}},
      true,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInRowVector) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4})});
  auto positions = serializeWithPositions(data);
  auto someNulls = makeNullableFlatVector<int32_t>({1, 3, 2, std::nullopt});
  auto rowVector = makeRowVector({someNulls});
  DecodedVector decodedVector(*rowVector);

  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {-1}, {1}, std::nullopt},
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareWithNulls(
      decodedVector,
      positions,
      {{0}, {-1}, {1}, {1}},
      false,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInArrayByteStream) {
  auto left = makeNullableArrayVector<int64_t>({
      {1, 2},
      {1, 5},
      {1, 3, 5},
      {1, 2, 3, 4},
      {1, 2, std::nullopt, 4},
      {1, std::nullopt, 5},
  });
  auto leftPositions = serializeWithPositions(left);

  auto right = makeNullableArrayVector<int64_t>({
      {1, 2},
      {1, 3},
      {1, 5},
      {std::nullopt, 1},
      {1, 2, std::nullopt, 4},
      {1, 5},
  });
  auto rightPositions = serializeWithPositions(right);

  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {1}, {-1}, std::nullopt, std::nullopt, std::nullopt},
      ARRAY(BIGINT()),
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {1}, {1}, {1}, std::nullopt, {1}},
      ARRAY(BIGINT()),
      true,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {1}, {-1}, {1}, {0}, {-1}},
      ARRAY(BIGINT()),
      false,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInRowByteStream) {
  auto left = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3, 4}),
       makeFlatVector<int32_t>({1, 2, 3, 4})});
  auto leftPositions = serializeWithPositions(left);
  auto right = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 3, 2, std::nullopt}),
       makeFlatVector<int32_t>({1, 2, 3, 4})});
  auto rightPositions = serializeWithPositions(right);

  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {-1}, {1}, std::nullopt},
      ROW({INTEGER(), INTEGER()}),
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{0}, {-1}, {1}, {1}},
      ROW({INTEGER(), INTEGER()}),
      false,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, compareNullsInMapByteStream) {
  auto left = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 10}, {4, 30}, {2, 3}}},
      {{{2, 20}}},
      {{{3, 50}}},
      {{{4, std::nullopt}}},
  });
  auto leftPositions = serializeWithPositions(left);

  auto right = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 10}, {3, 20}}},
      {{{2, 20}}},
      {{{3, 40}}},
      {{{4, std::nullopt}}},
  });
  auto rightPositions = serializeWithPositions(right);

  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{-1}, {0}, {1}, std::nullopt},
      MAP(BIGINT(), BIGINT()),
      false,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{1}, {0}, {1}, std::nullopt},
      MAP(BIGINT(), BIGINT()),
      true,
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);
  testCompareByteStreamWithNulls(
      leftPositions,
      rightPositions,
      {{1}, {0}, {1}, {0}},
      MAP(BIGINT(), BIGINT()),
      true,
      CompareFlags::NullHandlingMode::kNullAsValue);

  allocator_.clear();
}

TEST_F(ContainerRowSerdeTest, fuzzCompare) {
  VectorFuzzer::Options opts;
  opts.vectorSize = 1'000;
  opts.nullRatio = 0.5;
  opts.dictionaryHasNulls = true;

  VectorFuzzer fuzzer(opts, pool_.get());

  std::vector<vector_size_t> offsets(100);
  for (auto i = 0; i < offsets.size(); ++i) {
    offsets[i] = i * 10;
  }

  for (auto i = 0; i < 1'000; ++i) {
    auto seed = folly::Random::rand32();

    LOG(INFO) << i << ": seed: " << seed;

    fuzzer.reSeed(seed);

    {
      SCOPED_TRACE(fmt::format("seed: {}, ARRAY", seed));
      auto elements = fuzzer.fuzz(BIGINT());
      auto arrayVector = makeArrayVector(offsets, elements);
      testCompare(arrayVector);
    }

    {
      SCOPED_TRACE(fmt::format("seed: {}, MAP", seed));
      auto keys = fuzzer.fuzz(BIGINT());
      auto values = fuzzer.fuzz(BIGINT());
      auto mapVector = makeMapVector(offsets, keys, values);
      testCompare(mapVector);
    }

    {
      SCOPED_TRACE(fmt::format("seed: {}, ROW", seed));
      std::vector<VectorPtr> children{
          fuzzer.fuzz(BIGINT()),
          fuzzer.fuzz(BIGINT()),
      };
      auto rowVector = makeRowVector(children);
      testCompare(rowVector);
    }
  }
}

} // namespace
} // namespace facebook::velox::exec
