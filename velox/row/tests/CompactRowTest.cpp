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

#include "velox/row/CompactRow.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::test;

namespace facebook::velox::row {
namespace {

class CompactRowTest : public ::testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  /// TODO Replace with VectorFuzzer::fuzzInputRow once
  /// https://github.com/facebookincubator/velox/issues/6195 is fixed.
  RowVectorPtr fuzzInputRow(VectorFuzzer& fuzzer, const RowTypePtr& rowType) {
    const auto size = fuzzer.getOptions().vectorSize;
    std::vector<VectorPtr> children;
    for (auto i = 0; i < rowType->size(); ++i) {
      children.push_back(fuzzer.fuzz(rowType->childAt(i), size));
    }

    return std::make_shared<RowVector>(
        pool_.get(), rowType, nullptr, size, std::move(children));
  }

  void testRoundTrip(const RowVectorPtr& data) {
    SCOPED_TRACE(data->toString());

    auto rowType = asRowType(data->type());
    auto numRows = data->size();

    CompactRow row(data);

    size_t totalSize = 0;
    if (auto fixedRowSize = CompactRow::fixedRowSize(rowType)) {
      totalSize = fixedRowSize.value() * numRows;
    } else {
      for (auto i = 0; i < numRows; ++i) {
        totalSize += row.rowSize(i);
      }
    }

    std::vector<std::string_view> serialized;

    BufferPtr buffer = AlignedBuffer::allocate<char>(totalSize, pool(), 0);
    auto* rawBuffer = buffer->asMutable<char>();
    size_t offset = 0;
    for (auto i = 0; i < numRows; ++i) {
      auto size = row.serialize(i, rawBuffer + offset);
      serialized.push_back(std::string_view(rawBuffer + offset, size));
      offset += size;

      VELOX_CHECK_EQ(size, row.rowSize(i), "Row {}: {}", i, data->toString(i));
    }

    VELOX_CHECK_EQ(offset, totalSize);

    auto copy = CompactRow::deserialize(serialized, rowType, pool());
    assertEqualVectors(data, copy);
  }
};

TEST_F(CompactRowTest, fixedRowSize) {
  ASSERT_EQ(1 + 1, CompactRow::fixedRowSize(ROW({BOOLEAN()})));
  ASSERT_EQ(1 + 8, CompactRow::fixedRowSize(ROW({BIGINT()})));
  ASSERT_EQ(1 + 4, CompactRow::fixedRowSize(ROW({INTEGER()})));
  ASSERT_EQ(1 + 2, CompactRow::fixedRowSize(ROW({SMALLINT()})));
  ASSERT_EQ(1 + 8, CompactRow::fixedRowSize(ROW({DOUBLE()})));
  ASSERT_EQ(std::nullopt, CompactRow::fixedRowSize(ROW({VARCHAR()})));
  ASSERT_EQ(std::nullopt, CompactRow::fixedRowSize(ROW({ARRAY(BIGINT())})));
  ASSERT_EQ(
      1 + 1 + 8 + 4 + 2 + 8,
      CompactRow::fixedRowSize(
          ROW({BOOLEAN(), BIGINT(), INTEGER(), SMALLINT(), DOUBLE()})));

  ASSERT_EQ(std::nullopt, CompactRow::fixedRowSize(ROW({BIGINT(), VARCHAR()})));
  ASSERT_EQ(
      std::nullopt,
      CompactRow::fixedRowSize(ROW({BIGINT(), ROW({VARCHAR()})})));

  ASSERT_EQ(1, CompactRow::fixedRowSize(ROW({UNKNOWN()})));
}

TEST_F(CompactRowTest, rowSizeString) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({"a", "abc", "Longer string", "d", ""}),
  });

  CompactRow row(data);

  // 1 byte for null flags. 4 bytes for string size. N bytes for the string
  // itself.
  ASSERT_EQ(1 + 4 + 1, row.rowSize(0));
  ASSERT_EQ(1 + 4 + 3, row.rowSize(1));
  ASSERT_EQ(1 + 4 + 13, row.rowSize(2));
  ASSERT_EQ(1 + 4 + 1, row.rowSize(3));
  ASSERT_EQ(1 + 4 + 0, row.rowSize(4));
}

TEST_F(CompactRowTest, rowSizeArrayOfBigint) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {},
          {6},
      }),
  });

  {
    CompactRow row(data);

    // 1 byte for null flags. 4 bytes for array
    // size. 1 byte for null flags for elements. N bytes for array elements.
    ASSERT_EQ(1 + 4 + 1 + 8 * 3, row.rowSize(0));
    ASSERT_EQ(1 + 4 + 1 + 8 * 2, row.rowSize(1));
    ASSERT_EQ(1 + 4, row.rowSize(2));
    ASSERT_EQ(1 + 4 + 1 + 8, row.rowSize(3));
  }

  data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {{1, 2, std::nullopt, 3}},
          {{4, 5}},
          {{}},
          std::nullopt,
          {{6}},
      }),
  });

  {
    CompactRow row(data);

    // 1 byte for null flags. 4 bytes for array
    // size. 1 byte for null flags for elements. N bytes for array elements.
    ASSERT_EQ(1 + 4 + 1 + 8 * 4, row.rowSize(0));
    ASSERT_EQ(1 + 4 + 1 + 8 * 2, row.rowSize(1));
    ASSERT_EQ(1 + 4, row.rowSize(2));
    ASSERT_EQ(1, row.rowSize(3));
    ASSERT_EQ(1 + 4 + 1 + 8, row.rowSize(4));
  }
}

TEST_F(CompactRowTest, rowSizeMixed) {
  auto data = makeRowVector({
      makeNullableFlatVector<int64_t>({1, 2, 3, std::nullopt}),
      makeNullableFlatVector<std::string>({"a", "abc", "", std::nullopt}),
  });

  CompactRow row(data);

  // 1 byte for null flags. 8 bytes for bigint field. 4 bytes for string size.
  // N bytes for the string itself.
  ASSERT_EQ(1 + 8 + (4 + 1), row.rowSize(0));
  ASSERT_EQ(1 + 8 + (4 + 3), row.rowSize(1));
  ASSERT_EQ(1 + 8 + (4 + 0), row.rowSize(2));
  ASSERT_EQ(1 + 8, row.rowSize(3));
}

TEST_F(CompactRowTest, rowSizeArrayOfStrings) {
  auto data = makeRowVector({
      makeArrayVector<std::string>({
          {"a", "Abc"},
          {},
          {"a", "Longer string", "abc"},
      }),
  });

  {
    CompactRow row(data);

    // 1 byte for null flags. 4 bytes for array
    // size. 1 byte for nulls flags for elements. 4 bytes for serialized size. 4
    // bytes per offset of an element. N bytes for elements. Each string element
    // is 4 bytes for size + string length.
    ASSERT_EQ(1 + 4 + 1 + (4 + 1) + (4 + 3), row.rowSize(0));
    ASSERT_EQ(1 + 4, row.rowSize(1));
    ASSERT_EQ(1 + 4 + 1 + (4 + 1) + (4 + 13) + (4 + 3), row.rowSize(2));
  }

  data = makeRowVector({
      makeNullableArrayVector<std::string>({
          {{"a", "Abc", std::nullopt}},
          {{}},
          std::nullopt,
          {{"a", std::nullopt, "Longer string", "abc"}},
      }),
  });

  {
    CompactRow row(data);

    // Null strings do not take space.
    ASSERT_EQ(1 + 4 + 1 + (4 + 1) + (4 + 3) + 0, row.rowSize(0));
    ASSERT_EQ(1 + 4, row.rowSize(1));
    ASSERT_EQ(1, row.rowSize(2));
    ASSERT_EQ(1 + 4 + 1 + (4 + 1) + 0 + (4 + 13) + (4 + 3), row.rowSize(3));
  }
}

TEST_F(CompactRowTest, boolean) {
  auto data = makeRowVector({
      makeFlatVector<bool>(
          {true, false, true, true, false, false, true, false}),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableFlatVector<bool>({
          true,
          false,
          std::nullopt,
          true,
          std::nullopt,
          false,
          true,
          false,
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, bigint) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableFlatVector<int64_t>(
          {1, std::nullopt, 3, std::nullopt, 5, std::nullopt}),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, hugeint) {
  auto data = makeRowVector({
      makeFlatVector<int128_t>({1, 2, 3, 4, 5}),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableFlatVector<int128_t>(
          {std::nullopt, 1, 2, std::nullopt, std::nullopt, 3, 4, 5}),
  });

  testRoundTrip(data);
}

Timestamp ts(int64_t micros) {
  return Timestamp::fromMicros(micros);
}

TEST_F(CompactRowTest, timestamp) {
  auto data = makeRowVector({
      makeFlatVector<Timestamp>({
          ts(0),
          ts(1),
          ts(2),
      }),
  });

  testRoundTrip(data);

  // Serialize null Timestamp values with null flags set over a large
  // non-serializable value (e.g. a value that triggers an exception in
  // Timestamp::toMicros()).
  data = makeRowVector({
      makeFlatVector<Timestamp>({
          ts(0),
          Timestamp::max(),
          ts(123'456),
          Timestamp::min(),
      }),
  });

  data->childAt(0)->setNull(1, true);
  data->childAt(0)->setNull(3, true);

  testRoundTrip(data);
}

TEST_F(CompactRowTest, string) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({"a", "Abc", "", "Longer test string"}),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, unknown) {
  auto data = makeRowVector({
      makeAllNullFlatVector<UnknownValue>(10),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeArrayVector({0, 3, 5, 9}, makeAllNullFlatVector<UnknownValue>(10)),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, mix) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({"a", "Abc", "", "Longer test string"}),
      makeAllNullFlatVector<UnknownValue>(4),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, arrayOfBigint) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6},
          {},
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {{1, 2, std::nullopt, 3}},
          {{4, 5, std::nullopt}},
          {{std::nullopt, 6}},
          {{std::nullopt}},
          std::nullopt,
          {{}},
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, arrayOfTimestamp) {
  auto data = makeRowVector({
      makeArrayVector<Timestamp>({
          {ts(1), ts(2), ts(3)},
          {ts(4), ts(5)},
          {ts(6)},
          {},
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableArrayVector<Timestamp>({
          {{ts(1), ts(2), std::nullopt, ts(3)}},
          {{ts(4), ts(5), std::nullopt}},
          {{std::nullopt, ts(6)}},
          {{std::nullopt}},
          std::nullopt,
          {{}},
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, arrayOfString) {
  auto data = makeRowVector({
      makeArrayVector<std::string>({
          {"a", "abc", "Longer test string"},
          {"b", "Abc 12345 ...test", "foo"},
          {},
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeNullableArrayVector<std::string>({
          {{"a", std::nullopt, "abc", "Longer test string"}},
          {{std::nullopt,
            "b",
            std::nullopt,
            "Abc 12345 ...test",
            std::nullopt,
            "foo"}},
          {{}},
          {{std::nullopt}},
          std::nullopt,
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, map) {
  auto data = makeRowVector({
      makeMapVector<int16_t, int64_t>(
          {{{1, 10}, {2, 20}, {3, 30}}, {{1, 11}, {2, 22}}, {{4, 444}}, {}}),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeMapVector<std::string, std::string>({
          {{"a", "100"},
           {"b", "200"},
           {"Long string for testing", "Another long string"}},
          {{"abc", "300"}, {"d", "400"}},
          {},
      }),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, row) {
  auto data = makeRowVector({
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeFlatVector<double>({1.05, 2.05, 3.05, 4.05, 5.05}),
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeFlatVector<std::string>(
              {"a", "Abc", "Long test string", "", "d"}),
          makeFlatVector<double>({1.05, 2.05, 3.05, 4.05, 5.05}),
      }),
  });

  testRoundTrip(data);

  data = makeRowVector({
      makeRowVector(
          {
              makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
              makeNullableFlatVector<int64_t>({-1, 2, -3, std::nullopt, -5}),
              makeFlatVector<double>({1.05, 2.05, 3.05, 4.05, 5.05}),
              makeFlatVector<std::string>(
                  {"a", "Abc", "Long test string", "", "d"}),
          },
          nullEvery(2)),
  });

  testRoundTrip(data);
}

TEST_F(CompactRowTest, fuzz) {
  auto rowType = ROW({
      ROW({BIGINT(), VARCHAR(), DOUBLE()}),
      MAP(VARCHAR(), ROW({ARRAY(BIGINT()), ARRAY(VARCHAR()), REAL()})),
      ARRAY(ROW({BIGINT(), DOUBLE()})),
      ARRAY(MAP(BIGINT(), DOUBLE())),
      BIGINT(),
      ARRAY(MAP(BIGINT(), VARCHAR())),
      ARRAY(MAP(VARCHAR(), REAL())),
      MAP(BIGINT(), ARRAY(BIGINT())),
      BIGINT(),
      ARRAY(BIGINT()),
      DOUBLE(),
      MAP(VARCHAR(), VARCHAR()),
      VARCHAR(),
      ARRAY(ARRAY(BIGINT())),
      BIGINT(),
      ARRAY(ARRAY(VARCHAR())),
  });

  VectorFuzzer::Options opts;
  opts.vectorSize = 100;
  opts.containerLength = 5;
  opts.nullRatio = 0.1;
  opts.dictionaryHasNulls = false;
  opts.stringVariableLength = true;
  opts.stringLength = 20;
  opts.containerVariableLength = true;
  opts.complexElementsMaxSize = 1'000;

  // Spark uses microseconds to store timestamp
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;

  VectorFuzzer fuzzer(opts, pool_.get());

  const auto iterations = 200;
  for (size_t i = 0; i < iterations; ++i) {
    auto seed = folly::Random::rand32();

    LOG(INFO) << i << ": seed: " << seed;
    SCOPED_TRACE(fmt::format("seed: {}", seed));

    fuzzer.reSeed(seed);
    auto data = fuzzInputRow(fuzzer, rowType);

    testRoundTrip(data);

    if (Test::HasFailure()) {
      break;
    }
  }
}

} // namespace
} // namespace facebook::velox::row
