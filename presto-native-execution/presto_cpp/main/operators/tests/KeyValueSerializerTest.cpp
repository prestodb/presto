/*
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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "presto_cpp/main/operators/KeyValueSerializer.h"

namespace facebook::presto::operators::test {
namespace {

class KeyValueSerializerTest : public ::testing::Test,
                                    public velox::test::VectorTestBase {
 public:
  // return -1 if key1 < key2, 0 if key1 == key2, 1 if key1 > key 2
  int lexicographicalCompare(
      velox::ByteOutputStream* key1,
      velox::ByteOutputStream* key2) {
    std::stringstream str1;
    std::stringstream str2;
    velox::OStreamOutputStream output1(&str1);
    velox::OStreamOutputStream output2(&str2);
    key1->flush(&output1);
    key2->flush(&output2);
    std::string res1 = str1.str();
    std::string res2 = str2.str();

    // doing unsinged byte comparison following the Cosco test suite's semantic.
    auto begin1 = reinterpret_cast<unsigned char*>(res1.data());
    auto end1 = begin1 + res1.size();
    auto begin2 = reinterpret_cast<unsigned char*>(res2.data());
    auto end2 = begin2 + res2.size();
    bool lessThan = std::lexicographical_compare(begin1, end1, begin2, end2);

    bool equal = std::equal(begin1, end1, begin2, end2);

    return lessThan ? -1 : (equal ? 0 : 1);
  }

  int lexicographicalCompareRawBuffer(RawBuffer* key1, RawBuffer* key2) {
    // doing unsinged byte comparison following the Cosco test suite's semantic.
    auto begin1 = reinterpret_cast<unsigned char*>(key1->data());
    auto end1 = begin1 + key1->position();
    auto begin2 = reinterpret_cast<unsigned char*>(key2->data());
    auto end2 = begin2 + key2->position();
    bool lessThan = std::lexicographical_compare(begin1, end1, begin2, end2);

    bool equal = std::equal(begin1, end1, begin2, end2);

    return lessThan ? -1 : (equal ? 0 : 1);
  }

  void serializeRowUnsafe(
      KeyValueSerializer keyValueSerializer,
      size_t index,
      RawBuffer* out) {
    keyValueSerializer.serializeUnsafe(/*index=*/index, out);
  }

  void serializeRow(
      KeyValueSerializer keyValueSerializer,
      size_t index,
      velox::ByteOutputStream* out) {
    // allocator_.newWrite(out);
    out->startWrite(100);
    keyValueSerializer.serialize(/*index=*/index, out);
    // allocator_.finishWrite(out, 0);
  }

  int compareRowVector(
      const velox::RowVectorPtr& rowVector,
      const std::vector<
          std::shared_ptr<const velox::core::FieldAccessTypedExpr>>& fields,
      const std::vector<velox::core::SortOrder>& ordering) {
    VELOX_CHECK_EQ(rowVector->size(), 2);
    KeyValueSerializer keyValueSerializer(rowVector, ordering, fields);
    velox::ByteOutputStream key1(streamArena_.get(), false, false);
    serializeRow(keyValueSerializer, /*index=*/0, &key1);
    velox::ByteOutputStream key2(streamArena_.get(), false, false);
    serializeRow(keyValueSerializer, /*index=*/1, &key2);

    std::string str1;
    std::string str2;
    str1.resize(100);
    str2.resize(100);
    RawBuffer buffer1(reinterpret_cast<char*>(&str1[0]), str1.size());
    RawBuffer buffer2(reinterpret_cast<char*>(&str2[0]), str2.size());
    serializeRowUnsafe(keyValueSerializer, /*index=*/0, &buffer1);
    serializeRowUnsafe(keyValueSerializer, /*index=*/1, &buffer2);

    // test velox::core::ByteStream as buffer;
    int result1 = lexicographicalCompare(&key1, &key2);
    // test rawBuffer as buffer;
    int result2 = lexicographicalCompareRawBuffer(&buffer1, &buffer2);
    EXPECT_EQ(result1, result2);

    return result1;
  }

  template <typename T>
  int singlePrimitiveFieldCompare(
      const std::vector<std::optional<T>>& values,
      const velox::core::SortOrder& ordering) {
    VELOX_CHECK_EQ(values.size(), 2);
    auto c0 = vectorMaker_.flatVectorNullable<T>(values);
    auto rowVector = vectorMaker_.rowVector({c0});

    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::CppToType<T>::create(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

  template <typename T>
  int singleArrayFieldCompare(
      const std::vector<std::optional<std::vector<std::optional<T>>>>& values,
      const velox::core::SortOrder& ordering) {
    VELOX_CHECK_EQ(values.size(), 2);
    auto c0 = makeNullableArrayVector<T>(values);
    // auto c0 = makeNullableArrayVector<T>(values);
    auto rowVector = vectorMaker_.rowVector({c0});

    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(std::make_shared<velox::core::FieldAccessTypedExpr>(
        velox::CppToType<T>::create(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

  template <typename T0, typename T1>
  int singleRowFieldCompare(
      const std::vector<std::optional<T0>>& col0,
      const std::vector<std::optional<T1>>& col1,
      const velox::core::SortOrder& ordering,
      std::function<bool(velox::vector_size_t /* row */)> isNullAt = nullptr) {
    VELOX_CHECK_EQ(col0.size(), 2);
    VELOX_CHECK_EQ(col1.size(), 2);
    auto c0 = vectorMaker_.flatVectorNullable<T0>(col0);
    auto c1 = vectorMaker_.flatVectorNullable<T1>(col1);
    auto rowField = vectorMaker_.rowVector({c0, c1});
    auto rowVector = vectorMaker_.rowVector({rowField});

    if (isNullAt) {
      rowVector->childAt(0)->setNull(0, isNullAt(0));
      rowVector->childAt(0)->setNull(1, isNullAt(1));
    }
    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        rowField->type(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_ =
      velox::memory::deprecatedAddDefaultLeafMemoryPool();
  velox::test::VectorMaker vectorMaker_{pool_.get()};
  std::unique_ptr<velox::StreamArena> streamArena_ =
      std::make_unique<velox::StreamArena>(pool_.get());
};
} // namespace

TEST_F(KeyValueSerializerTest, LongTypeAllFields) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  KeyValueSerializer keyValueSerializer(
      rowVector, ordering, std::move(fields));

  velox::ByteOutputStream key1(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/0, &key1);
  velox::ByteOutputStream key2(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/1, &key2);

  // (0,1,2) < (2,1,0)
  EXPECT_TRUE(lexicographicalCompare(&key1, &key2) < 0);
}

TEST_F(KeyValueSerializerTest, LongTypeAllFieldsDescending) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  KeyValueSerializer keyValueSerializer(
      rowVector, ordering, std::move(fields));

  velox::ByteOutputStream key1(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/0, &key1);
  velox::ByteOutputStream key2(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/1, &key2);

  // (0d,1,2) > (2d,1,0)
  EXPECT_TRUE(lexicographicalCompare(&key1, &key2) > 0);
}

TEST_F(KeyValueSerializerTest, LongTypeAllFieldsWithNulls) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  KeyValueSerializer keyValueSerializer(
      rowVector, ordering, std::move(fields));

  velox::ByteOutputStream key1(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/0, &key1);
  velox::ByteOutputStream key2(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/1, &key2);

  // (null,null,2) > (null,null,0)
  EXPECT_TRUE(lexicographicalCompare(&key1, &key2) > 0);
}

TEST_F(KeyValueSerializerTest, DefaultNullsOrdering) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  KeyValueSerializer keyValueSerializer(
      rowVector, ordering, std::move(fields));

  velox::ByteOutputStream key1(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/0, &key1);
  velox::ByteOutputStream key2(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/1, &key2);

  // (null,null,0) > (null,null,null)
  EXPECT_TRUE(lexicographicalCompare(&key1, &key2) > 0);
}

TEST_F(
    KeyValueSerializerTest,
    ExplicitNullsOrdering_ascending_nulls_last) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsLast)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  KeyValueSerializer keyValueSerializer(
      rowVector, ordering, std::move(fields));

  velox::ByteOutputStream key1(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/0, &key1);
  velox::ByteOutputStream key2(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/1, &key2);

  // (null,null,0) < (null,null,null)
  EXPECT_TRUE(lexicographicalCompare(&key1, &key2) < 0);
}

TEST_F(
    KeyValueSerializerTest,
    ExplicitNullsOrdering_descending_nulls_last) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kDescNullsLast)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  KeyValueSerializer keyValueSerializer(
      rowVector, ordering, std::move(fields));

  velox::ByteOutputStream key1(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/0, &key1);
  velox::ByteOutputStream key2(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/1, &key2);

  // (null,null,0) < (null,null,null)
  EXPECT_TRUE(lexicographicalCompare(&key1, &key2) < 0);
}

TEST_F(KeyValueSerializerTest, LongTypeSubsetOfFields) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
      velox::BIGINT(), /*name=*/"c2"));
  fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
      velox::BIGINT(), /*name=*/"c0"));

  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  KeyValueSerializer keyValueSerializer(
      rowVector, ordering, std::move(fields));

  velox::ByteOutputStream key1(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/0, &key1);
  velox::ByteOutputStream key2(streamArena_.get(), false, false);
  serializeRow(keyValueSerializer, /*index=*/1, &key2);

  // (2, 0) > (0, 2)
  EXPECT_TRUE(lexicographicalCompare(&key1, &key2) > 0);
}

TEST_F(KeyValueSerializerTest, BooleanTypeSingleFieldTests) {
  // true == true
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {true, true}, velox::core::kAscNullsFirst) == 0);

  // false < true
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {false, true}, velox::core::kAscNullsFirst) < 0);

  // true < false (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {true, false}, velox::core::kDescNullsLast) < 0);

  // null < false
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {std::nullopt, false}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, DoubleTypeSingleFieldTests) {
  // 0.1 == 0.1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.1, 0.1}, velox::core::kAscNullsFirst) == 0);

  // 0.01 < 0.1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.01, 0.1}, velox::core::kAscNullsFirst) < 0);

  // 0.1 < 0.01 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.1, 0.01}, velox::core::kDescNullsLast) < 0);

  // Double.NaN == Double.NaN
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::numeric_limits<double>::quiet_NaN(),
           std::numeric_limits<double>::quiet_NaN()},
          velox::core::kAscNullsFirst) == 0);

  // null < Double.NaN
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::nullopt, std::numeric_limits<double>::quiet_NaN()},
          velox::core::kAscNullsFirst) < 0);

  // null < Double.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::nullopt, std::numeric_limits<double>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, FloatTypeSingleFieldTests) {
  // 0.1 == 0.1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.1f, 0.1f}, velox::core::kAscNullsFirst) == 0);

  // 0.01 < 0.1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.01f, 0.1f}, velox::core::kAscNullsFirst) < 0);

  // 0.1 < 0.01 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.1f, 0.01f}, velox::core::kDescNullsLast) < 0);

  // Float.NaN == Float.NaN
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::numeric_limits<float>::quiet_NaN(),
           std::numeric_limits<float>::quiet_NaN()},
          velox::core::kAscNullsFirst) == 0);

  // null < Float.NaN
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::nullopt, std::numeric_limits<float>::quiet_NaN()},
          velox::core::kAscNullsFirst) < 0);

  // null < Float.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::nullopt, std::numeric_limits<float>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, ByteTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>({1, 2}, velox::core::kAscNullsFirst) <
      0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>({2, 1}, velox::core::kDescNullsLast) <
      0);

  // Byte.MinValue < Byte.MaxValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::numeric_limits<int8_t>::min(),
           std::numeric_limits<int8_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  // Byte.MaxValue < Byte.MinValue (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::numeric_limits<int8_t>::min(),
           std::numeric_limits<int8_t>::max()},
          velox::core::kDescNullsLast) > 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  // null < Byte.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::nullopt, std::numeric_limits<int8_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, ShortTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  // Short.MinValue < Short.MaxValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::numeric_limits<int16_t>::min(),
           std::numeric_limits<int16_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  // Short.MaxValue < Short.MinValue (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::numeric_limits<int16_t>::min(),
           std::numeric_limits<int16_t>::max()},
          velox::core::kDescNullsLast) > 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  // null < Short.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::nullopt, std::numeric_limits<int16_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, IntegerTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  // Int.MinValue < Int.MaxValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::numeric_limits<int32_t>::min(),
           std::numeric_limits<int32_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  // Int.MaxValue < Int.MinValue (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::numeric_limits<int32_t>::min(),
           std::numeric_limits<int32_t>::max()},
          velox::core::kDescNullsLast) > 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  // null < Int.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, std::numeric_limits<int32_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, DateTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, LongTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  // Long.MinValue < Long.MaxValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::numeric_limits<int64_t>::min(),
           std::numeric_limits<int64_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  // Long.MaxValue < Long.MinValue (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::numeric_limits<int64_t>::min(),
           std::numeric_limits<int64_t>::max()},
          velox::core::kDescNullsLast) > 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  // null < Long.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::nullopt, std::numeric_limits<int64_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, TimestampTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(1), velox::Timestamp::fromMicros(1)},
          velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(1), velox::Timestamp::fromMicros(2)},
          velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(2), velox::Timestamp::fromMicros(1)},
          velox::core::kDescNullsLast) < 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {std::nullopt, velox::Timestamp::fromMicros(1)},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, StringTypeSingleFieldTests) {
  // "abc" == "abc"
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"abc", "abc"}, velox::core::kAscNullsFirst) == 0);

  // "aaa" < "abc"
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"aaa", "abc"}, velox::core::kAscNullsFirst) < 0);

  // "aaa" < "aaaa"
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"aaa", "aaaa"}, velox::core::kAscNullsFirst) < 0);

  // "abc" < "aaa" (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"abc", "aaa"}, velox::core::kDescNullsLast) < 0);

  // null < ""
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {std::nullopt, ""}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, ArrayTypeSingleFieldTests) {
  // [1, 1] == [1, 1]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L}}, {{1L, 1L}}}, velox::core::kAscNullsFirst) == 0);

  // [1, 1] < [1, 1, 1]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L}}, {{1L, 1L, 1L}}}, velox::core::kAscNullsFirst) < 0);

  // [1, 1, 1] < [1, 1] (descending)
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L, 1L}}, {{1L, 1L}}}, velox::core::kDescNullsLast) < 0);

  // [1, 1, 1] < [1, 2]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L, 1L}}, {{1L, 2L}}}, velox::core::kAscNullsFirst) < 0);

  //[ 1, null, 1 ] < [ 1, 0, 1 ]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, std::nullopt, 1L}}, {{1L, 0L, 1L}}},
          velox::core::kAscNullsFirst) < 0);

  // null < [1]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, {{1L}}}, velox::core::kAscNullsFirst) < 0);

  // null < [null]
  std::vector<std::optional<int64_t>> arrayWithNull{std::nullopt};
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, arrayWithNull}, velox::core::kAscNullsFirst) < 0);

  // null < []
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, {{}}}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(KeyValueSerializerTest, RowTypeSingleFieldTests) {
  // (1, "aaa") == (1, "aaa")
  int cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "aaa"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp == 0);

  // (1, "aaa") < (1, "abc")
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "abc"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp < 0);

  // (1, "abc") < (1, "aaa") (descending)
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "abc"}, velox::core::kDescNullsLast);
  EXPECT_TRUE(cmp > 0);

  // (1, null) < (1, "abc")
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {std::nullopt, "abc"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp < 0);

  // null < (null, null)
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt},
      velox::core::kAscNullsFirst,
      [](velox::vector_size_t row) { return row == 0 ? true : false; });
  EXPECT_TRUE(cmp < 0);
}
} // namespace facebook::spark::test
