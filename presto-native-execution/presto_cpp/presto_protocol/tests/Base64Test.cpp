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
#include <gtest/gtest.h>

#include "presto_cpp/presto_protocol/Base64Util.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::presto::protocol;
using namespace facebook::velox;

class Base64Test : public ::testing::Test {
 public:
  void SetUp() override {
    pool_ = memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(Base64Test, singleInt) {
  std::string data = "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==";

  auto vector = readBlock(INTEGER(), data, pool_.get());

  ASSERT_EQ(TypeKind::INTEGER, vector->typeKind());
  ASSERT_EQ(1, vector->size());
  ASSERT_FALSE(vector->isNullAt(0));

  auto intVector = vector->as<SimpleVector<int32_t>>();
  ASSERT_EQ(1, intVector->valueAt(0));
}

TEST_F(Base64Test, singleShort) {
  std::string data = "CwAAAFNIT1JUX0FSUkFZAQAAAAEAAwA=";

  auto vector = readBlock(SMALLINT(), data, pool_.get());

  ASSERT_EQ(TypeKind::SMALLINT, vector->typeKind());
  ASSERT_EQ(1, vector->size());
  ASSERT_FALSE(vector->isNullAt(0));

  auto intVector = vector->as<SimpleVector<int16_t>>();
  ASSERT_EQ(3, intVector->valueAt(0));
}

TEST_F(Base64Test, singleLong) {
  std::string data = "CgAAAExPTkdfQVJSQVkBAAAAAAMAAAAAAAAA";

  auto vector = readBlock(BIGINT(), data, pool_.get());

  ASSERT_EQ(TypeKind::BIGINT, vector->typeKind());
  ASSERT_EQ(1, vector->size());
  ASSERT_FALSE(vector->isNullAt(0));

  auto intVector = vector->as<SimpleVector<int64_t>>();
  ASSERT_EQ(3, intVector->valueAt(0));
}

TEST_F(Base64Test, singleTinyint) {
  std::string data = "CgAAAEJZVEVfQVJSQVkBAAAAAAE=";

  auto vector = readBlock(TINYINT(), data, pool_.get());

  ASSERT_EQ(TypeKind::TINYINT, vector->typeKind());
  ASSERT_EQ(1, vector->size());
  ASSERT_FALSE(vector->isNullAt(0));

  auto intVector = vector->as<SimpleVector<int8_t>>();
  ASSERT_EQ(1, intVector->valueAt(0));
}

TEST_F(Base64Test, simpleLongDecimal) {
  // Note: string values of block representations in following test cases (e.g.,
  // "data0") can be obtained by reading the actual block representations of
  // corresponding unscaled values in running services of Prestissimo.

  // Unscaled value = 0
  const std::string data0 =
      "DAAAAElOVDEyOF9BUlJBWQEAAAAAAAAAAAAAAAAAAAAAAAAAAA==";
  auto vector0 = readBlock(DECIMAL(24, 2), data0, pool_.get());

  ASSERT_EQ(TypeKind::HUGEINT, vector0->typeKind());
  ASSERT_EQ(1, vector0->size());
  ASSERT_FALSE(vector0->isNullAt(0));

  auto decimalVector0 = vector0->as<FlatVector<int128_t>>();
  ASSERT_EQ(0, decimalVector0->valueAt(0));

  // Unscaled value = 100
  const std::string data1 =
      "DAAAAElOVDEyOF9BUlJBWQEAAAAAZAAAAAAAAAAAAAAAAAAAAA==";
  auto vector1 = readBlock(DECIMAL(24, 2), data1, pool_.get());

  ASSERT_EQ(TypeKind::HUGEINT, vector1->typeKind());
  ASSERT_EQ(1, vector1->size());
  ASSERT_FALSE(vector1->isNullAt(0));

  auto decimalVector1 = vector1->as<FlatVector<int128_t>>();
  ASSERT_EQ(100, decimalVector1->valueAt(0));

  // Unscaled value = -100
  const std::string data2 =
      "DAAAAElOVDEyOF9BUlJBWQEAAAAAZAAAAAAAAAAAAAAAAAAAgA==";
  auto vector2 = readBlock(DECIMAL(24, 2), data2, pool_.get());

  ASSERT_EQ(TypeKind::HUGEINT, vector2->typeKind());
  ASSERT_EQ(1, vector2->size());
  ASSERT_FALSE(vector2->isNullAt(0));

  auto decimalVector2 = vector2->as<FlatVector<int128_t>>();
  ASSERT_EQ(-100, decimalVector2->valueAt(0));

  // Unscaled value = 10^20
  const std::string data3 =
      "DAAAAElOVDEyOF9BUlJBWQEAAAAAAAAQYy1ex2sFAAAAAAAAAA==";
  auto vector3 = readBlock(DECIMAL(24, 2), data3, pool_.get());

  ASSERT_EQ(TypeKind::HUGEINT, vector3->typeKind());
  ASSERT_EQ(1, vector3->size());
  ASSERT_FALSE(vector3->isNullAt(0));

  auto decimalVector3 = vector3->as<FlatVector<int128_t>>();
  ASSERT_EQ(DecimalUtil::kPowersOfTen[20], decimalVector3->valueAt(0));

  // Unscaled value = -10^20
  const std::string data4 =
      "DAAAAElOVDEyOF9BUlJBWQEAAAAAAAAQYy1ex2sFAAAAAAAAgA==";
  auto vector4 = readBlock(DECIMAL(24, 2), data4, pool_.get());

  ASSERT_EQ(TypeKind::HUGEINT, vector4->typeKind());
  ASSERT_EQ(1, vector4->size());
  ASSERT_FALSE(vector4->isNullAt(0));

  auto decimalVector4 = vector4->as<FlatVector<int128_t>>();
  ASSERT_EQ(-DecimalUtil::kPowersOfTen[20], decimalVector4->valueAt(0));
}

TEST_F(Base64Test, singleString) {
  std::string data = "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAoAAAAACgAAADIwMTktMTEtMTA=";

  auto vector = readBlock(VARCHAR(), data, pool_.get());

  ASSERT_EQ(TypeKind::VARCHAR, vector->typeKind());
  ASSERT_EQ(1, vector->size());
  ASSERT_FALSE(vector->isNullAt(0));

  auto stringVector = vector->as<SimpleVector<StringView>>();
  ASSERT_EQ("2019-11-10", stringVector->valueAt(0).str());
}

TEST_F(Base64Test, singleNull) {
  std::string data = "AwAAAFJMRQEAAAAKAAAAQllURV9BUlJBWQEAAAABgA==";

  auto vector = readBlock(BIGINT(), data, pool_.get());

  ASSERT_EQ(TypeKind::BIGINT, vector->typeKind());
  ASSERT_EQ(1, vector->size());
  ASSERT_TRUE(vector->isNullAt(0));
}

TEST_F(Base64Test, arrayOfNulls) {
  const std::string data =
      "BQAAAEFSUkFZAwAAAFJMRQQAAAAKAAAAQllURV9BUlJBWQEAAAABgAEAAAAAAAAABAAAAAA=";
  auto vector = readBlock(ARRAY(BIGINT()), data, pool_.get());

  ASSERT_EQ(VectorEncoding::Simple::ARRAY, vector->encoding());
  ASSERT_EQ(1, vector->size());

  auto arrayVector = vector->as<ArrayVector>();
  ASSERT_EQ(TypeKind::BIGINT, arrayVector->elements()->typeKind());

  auto elementsVector = arrayVector->elements();
  ASSERT_EQ(4, elementsVector->size());
  for (auto i = 0; i < 4; i++) {
    ASSERT_TRUE(elementsVector->isNullAt(i));
  }
}

TEST_F(Base64Test, arrayOfInts) {
  const std::string data =
      "BQAAAEFSUkFZCQAAAElOVF9BUlJBWQMAAAAAAQAAAAIAAAADAAAAAQAAAAAAAAADAAAAAA==";

  auto vector = readBlock(ARRAY(INTEGER()), data, pool_.get());

  ASSERT_EQ(VectorEncoding::Simple::ARRAY, vector->encoding());
  ASSERT_EQ(1, vector->size());

  ASSERT_FALSE(vector->isNullAt(0));

  auto arrayVector = vector->as<ArrayVector>();
  ASSERT_EQ(0, arrayVector->offsetAt(0));
  ASSERT_EQ(3, arrayVector->sizeAt(0));

  auto elementsVector = arrayVector->elements()->as<SimpleVector<int32_t>>();
  ASSERT_EQ(1, elementsVector->valueAt(0));
  ASSERT_EQ(2, elementsVector->valueAt(1));
  ASSERT_EQ(3, elementsVector->valueAt(2));
}

TEST_F(Base64Test, arrayOfIntsWithNulls) {
  const std::string data =
      "BQAAAEFSUkFZCQAAAElOVF9BUlJBWQQAAAABIAEAAAAXAAAAyAEAAAEAAAAAAAAABAAAAAA=";

  auto vector = readBlock(ARRAY(INTEGER()), data, pool_.get());

  ASSERT_EQ(VectorEncoding::Simple::ARRAY, vector->encoding());
  ASSERT_EQ(1, vector->size());

  ASSERT_FALSE(vector->isNullAt(0));

  auto arrayVector = vector->as<ArrayVector>();
  ASSERT_EQ(0, arrayVector->offsetAt(0));
  ASSERT_EQ(4, arrayVector->sizeAt(0));

  auto elementsVector = arrayVector->elements()->as<SimpleVector<int32_t>>();
  ASSERT_EQ(1, elementsVector->valueAt(0));
  ASSERT_EQ(23, elementsVector->valueAt(1));
  ASSERT_TRUE(elementsVector->isNullAt(2));
  ASSERT_EQ(456, elementsVector->valueAt(3));
}

TEST_F(Base64Test, arrayOfStrings) {
  const std::string data =
      "BQAAAEFSUkFZDgAAAFZBUklBQkxFX1dJRFRIAwAAAAEAAAACAAAAAwAAAAADAAAAYWJjAQAAAAAAAAADAAAAAA==";

  auto vector = readBlock(ARRAY(VARCHAR()), data, pool_.get());

  ASSERT_EQ(VectorEncoding::Simple::ARRAY, vector->encoding());
  ASSERT_EQ(1, vector->size());

  ASSERT_FALSE(vector->isNullAt(0));

  auto arrayVector = vector->as<ArrayVector>();
  ASSERT_EQ(0, arrayVector->offsetAt(0));
  ASSERT_EQ(3, arrayVector->sizeAt(0));

  auto elementsVector = arrayVector->elements()->as<SimpleVector<StringView>>();
  ASSERT_EQ("a", elementsVector->valueAt(0).str());
  ASSERT_EQ("b", elementsVector->valueAt(1).str());
  ASSERT_EQ("c", elementsVector->valueAt(2).str());
}

TEST_F(Base64Test, arrayOfStringsWithNulls) {
  const std::string data =
      "BQAAAEFSUkFZDgAAAFZBUklBQkxFX1dJRFRIBAAAAAUAAAAFAAAACwAAABEAAAABQBEAAABhcHBsZWJhbmFuYWNhcnJvdAEAAAAAAAAABAAAAAA=";

  auto vector = readBlock(ARRAY(VARCHAR()), data, pool_.get());

  ASSERT_EQ(VectorEncoding::Simple::ARRAY, vector->encoding());
  ASSERT_EQ(1, vector->size());

  ASSERT_FALSE(vector->isNullAt(0));

  auto arrayVector = vector->as<ArrayVector>();
  ASSERT_EQ(0, arrayVector->offsetAt(0));
  ASSERT_EQ(4, arrayVector->sizeAt(0));

  auto elementsVector = arrayVector->elements()->as<SimpleVector<StringView>>();
  ASSERT_EQ("apple", elementsVector->valueAt(0).str());
  ASSERT_TRUE(elementsVector->isNullAt(1));
  ASSERT_EQ("banana", elementsVector->valueAt(2).str());
  ASSERT_EQ("carrot", elementsVector->valueAt(3).str());
}

TEST_F(Base64Test, arrayOfArrayOfInts) {
  const std::string data =
      "BQAAAEFSUkFZBQAAAEFSUkFZCQAAAElOVF9BUlJBWQYAAAAAAQAAAAIAAAADAAAABAAAAAUAAAAGAAAAAwAAAAAAAAACAAAABAAAAAYAAAAAAQAAAAAAAAADAAAAAA==";

  auto vector = readBlock(ARRAY(ARRAY(INTEGER())), data, pool_.get());

  ASSERT_EQ(VectorEncoding::Simple::ARRAY, vector->encoding());
  ASSERT_EQ(1, vector->size());

  auto arrayVector = vector->as<ArrayVector>();
  ASSERT_EQ(0, arrayVector->offsetAt(0));
  ASSERT_EQ(3, arrayVector->sizeAt(0));

  auto innerArrayVector = arrayVector->elements()->as<ArrayVector>();
  ASSERT_EQ(0, innerArrayVector->offsetAt(0));
  ASSERT_EQ(2, innerArrayVector->sizeAt(0));
  ASSERT_EQ(2, innerArrayVector->offsetAt(1));
  ASSERT_EQ(2, innerArrayVector->sizeAt(1));
  ASSERT_EQ(4, innerArrayVector->offsetAt(2));
  ASSERT_EQ(2, innerArrayVector->sizeAt(2));

  auto elementsVector =
      innerArrayVector->elements()->as<SimpleVector<int32_t>>();
  ASSERT_EQ(1, elementsVector->valueAt(0));
  ASSERT_EQ(2, elementsVector->valueAt(1));
  ASSERT_EQ(3, elementsVector->valueAt(2));
  ASSERT_EQ(4, elementsVector->valueAt(3));
  ASSERT_EQ(5, elementsVector->valueAt(4));
  ASSERT_EQ(6, elementsVector->valueAt(5));
}

TEST_F(Base64Test, arraySingleNullRow) {
  // SELECT CAST(NULL AS ARRAY(VARCHAR));
  const std::string data =
      "BQAAAEFSUkFZDgAAAFZBUklBQkxFX1dJRFRIAAAAAAAAAAAAAQAAAAAAAAAAAAAAAYA=";
  auto array = readBlock(ARRAY(VARCHAR()), data, pool_.get());

  EXPECT_EQ(VectorEncoding::Simple::ARRAY, array->encoding());
  EXPECT_EQ(1, array->size()); // Number of rows.

  auto arrayVector = array->as<ArrayVector>();
  EXPECT_EQ(true, arrayVector->isNullAt(0));
}

TEST_F(Base64Test, mapIntToVarchar) {
  // SELECT MAP(ARRAY[1, 5], ARRAY['AAA', 'B']);
  const std::string data =
      "AwAAAE1BUAkAAABJTlRfQVJSQVkCAAAAAAEAAAAFAAAADgAAAFZBUklBQ"
      "kxFX1dJRFRIAgAAAAMAAAAEAAAAAAQAAABBQUFCBAAAAP//////////AA"
      "AAAAEAAAABAAAAAAAAAAIAAAABAA==";
  auto map = readBlock(MAP(INTEGER(), VARCHAR()), data, pool_.get());

  EXPECT_EQ(VectorEncoding::Simple::MAP, map->encoding());
  EXPECT_EQ(1, map->size()); // Number of rows.

  auto mapVector = map->as<MapVector>();
  auto keysVector = mapVector->mapKeys()->as<SimpleVector<int32_t>>();
  auto valuesVector = mapVector->mapValues()->as<SimpleVector<StringView>>();

  EXPECT_EQ(TypeKind::INTEGER, keysVector->typeKind());
  EXPECT_EQ(TypeKind::VARCHAR, valuesVector->typeKind());

  EXPECT_EQ(2, keysVector->size());
  EXPECT_EQ(2, valuesVector->size());
  EXPECT_EQ(1, keysVector->valueAt(0));
  EXPECT_EQ(5, keysVector->valueAt(1));
  EXPECT_EQ("AAA", valuesVector->valueAt(0).str());
  EXPECT_EQ("B", valuesVector->valueAt(1).str());
}

TEST_F(Base64Test, mapEmptyVarcharToBigint) {
  // SELECT CAST(MAP(ARRAY[], ARRAY[]) AS MAP(VARCHAR, BIGINT));
  const std::string data =
      "AwAAAE1BUA4AAABWQVJJQUJMRV9XSURUSAAAAAAAAAAAAAMAAABSTEUAAAAACgAAA"
      "ExPTkdfQVJSQVkBAAAAAYD/////AQAAAAAAAAAAAAAAAQA=";
  auto map = readBlock(MAP(VARCHAR(), BIGINT()), data, pool_.get());

  EXPECT_EQ(VectorEncoding::Simple::MAP, map->encoding());
  EXPECT_EQ(1, map->size()); // Number of rows.

  auto mapVector = map->as<MapVector>();
  auto keysVector = mapVector->mapKeys()->as<SimpleVector<StringView>>();
  auto valuesVector = mapVector->mapValues()->as<SimpleVector<int64_t>>();

  EXPECT_EQ(TypeKind::VARCHAR, keysVector->typeKind());
  EXPECT_EQ(TypeKind::BIGINT, valuesVector->typeKind());

  EXPECT_EQ(0, keysVector->size());
  EXPECT_EQ(0, valuesVector->size());
}

TEST_F(Base64Test, mapVarcharToIntWithNulls) {
  // SELECT MAP(ARRAY['AAA', 'B', 'CCCCC'], ARRAY[NULL, 5, NULL]);
  const std::string data =
      "AwAAAE1BUA4AAABWQVJJQUJMRV9XSURUSAMAAAADAAAABAAAAAkAAAAACQAAAEFBQUJDQ"
      "0NDQwkAAABJTlRfQVJSQVkDAAAAAaAFAAAABgAAAAAAAAACAAAAAQAAAP"
      "///////////////wEAAAAAAAAAAwAAAAEA";
  auto map = readBlock(MAP(VARCHAR(), INTEGER()), data, pool_.get());

  EXPECT_EQ(VectorEncoding::Simple::MAP, map->encoding());
  EXPECT_EQ(1, map->size()); // Number of rows.

  auto mapVector = map->as<MapVector>();
  auto keysVector = mapVector->mapKeys()->as<SimpleVector<StringView>>();
  auto valuesVector = mapVector->mapValues()->as<SimpleVector<int32_t>>();

  EXPECT_EQ(TypeKind::VARCHAR, keysVector->typeKind());
  EXPECT_EQ(TypeKind::INTEGER, valuesVector->typeKind());

  EXPECT_EQ(3, keysVector->size());
  EXPECT_EQ(3, valuesVector->size());
  EXPECT_EQ("AAA", keysVector->valueAt(0).str());
  EXPECT_EQ("B", keysVector->valueAt(1).str());
  EXPECT_EQ("CCCCC", keysVector->valueAt(2).str());
  EXPECT_EQ(true, valuesVector->isNullAt(0));
  EXPECT_EQ(false, valuesVector->isNullAt(1));
  EXPECT_EQ(5, valuesVector->valueAt(1));
  EXPECT_EQ(true, valuesVector->isNullAt(2));
}

TEST_F(Base64Test, mapSingleNullRow) {
  // SELECT CAST(NULL AS MAP(VARCHAR, BIGINT));
  const std::string data =
      "AwAAAE1BUA4AAABWQVJJQUJMRV9XSURUSAAAAAAAAAAAAAMAAABSTEU"
      "AAAAACgAAAExPTkdfQVJSQVkBAAAAAYD/////AQAAAAAAAAAAAAAAAYA=";
  auto map = readBlock(MAP(VARCHAR(), BIGINT()), data, pool_.get());

  EXPECT_EQ(VectorEncoding::Simple::MAP, map->encoding());
  EXPECT_EQ(1, map->size()); // Number of rows.

  auto mapVector = map->as<MapVector>();
  EXPECT_EQ(true, mapVector->isNullAt(0));
}

TEST_F(Base64Test, timestampWithTimezone) {
  // Base64 encoding + serialization of time stamp '2020-10-31 01:00 UTC' AT
  // TIME ZONE 'America/Los_Angeles' Serialization info see
  // https://prestodb.io/docs/current/develop/serialized-page.html
  const std::string data = "CgAAAExPTkdfQVJSQVkBAAAAACEHaLHCVxcA";
  auto resultVector = readBlock(TIMESTAMP_WITH_TIME_ZONE(), data, pool_.get());
  ASSERT_EQ(TypeKind::BIGINT, resultVector->typeKind());
  ASSERT_EQ(1, resultVector->size());

  auto timestampTZVector = resultVector->as<FlatVector<int64_t>>();
  ASSERT_EQ(6570418176001825, timestampTZVector->valueAt(0));
  ASSERT_EQ(1604106000000, unpackMillisUtc(timestampTZVector->valueAt(0)));
  ASSERT_EQ(1825, unpackZoneKeyId(timestampTZVector->valueAt(0)));
}

TEST_F(Base64Test, rowOfNull) {
  // SELECT CAST(NULL AS ROW(c1 VARCHAR, c2 DOUBLE))
  const std::string data =
      "AwAAAFJPVwIAAAAOAAAAVkFSSUFCTEVfV0lEVEgAAAAAAAAAAAADAAAAUkxFAAAAAAoAAAB"
      "MT05HX0FSUkFZAQAAAAGAAQAAAAAAAAAAAAAAAYA=";
  auto resultVector =
      readBlock(ROW({"c1", "c2"}, {VARCHAR(), DOUBLE()}), data, pool_.get());
  ASSERT_EQ(resultVector->as<RowVector>()->childrenSize(), 2);
  ASSERT_EQ(resultVector->isNullAt(0), true);
}

TEST_F(Base64Test, rowWithNullChild) {
  // SELECT CAST(ROW(1, 'b', null, 4.0) AS ROW(c1 BIGINT, c2 VARCHAR, c3 BIGINT,
  // c4 DOUBLE))
  const std::string data =
      "AwAAAFJPVwQAAAAKAAAATE9OR19BUlJBWQEAAAAAAQAAAAAAAAAOAAAAVkFSSUFCTEVfV0l"
      "EVEgBAAAAAQAAAAABAAAAYgMAAABSTEUBAAAACgAAAExPTkdfQVJSQVkBAAAAAYAKAAAATE"
      "9OR19BUlJBWQEAAAAAAAAAAAAAEEABAAAAAAAAAAEAAAAA";
  auto resultVector = readBlock(
      ROW({"c1", "c2", "c3", "c4"}, {BIGINT(), VARCHAR(), BIGINT(), DOUBLE()}),
      data,
      pool_.get());
  ASSERT_EQ(resultVector->as<RowVector>()->childrenSize(), 4);
  ASSERT_EQ(
      resultVector->as<RowVector>()
          ->childAt(0)
          ->asFlatVector<int64_t>()
          ->valueAt(0),
      1);
  ASSERT_EQ(
      resultVector->as<RowVector>()
          ->childAt(1)
          ->asFlatVector<StringView>()
          ->valueAt(0)
          .str(),
      "b");
  ASSERT_EQ(
      resultVector->as<RowVector>()
          ->childAt(2)
          ->as<ConstantVector<int64_t>>()
          ->isNullAt(0),
      true);
  ASSERT_EQ(
      resultVector->as<RowVector>()
          ->childAt(3)
          ->asFlatVector<double>()
          ->valueAt(0),
      4.0);
}

TEST_F(Base64Test, rowWithNestedRow) {
  // SELECT ROW(ROW(1, 'a'), ROW(2.0, TRUE, null))
  const std::string data =
      "AwAAAFJPVwIAAAADAAAAUk9XAgAAAAkAAABJTlRfQVJSQVkBAAAAAAEAAAAOAAAAVkFSSUF"
      "CTEVfV0lEVEgBAAAAAQAAAAABAAAAYQEAAAAAAAAAAQAAAAADAAAAUk9XAwAAAAoAAABMT0"
      "5HX0FSUkFZAQAAAAAAAAAAAAAAQAoAAABCWVRFX0FSUkFZAQAAAAABAwAAAFJMRQEAAAAKA"
      "AAAQllURV9BUlJBWQEAAAABgAEAAAAAAAAAAQAAAAABAAAAAAAAAAEAAAAA";
  auto resultVector = readBlock(
      ROW({"", ""},
          {ROW({"", ""}, {INTEGER(), VARCHAR()}),
           ROW({"", "", ""}, {DOUBLE(), BOOLEAN(), UNKNOWN()})}),
      data,
      pool_.get());
  ASSERT_EQ(resultVector->as<RowVector>()->childrenSize(), 2);
  auto rowVector1 = resultVector->as<RowVector>()->childAt(0)->as<RowVector>();
  auto rowVector2 = resultVector->as<RowVector>()->childAt(1)->as<RowVector>();
  ASSERT_EQ(rowVector1->childAt(0)->asFlatVector<int32_t>()->valueAt(0), 1);
  ASSERT_EQ(
      rowVector1->childAt(1)->asFlatVector<StringView>()->valueAt(0), "a");
  ASSERT_EQ(rowVector2->childAt(0)->asFlatVector<double>()->valueAt(0), 2.0);
  ASSERT_EQ(rowVector2->childAt(1)->asFlatVector<bool>()->valueAt(0), true);
  ASSERT_EQ(
      rowVector2->childAt(2)->as<SimpleVector<UnknownValue>>()->isNullAt(0),
      true);
}

TEST_F(Base64Test, rowWithIntermediateNulls) {
  // Ensure that a ROW with intermediate nulls is handled correctly. Since only
  // non-null values are serialized, we need to ensure that gaps in the child
  // vectors are introduced for the null indices to ensure a valid vector.
  // Vector used: ROW(c0: INTEGER) types with values [null, 2, null, 4]
  const std::string data =
      "AwAAAFJPVwEAAAAJAAAASU5UX0FSUkFZAgAAAAACAAAABAAAAAQAAAAAAAAAAAAAAAEAAAABAAAAAgAAAAGt";
  auto resultVector = readBlock(ROW({"c0"}, {INTEGER()}), data, pool_.get());
  auto resultRow = resultVector->as<RowVector>();
  ASSERT_EQ(resultVector->as<RowVector>()->childrenSize(), 1);
  auto child = resultRow->childAt(0)->asFlatVector<int32_t>();
  ASSERT_EQ(child->size(), resultRow->size());
  ASSERT_TRUE(resultVector->isNullAt(0));
  ASSERT_EQ(child->valueAt(1), 2);
  ASSERT_TRUE(resultVector->isNullAt(2));
  ASSERT_EQ(child->valueAt(3), 4);
}
