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
    pool_ = memory::getDefaultScopedMemoryPool();
  }

  std::unique_ptr<memory::MemoryPool> pool_;
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

  ASSERT_EQ(TypeKind::UNKNOWN, vector->typeKind());
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
  ASSERT_EQ(TypeKind::UNKNOWN, arrayVector->elements()->typeKind());

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

TEST_F(Base64Test, timestampWithTimezone) {
  // Base64 encoding + serialization of time stamp '2020-10-31 01:00 UTC' AT
  // TIME ZONE 'America/Los_Angeles' Serialization info see
  // https://prestodb.io/docs/current/develop/serialized-page.html
  const std::string data = "CgAAAExPTkdfQVJSQVkBAAAAACEHaLHCVxcA";
  auto resultVector = readBlock(TIMESTAMP_WITH_TIME_ZONE(), data, pool_.get());
  ASSERT_EQ(resultVector->as<RowVector>()->childrenSize(), 2);
  ASSERT_EQ(
      resultVector->as<RowVector>()
          ->childAt(0)
          ->asFlatVector<int64_t>()
          ->valueAt(0),
      1604106000000);
  ASSERT_EQ(
      resultVector->as<RowVector>()
          ->childAt(1)
          ->asFlatVector<int16_t>()
          ->valueAt(0),
      1825);
}
