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

#include "gtest/gtest.h"

#include "velox/common/base/Nulls.h"
#include "velox/core/QueryCtx.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/VectorMaker.h"

namespace {

using namespace facebook::velox;

class ArrowBridgeTest : public testing::Test {
 protected:
  template <typename T>
  void testFlatVector(const std::vector<std::optional<T>>& inputData) {
    auto flatVector = vectorMaker_.flatVectorNullable(inputData);
    ArrowArray arrowArray;
    arrow::exportToArrow(flatVector, arrowArray);

    size_t nullCount =
        std::count(inputData.begin(), inputData.end(), std::nullopt);
    EXPECT_EQ(inputData.size(), arrowArray.length);
    EXPECT_EQ(nullCount, arrowArray.null_count);
    EXPECT_EQ(0, arrowArray.offset);
    EXPECT_EQ(0, arrowArray.n_children);

    EXPECT_EQ(nullptr, arrowArray.children);
    EXPECT_EQ(nullptr, arrowArray.dictionary);

    // Validate buffers.
    EXPECT_EQ(2, arrowArray.n_buffers); // null and values buffers.

    const uint64_t* nulls = static_cast<const uint64_t*>(arrowArray.buffers[0]);
    const T* values = static_cast<const T*>(arrowArray.buffers[1]);

    for (size_t i = 0; i < inputData.size(); ++i) {
      if (inputData[i] == std::nullopt) {
        EXPECT_TRUE(bits::isBitNull(nulls, i));
      } else {
        EXPECT_FALSE(bits::isBitNull(nulls, i));
        EXPECT_EQ(inputData[i], values[i]);
      }
    }

    arrowArray.release(&arrowArray);
    EXPECT_EQ(nullptr, arrowArray.release);
    EXPECT_EQ(nullptr, arrowArray.private_data);
  }

  // Boiler plate structures required by vectorMaker.
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  facebook::velox::test::VectorMaker vectorMaker_{execCtx_.pool()};
};

TEST_F(ArrowBridgeTest, flatNotNull) {
  std::vector<int64_t> inputData = {1, 2, 3, 4, 5};
  ArrowArray arrowArray;
  {
    // Make sure that ArrowArray is correctly acquiring ownership, even after
    // the initial vector shared_ptr is gone.
    auto flatVector = vectorMaker_.flatVector(inputData);
    arrow::exportToArrow(flatVector, arrowArray);
  }

  EXPECT_EQ(inputData.size(), arrowArray.length);
  EXPECT_EQ(0, arrowArray.null_count);
  EXPECT_EQ(0, arrowArray.offset);
  EXPECT_EQ(0, arrowArray.n_children);

  EXPECT_EQ(nullptr, arrowArray.children);
  EXPECT_EQ(nullptr, arrowArray.dictionary);

  // Validate buffers.
  EXPECT_EQ(2, arrowArray.n_buffers); // null and values buffers.
  EXPECT_EQ(nullptr, arrowArray.buffers[0]); // no nulls.

  const int64_t* values = static_cast<const int64_t*>(arrowArray.buffers[1]);

  for (size_t i = 0; i < inputData.size(); ++i) {
    EXPECT_EQ(inputData[i], values[i]);
  }

  // Consumers are required to call release. Ensure release and private_data
  // are null after releasing it.
  arrowArray.release(&arrowArray);
  EXPECT_EQ(nullptr, arrowArray.release);
  EXPECT_EQ(nullptr, arrowArray.private_data);
}

TEST_F(ArrowBridgeTest, flatTinyint) {
  testFlatVector<int8_t>({
      1,
      std::numeric_limits<int8_t>::min(),
      std::nullopt,
      std::numeric_limits<int8_t>::max(),
      std::nullopt,
      4,
  });
}

TEST_F(ArrowBridgeTest, flatSmallint) {
  testFlatVector<int16_t>({
      std::numeric_limits<int16_t>::min(),
      1000,
      std::nullopt,
      std::numeric_limits<int16_t>::max(),
  });
}

TEST_F(ArrowBridgeTest, flatInteger) {
  testFlatVector<int32_t>({
      std::numeric_limits<int32_t>::min(),
      std::nullopt,
      std::numeric_limits<int32_t>::max(),
      std::numeric_limits<int32_t>::max(),
      std::nullopt,
      std::nullopt,
  });
}

TEST_F(ArrowBridgeTest, flatBigint) {
  testFlatVector<int64_t>({
      std::nullopt,
      99876,
      std::nullopt,
      12345678,
      std::numeric_limits<int64_t>::max(),
      std::numeric_limits<int64_t>::min(),
      std::nullopt,
  });
}

TEST_F(ArrowBridgeTest, flatReal) {
  testFlatVector<float>({
      std::nullopt,
      std::numeric_limits<float>::infinity(),
      std::numeric_limits<float>::lowest(),
      std::numeric_limits<float>::max(),
      std::nullopt,
      77.8,
      12.34,
  });
}

TEST_F(ArrowBridgeTest, flatDouble) {
  testFlatVector<double>({
      1.1,
      std::numeric_limits<double>::infinity(),
      std::numeric_limits<double>::lowest(),
      std::nullopt,
      std::numeric_limits<double>::max(),
      std::nullopt,
      99.4,
  });
}

TEST_F(ArrowBridgeTest, unsupported) {
  ArrowArray arrowArray;
  VectorPtr vector;

  // Strings.
  vector = vectorMaker_.flatVectorNullable<std::string>({});
  EXPECT_THROW(arrow::exportToArrow(vector, arrowArray), VeloxException);

  // Timestamps.
  vector = vectorMaker_.flatVectorNullable<Timestamp>({});
  EXPECT_THROW(arrow::exportToArrow(vector, arrowArray), VeloxException);

  // Arrays.
  vector = vectorMaker_.arrayVector<int64_t>({{1, 2, 3}, {4, 5}});
  EXPECT_THROW(arrow::exportToArrow(vector, arrowArray), VeloxException);

  // Maps.
  auto lambda = [](vector_size_t /* row */) { return 1; };
  vector = vectorMaker_.mapVector<int64_t, int64_t>(2, lambda, lambda, lambda);
  EXPECT_THROW(arrow::exportToArrow(vector, arrowArray), VeloxException);

  // Row vectors.
  vector =
      vectorMaker_.rowVector({vectorMaker_.flatVector<int64_t>({1, 2, 3})});
  EXPECT_THROW(arrow::exportToArrow(vector, arrowArray), VeloxException);

  // Constant encoding.
  vector = BaseVector::createConstant(variant(10), 10, pool_.get());
  EXPECT_THROW(arrow::exportToArrow(vector, arrowArray), VeloxException);

  // Dictionary encoding.
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(3, pool_.get());
  vector = BaseVector::wrapInDictionary(
      BufferPtr(), indices, 3, vectorMaker_.flatVector<int64_t>({1, 2, 3}));
  EXPECT_THROW(arrow::exportToArrow(vector, arrowArray), VeloxException);
}

class ArrowBridgeSchemaTest : public ArrowBridgeTest {
 protected:
  void testScalarType(const TypePtr& type, const char* arrowFormat) {
    ArrowSchema arrowSchema;
    arrow::exportToArrow(type, arrowSchema);

    EXPECT_EQ(std::string{arrowFormat}, std::string{arrowSchema.format});
    EXPECT_EQ(nullptr, arrowSchema.name);

    EXPECT_EQ(0, arrowSchema.n_children);
    EXPECT_EQ(nullptr, arrowSchema.children);

    arrowSchema.release(&arrowSchema);
    EXPECT_EQ(nullptr, arrowSchema.release);
    EXPECT_EQ(nullptr, arrowSchema.private_data);
  }

  // Doesn't check the actual format string of the scalar leaf types (this is
  // tested by the function above), but tests that the types are nested in the
  // correct way.
  void testNestedType(const TypePtr& type) {
    ArrowSchema arrowSchema;
    arrow::exportToArrow(type, arrowSchema);

    verifyNestedType(type, arrowSchema);

    arrowSchema.release(&arrowSchema);
    EXPECT_EQ(nullptr, arrowSchema.release);
    EXPECT_EQ(nullptr, arrowSchema.private_data);
  }

  void verifyNestedType(const TypePtr& type, ArrowSchema& schema) {
    if (type->kind() == TypeKind::ARRAY) {
      EXPECT_EQ(std::string{"+L"}, std::string{schema.format});
      EXPECT_EQ(1, schema.n_children);
    } else if (type->kind() == TypeKind::MAP) {
      EXPECT_EQ(std::string{"+m"}, std::string{schema.format});
      EXPECT_EQ(2, schema.n_children);
    } else if (type->kind() == TypeKind::ROW) {
      // Structs can have zero of more children.
      EXPECT_EQ(std::string{"+s"}, std::string{schema.format});
    }
    // Scalar type.
    else {
      EXPECT_EQ(0, schema.n_children);
      EXPECT_EQ(nullptr, schema.children);
    }

    // Recurse down the children.
    for (size_t i = 0; i < type->size(); ++i) {
      verifyNestedType(type->childAt(i), *schema.children[i]);

      // If this is a rowType, assert that the children returned with the
      // correct name set.
      if (auto rowType = std::dynamic_pointer_cast<const RowType>(type)) {
        EXPECT_EQ(rowType->nameOf(i), std::string(schema.children[i]->name));
      }
    }
  }
};

TEST_F(ArrowBridgeSchemaTest, scalar) {
  testScalarType(TINYINT(), "c");
  testScalarType(SMALLINT(), "s");
  testScalarType(INTEGER(), "i");
  testScalarType(BIGINT(), "l");

  testScalarType(BOOLEAN(), "b");

  testScalarType(REAL(), "f");
  testScalarType(DOUBLE(), "g");

  testScalarType(VARCHAR(), "u");
  testScalarType(VARBINARY(), "z");

  testScalarType(TIMESTAMP(), "ttn");
}

TEST_F(ArrowBridgeSchemaTest, nested) {
  // Array.
  testNestedType(ARRAY(INTEGER()));
  testNestedType(ARRAY(VARCHAR()));
  testNestedType(ARRAY(ARRAY(TINYINT())));
  testNestedType(ARRAY(ARRAY(ARRAY(ARRAY(BOOLEAN())))));

  // Map.
  testNestedType(MAP(INTEGER(), DOUBLE()));
  testNestedType(MAP(VARBINARY(), BOOLEAN()));
  testNestedType(MAP(VARBINARY(), MAP(SMALLINT(), REAL())));
  testNestedType(MAP(VARBINARY(), MAP(SMALLINT(), MAP(INTEGER(), BIGINT()))));

  // Row.
  testNestedType(ROW({}));
  testNestedType(ROW({INTEGER()}));
  testNestedType(ROW({INTEGER(), DOUBLE()}));
  testNestedType(
      ROW({INTEGER(), DOUBLE(), ROW({BIGINT(), REAL(), BOOLEAN()})}));

  // Row with names.
  testNestedType(ROW({"my_col"}, {INTEGER()}));
  testNestedType(ROW({"my_col", "my_other_col"}, {INTEGER(), VARCHAR()}));

  // Mix and match.
  testNestedType(
      ROW({"c1", "c2", "c3"},
          {
              ARRAY(INTEGER()),
              MAP(ROW({VARBINARY(), SMALLINT()}), BOOLEAN()),
              ARRAY(MAP(INTEGER(), VARCHAR())),
          }));
}

TEST_F(ArrowBridgeSchemaTest, unsupported) {
  // Try some combination of unsupported types to ensure there's no crash or
  // memory leak in failure scenarios.
  EXPECT_THROW(testScalarType(UNKNOWN(), ""), VeloxException);

  EXPECT_THROW(testScalarType(ARRAY(UNKNOWN()), ""), VeloxException);
  EXPECT_THROW(testScalarType(MAP(UNKNOWN(), INTEGER()), ""), VeloxException);
  EXPECT_THROW(testScalarType(MAP(BIGINT(), UNKNOWN()), ""), VeloxException);

  EXPECT_THROW(testScalarType(ROW({BIGINT(), UNKNOWN()}), ""), VeloxException);
  EXPECT_THROW(
      testScalarType(ROW({BIGINT(), REAL(), UNKNOWN()}), ""), VeloxException);
}

} // namespace
