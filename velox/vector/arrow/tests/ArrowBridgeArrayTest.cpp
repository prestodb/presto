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

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/testing/gtest_util.h> // @manual=fbsource//third-party/apache-arrow:arrow_testing
#include <gtest/gtest.h>

#include "velox/common/base/Nulls.h"
#include "velox/core/QueryCtx.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {
namespace {

void mockSchemaRelease(ArrowSchema*) {}
void mockArrayRelease(ArrowArray*) {}

template <typename T>
struct VeloxToArrowType {
  using type = T;
};

template <>
struct VeloxToArrowType<Timestamp> {
  using type = int64_t;
};

class ArrowBridgeArrayExportTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  template <typename T>
  void testFlatVector(
      const std::vector<std::optional<T>>& inputData,
      const TypePtr& type = CppToType<T>::create()) {
    auto flatVector = vectorMaker_.flatVectorNullable(inputData, type);
    ArrowArray arrowArray;
    velox::exportToArrow(flatVector, arrowArray, pool_.get(), options_);

    validateArray(inputData, arrowArray);

    arrowArray.release(&arrowArray);
    EXPECT_EQ(nullptr, arrowArray.release);
    EXPECT_EQ(nullptr, arrowArray.private_data);
  }

  template <typename T>
  void testArrayVector(const T& inputData) {
    auto arrayVector = vectorMaker_.arrayVectorNullable(inputData);
    ArrowArray arrowArray;
    velox::exportToArrow(arrayVector, arrowArray, pool_.get(), options_);

    validateListArray(inputData, arrowArray);

    arrowArray.release(&arrowArray);
    EXPECT_EQ(nullptr, arrowArray.release);
    EXPECT_EQ(nullptr, arrowArray.private_data);
  }

  // Construct and test a constant vector based on a scalar value.
  template <typename T>
  void testConstant(
      const T& input,
      size_t vectorSize = 1000,
      const TypePtr& type = CppToType<T>::create()) {
    VectorPtr vector = BaseVector::createConstant(
        type, variant(input), vectorSize, pool_.get());
    testConstantVector<true /*isScalar*/, T>(vector, input);
  }

  // Test arrow conversion based on a constant vector already constructed.
  template <bool isScalar, typename T, typename TInput>
  void testConstantVector(
      const VectorPtr& constantVector,
      const TInput& input) {
    ArrowArray arrowArray;
    velox::exportToArrow(constantVector, arrowArray, pool_.get(), options_);
    validateConstant<isScalar, T>(
        input,
        constantVector->size(),
        constantVector->mayHaveNulls(),
        arrowArray);

    arrowArray.release(&arrowArray);
    EXPECT_EQ(nullptr, arrowArray.release);
    EXPECT_EQ(nullptr, arrowArray.private_data);
  }

  // Helper functions for verification.

  template <typename T>
  void validateArray(
      const std::vector<std::optional<T>>& inputData,
      const ArrowArray& arrowArray) {
    const bool isString =
        std::is_same_v<T, StringView> or std::is_same_v<T, std::string>;

    EXPECT_EQ(inputData.size(), arrowArray.length);
    EXPECT_EQ(0, arrowArray.offset);
    EXPECT_EQ(0, arrowArray.n_children);

    EXPECT_EQ(nullptr, arrowArray.children);
    EXPECT_EQ(nullptr, arrowArray.dictionary);
    EXPECT_NE(nullptr, arrowArray.release);

    validateNulls(inputData, arrowArray);

    // Validate array contents.
    if constexpr (isString) {
      validateStringArray(inputData, arrowArray);
    } else {
      validateNumericalArray(inputData, arrowArray);
    }
  }

  template <typename T>
  void validateNumericalArray(
      const std::vector<std::optional<T>>& inputData,
      const ArrowArray& arrowArray) {
    using TArrow = typename VeloxToArrowType<T>::type;

    ASSERT_EQ(2, arrowArray.n_buffers); // null and values buffers.
    ASSERT_NE(nullptr, arrowArray.buffers);

    const uint64_t* nulls = static_cast<const uint64_t*>(arrowArray.buffers[0]);
    const TArrow* values = static_cast<const TArrow*>(arrowArray.buffers[1]);

    EXPECT_NE(values, nullptr);

    for (size_t i = 0; i < inputData.size(); ++i) {
      if (inputData[i] == std::nullopt) {
        EXPECT_TRUE(bits::isBitNull(nulls, i));
      } else {
        if (nulls) {
          EXPECT_FALSE(bits::isBitNull(nulls, i));
        }

        // Boolean is packed in a single bit so it needs special treatment.
        if constexpr (std::is_same_v<T, bool>) {
          EXPECT_EQ(
              inputData[i],
              bits::isBitSet(reinterpret_cast<const uint64_t*>(values), i))
              << "mismatch at index " << i;
        } else if constexpr (std::is_same_v<T, Timestamp>) {
          EXPECT_TRUE(validateTimestamp(
              inputData[i].value(), options_.timestampUnit, values[i]))
              << "mismatch at index " << i;
        } else {
          EXPECT_EQ(inputData[i], values[i]) << "mismatch at index " << i;
        }
      }
    }
  }

  template <typename T>
  void validateStringArray(
      const std::vector<std::optional<T>>& inputData,
      const ArrowArray& arrowArray) {
    ASSERT_EQ(3, arrowArray.n_buffers); // null, values, and offsets buffers.
    ASSERT_NE(nullptr, arrowArray.buffers);

    const uint64_t* nulls = static_cast<const uint64_t*>(arrowArray.buffers[0]);
    const char* values = static_cast<const char*>(arrowArray.buffers[2]);
    const int32_t* offsets = static_cast<const int32_t*>(arrowArray.buffers[1]);

    EXPECT_NE(values, nullptr);
    EXPECT_NE(offsets, nullptr);

    for (size_t i = 0; i < inputData.size(); ++i) {
      if (inputData[i] == std::nullopt) {
        EXPECT_TRUE(bits::isBitNull(nulls, i));
      } else {
        if (nulls) {
          EXPECT_FALSE(bits::isBitNull(nulls, i));
        }
        EXPECT_EQ(
            0,
            std::memcmp(
                inputData[i]->data(),
                values + offsets[i],
                offsets[i + 1] - offsets[i]));
      }
    }
  }

  // Validate nulls and returns whether this array uses Arrow's Null Layout.
  template <typename T>
  void validateNulls(
      const std::vector<std::optional<T>>& inputData,
      const ArrowArray& arrowArray) {
    size_t nullCount =
        std::count(inputData.begin(), inputData.end(), std::nullopt);
    EXPECT_EQ(arrowArray.null_count, nullCount);

    if (arrowArray.null_count == 0) {
      EXPECT_EQ(arrowArray.buffers[0], nullptr);
    } else {
      EXPECT_NE(arrowArray.buffers[0], nullptr);
      EXPECT_EQ(
          arrowArray.null_count,
          bits::countNulls(
              static_cast<const uint64_t*>(arrowArray.buffers[0]),
              0,
              inputData.size()));
    }
  }

  template <typename T>
  void validateListArray(
      const std::vector<std::optional<std::vector<std::optional<T>>>>&
          inputData,
      const ArrowArray& arrowArray) {
    const bool isString =
        std::is_same_v<T, StringView> or std::is_same_v<T, std::string>;

    EXPECT_EQ(inputData.size(), arrowArray.length);
    EXPECT_EQ(0, arrowArray.offset);
    EXPECT_EQ(1, arrowArray.n_children);

    EXPECT_NE(nullptr, arrowArray.children);
    EXPECT_EQ(nullptr, arrowArray.dictionary);
    EXPECT_NE(nullptr, arrowArray.release);

    validateNulls(inputData, arrowArray);

    std::vector<std::optional<T>> flattenedData;
    for (const auto& item : inputData) {
      if (item) {
        flattenedData.insert(flattenedData.end(), item->begin(), item->end());
      }
    }

    // Validate offsets.
    auto* offsets = static_cast<const vector_size_t*>(arrowArray.buffers[1]);
    vector_size_t offset = 0;

    for (size_t i = 0; i < inputData.size(); ++i) {
      if (inputData[i]) {
        EXPECT_EQ(offset, offsets[i]);
        offset += inputData[i]->size();
      }
    }
    EXPECT_EQ(offset, offsets[inputData.size()]); // validate last offset.

    // Validate child array contents.
    auto* childArray = arrowArray.children[0];

    if constexpr (isString) {
      validateStringArray(flattenedData, *childArray);
    } else {
      validateNumericalArray(flattenedData, *childArray);
    }
  }

  template <bool isScalar, typename T, typename TInput>
  void validateConstant(
      const TInput& inputData,
      size_t vectorSize,
      bool isNullConstant,
      const ArrowArray& arrowArray) {
    using TRunEnds = int32_t;

    // Validate base REE array.
    EXPECT_EQ(vectorSize, arrowArray.length);
    if (isNullConstant) {
      EXPECT_EQ(vectorSize, arrowArray.null_count);
    } else {
      EXPECT_EQ(0, arrowArray.null_count);
    }
    EXPECT_EQ(0, arrowArray.offset);
    EXPECT_EQ(0, arrowArray.n_buffers);
    EXPECT_EQ(nullptr, arrowArray.buffers);
    EXPECT_EQ(nullptr, arrowArray.dictionary);
    EXPECT_NE(nullptr, arrowArray.release);

    // Validate children.
    EXPECT_EQ(2, arrowArray.n_children); // run_ends and values children.
    EXPECT_NE(nullptr, arrowArray.children);

    const auto& runEnds = *arrowArray.children[0];
    const auto& values = *arrowArray.children[1];

    // Validate values.
    if constexpr (isScalar) {
      validateArray<T>({inputData}, values);
    } else {
      validateListArray<T>({{{inputData}}}, values);
    }

    // Validate run ends - single run of the vector size.
    validateArray<TRunEnds>({vectorSize}, runEnds);
  }

  ArrowSchema makeArrowSchema(const char* format) {
    return ArrowSchema{
        .format = format,
        .name = nullptr,
        .metadata = nullptr,
        .flags = 0,
        .n_children = 0,
        .children = nullptr,
        .dictionary = nullptr,
        .release = mockSchemaRelease,
        .private_data = nullptr,
    };
  }

  ArrowArray makeArrowArray(
      const void** buffers,
      int64_t nBuffers,
      int64_t length,
      int64_t nullCount) {
    return ArrowArray{
        .length = length,
        .null_count = nullCount,
        .offset = 0,
        .n_buffers = nBuffers,
        .n_children = 0,
        .buffers = buffers,
        .children = nullptr,
        .dictionary = nullptr,
        .release = mockArrayRelease,
        .private_data = nullptr,
    };
  }

  template <typename T>
  BufferPtr makeBuffer(const std::vector<T>& data) {
    auto ans = AlignedBuffer::allocate<T>(data.size(), pool_.get());
    auto buf = ans->template asMutable<T>();
    for (int i = 0; i < data.size(); ++i) {
      buf[i] = data[i];
    }
    return ans;
  }

  void exportToArrow(const TypePtr& type, ArrowSchema& out) {
    velox::exportToArrow(
        BaseVector::create(type, 0, pool_.get()), out, options_);
  }

  // Boiler plate structures required by vectorMaker.
  ArrowOptions options_;
  std::shared_ptr<core::QueryCtx> queryCtx_{std::make_shared<core::QueryCtx>()};
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  facebook::velox::test::VectorMaker vectorMaker_{execCtx_.pool()};

 private:
  // Converts timestamp as bigint according to unit, and compares it with the
  // actual value.
  bool
  validateTimestamp(Timestamp ts, TimestampUnit unit, int64_t actualValue) {
    int64_t expectedValue;
    switch (unit) {
      case TimestampUnit::kSecond:
        expectedValue = ts.getSeconds();
        break;
      case TimestampUnit::kMilli:
        expectedValue = ts.toMillis();
        break;
      case TimestampUnit::kMicro:
        expectedValue = ts.toMicros();
        break;
      case TimestampUnit::kNano:
        expectedValue = ts.toNanos();
        break;
      default:
        VELOX_UNREACHABLE();
    }
    return expectedValue == actualValue;
  }
};

TEST_F(ArrowBridgeArrayExportTest, flatNotNull) {
  std::vector<int64_t> inputData = {1, 2, 3, 4, 5};
  ArrowArray arrowArray;
  {
    // Make sure that ArrowArray is correctly acquiring ownership, even after
    // the initial vector shared_ptr is gone.
    auto flatVector = vectorMaker_.flatVector(inputData);
    velox::exportToArrow(flatVector, arrowArray, pool_.get(), options_);
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

TEST_F(ArrowBridgeArrayExportTest, flatBool) {
  testFlatVector<bool>({
      true,
      false,
      false,
      std::nullopt,
      std::nullopt,
      true,
      std::nullopt,
  });
  testFlatVector<bool>({});
}

TEST_F(ArrowBridgeArrayExportTest, flatTinyint) {
  testFlatVector<int8_t>({
      1,
      std::numeric_limits<int8_t>::min(),
      std::nullopt,
      std::numeric_limits<int8_t>::max(),
      std::nullopt,
      4,
  });
  testFlatVector<int8_t>({std::nullopt});
}

TEST_F(ArrowBridgeArrayExportTest, flatSmallint) {
  testFlatVector<int16_t>({
      std::numeric_limits<int16_t>::min(),
      1000,
      std::nullopt,
      std::numeric_limits<int16_t>::max(),
  });
}

TEST_F(ArrowBridgeArrayExportTest, flatInteger) {
  testFlatVector<int32_t>({
      std::numeric_limits<int32_t>::min(),
      std::nullopt,
      std::numeric_limits<int32_t>::max(),
      std::numeric_limits<int32_t>::max(),
      std::nullopt,
      std::nullopt,
  });
}

TEST_F(ArrowBridgeArrayExportTest, flatBigint) {
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

TEST_F(ArrowBridgeArrayExportTest, flatReal) {
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

TEST_F(ArrowBridgeArrayExportTest, flatDouble) {
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

TEST_F(ArrowBridgeArrayExportTest, flatDate) {
  testFlatVector<int32_t>(
      {
          std::numeric_limits<int32_t>::min(),
          std::nullopt,
          std::numeric_limits<int32_t>::max(),
          std::numeric_limits<int32_t>::max(),
          std::nullopt,
          std::nullopt,
      },
      DATE());
}

TEST_F(ArrowBridgeArrayExportTest, flatTimestamp) {
  for (TimestampUnit unit :
       {TimestampUnit::kSecond,
        TimestampUnit::kMilli,
        TimestampUnit::kMicro,
        TimestampUnit::kNano}) {
    options_.timestampUnit = unit;
    testFlatVector<Timestamp>(
        {
            Timestamp(0, 0),
            std::nullopt,
            Timestamp(1699300965, 12'349),
            Timestamp(-2208960000, 0), // 1900-01-01
            Timestamp(3155788800, 999'999'999),
            std::nullopt,
        },
        TIMESTAMP());
  }

  // Out of range. If nanosecond precision is represented in Arrow, timestamps
  // starting around 2263-01-01 should overflow and throw a user exception.
  EXPECT_THROW(
      testFlatVector<Timestamp>({Timestamp(9246211200, 0)}, TIMESTAMP()),
      VeloxUserError);
}

TEST_F(ArrowBridgeArrayExportTest, flatString) {
  testFlatVector<std::string>({
      "my string",
      "another slightly longer string",
      std::nullopt,
      std::nullopt,
      "",
      std::nullopt,
      "a",
      "another even longer string to ensure it's for sure not stored inline!!!",
      std::nullopt,
  });

  // Empty vector.
  testFlatVector<std::string>({});
}

TEST_F(ArrowBridgeArrayExportTest, rowVector) {
  std::vector<std::optional<int64_t>> col1 = {1, 2, 3, 4};
  std::vector<std::optional<double>> col2 = {99.9, 88.8, 77.7, std::nullopt};
  std::vector<std::optional<std::string>> col3 = {
      "my", "string", "column", "Longer string so it's not inlined."};

  auto vector = vectorMaker_.rowVector({
      vectorMaker_.flatVectorNullable(col1),
      vectorMaker_.flatVectorNullable(col2),
      vectorMaker_.flatVectorNullable(col3),
  });

  ArrowArray arrowArray;
  velox::exportToArrow(vector, arrowArray, pool_.get(), options_);

  EXPECT_EQ(col1.size(), arrowArray.length);
  EXPECT_EQ(0, arrowArray.null_count);
  EXPECT_EQ(0, arrowArray.offset);
  EXPECT_EQ(1, arrowArray.n_buffers);
  EXPECT_EQ(vector->childrenSize(), arrowArray.n_children);

  EXPECT_NE(nullptr, arrowArray.children);
  EXPECT_EQ(nullptr, arrowArray.dictionary);

  // Validate data in the children arrays.
  validateNumericalArray(col1, *arrowArray.children[0]);
  validateNumericalArray(col2, *arrowArray.children[1]);
  validateStringArray(col3, *arrowArray.children[2]);

  arrowArray.release(&arrowArray);
  EXPECT_EQ(nullptr, arrowArray.release);
  EXPECT_EQ(nullptr, arrowArray.private_data);
}

// Test a rowVector containing null entries (in the parent RowVector).
TEST_F(ArrowBridgeArrayExportTest, rowVectorNullable) {
  std::vector<std::optional<int64_t>> col1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  auto vector = vectorMaker_.rowVector({
      vectorMaker_.flatVectorNullable(col1),
  });

  // Setting a few null elements.
  vector->setNull(1, true);
  vector->setNull(3, true);
  vector->setNull(7, true);

  vector->setNullCount(3);

  ArrowArray arrowArray;
  velox::exportToArrow(vector, arrowArray, pool_.get(), options_);

  EXPECT_EQ(col1.size(), arrowArray.length);
  EXPECT_EQ(3, arrowArray.null_count);
  EXPECT_EQ(0, arrowArray.offset);
  EXPECT_EQ(1, arrowArray.n_buffers);
  EXPECT_EQ(vector->childrenSize(), arrowArray.n_children);

  EXPECT_NE(nullptr, arrowArray.children);
  EXPECT_EQ(nullptr, arrowArray.dictionary);

  // Validate data in the children arrays.
  validateNumericalArray(col1, *arrowArray.children[0]);

  // Check if the null buffer has the correct bits set.
  const uint64_t* nulls = static_cast<const uint64_t*>(arrowArray.buffers[0]);
  EXPECT_FALSE(bits::isBitNull(nulls, 0));
  EXPECT_TRUE(bits::isBitNull(nulls, 1));
  EXPECT_TRUE(bits::isBitNull(nulls, 3));
  EXPECT_TRUE(bits::isBitNull(nulls, 7));

  arrowArray.release(&arrowArray);
  EXPECT_EQ(nullptr, arrowArray.release);
  EXPECT_EQ(nullptr, arrowArray.private_data);
}

TEST_F(ArrowBridgeArrayExportTest, rowVectorEmpty) {
  ArrowArray arrowArray;
  velox::exportToArrow(
      vectorMaker_.rowVector({}), arrowArray, pool_.get(), options_);
  EXPECT_EQ(0, arrowArray.n_children);
  EXPECT_EQ(1, arrowArray.n_buffers);
  EXPECT_EQ(nullptr, arrowArray.children);

  arrowArray.release(&arrowArray);
}

std::shared_ptr<arrow::Array> toArrow(
    const VectorPtr& vec,
    const ArrowOptions& options,
    memory::MemoryPool* pool) {
  ArrowSchema schema;
  ArrowArray array;
  exportToArrow(vec, schema, options);
  exportToArrow(vec, array, pool, options);
  EXPECT_OK_AND_ASSIGN(auto type, arrow::ImportType(&schema));
  EXPECT_OK_AND_ASSIGN(auto ans, arrow::ImportArray(&array, type));
  return ans;
}

void validateOffsets(
    const arrow::ListArray& array,
    const std::vector<int32_t>& offsets) {
  ASSERT_EQ(array.length() + 1, offsets.size());
  for (int i = 0; i < array.length(); ++i) {
    EXPECT_EQ(array.value_offset(i), offsets[i]);
    EXPECT_EQ(array.value_length(i), offsets[i + 1] - offsets[i]);
  }
}

template <typename T>
using TArrayContainer =
    std::vector<std::optional<std::vector<std::optional<T>>>>;

TEST_F(ArrowBridgeArrayExportTest, arraySimple) {
  TArrayContainer<int64_t> data1 = {{{1, 2, 3}}, {{4, 5}}};
  testArrayVector(data1);

  TArrayContainer<int64_t> data2 = {
      {{1, 2, 3}}, std::nullopt, {{4, std::nullopt, 5}}};
  testArrayVector(data2);

  TArrayContainer<StringView> data3 = {{{"a", "b", "c"}}, {{"d", "e"}}};
  testArrayVector(data3);
}

TEST_F(ArrowBridgeArrayExportTest, arrayCrossValidate) {
  auto vec = vectorMaker_.arrayVector<int64_t>({{1, 2, 3}, {4, 5}});
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  EXPECT_EQ(array->null_count(), 0);
  ASSERT_EQ(*array->type(), *arrow::list(arrow::int64()));
  auto& listArray = static_cast<const arrow::ListArray&>(*array);
  validateOffsets(listArray, {0, 3, 5});
  ASSERT_EQ(*listArray.values()->type(), *arrow::int64());
  auto& values = static_cast<const arrow::Int64Array&>(*listArray.values());
  ASSERT_EQ(values.length(), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(values.Value(i), i + 1);
  }
}

TEST_F(ArrowBridgeArrayExportTest, arrayDictionary) {
  auto vec = ({
    auto indices = makeBuffer<vector_size_t>({1, 2, 0});
    auto wrapped = vectorMaker_.flatVector<int64_t>({1, 2, 3});
    auto inner = BaseVector::wrapInDictionary(nullptr, indices, 3, wrapped);
    auto offsets = makeBuffer<vector_size_t>({2, 0});
    auto sizes = makeBuffer<vector_size_t>({1, 1});
    std::make_shared<ArrayVector>(
        pool_.get(), ARRAY(inner->type()), nullptr, 2, offsets, sizes, inner);
  });

  ArrowSchema schema;
  ArrowArray data;
  velox::exportToArrow(vec, schema, options_);
  velox::exportToArrow(vec, data, vec->pool(), options_);

  auto result = importFromArrowAsViewer(schema, data, vec->pool());
  test::assertEqualVectors(result, vec);
  schema.release(&schema);
  data.release(&data);
}

TEST_F(ArrowBridgeArrayExportTest, arrayGap) {
  auto elements = vectorMaker_.flatVector<int64_t>({1, 2, 3, 4, 5});
  elements->setNull(3, true);
  elements->setNullCount(1);
  auto offsets = makeBuffer<vector_size_t>({0, 3});
  auto sizes = makeBuffer<vector_size_t>({2, 2});
  auto vec = std::make_shared<ArrayVector>(
      pool_.get(), ARRAY(BIGINT()), nullptr, 2, offsets, sizes, elements);
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  EXPECT_EQ(array->null_count(), 0);
  ASSERT_EQ(*array->type(), *arrow::list(arrow::int64()));
  auto& listArray = static_cast<const arrow::ListArray&>(*array);
  validateOffsets(listArray, {0, 2, 4});
  ASSERT_EQ(*listArray.values()->type(), *arrow::int64());
  auto& values = static_cast<const arrow::Int64Array&>(*listArray.values());
  EXPECT_EQ(values.null_count(), 1);
  ASSERT_EQ(values.length(), 4);
  EXPECT_EQ(values.Value(0), 1);
  EXPECT_EQ(values.Value(1), 2);
  EXPECT_TRUE(values.IsNull(2));
  EXPECT_FALSE(values.IsNull(3));
  EXPECT_EQ(values.Value(3), 5);
}

TEST_F(ArrowBridgeArrayExportTest, arrayReorder) {
  auto elements = vectorMaker_.flatVector<int64_t>({1, 2, 3, 4, 5});
  elements->setNull(3, true);
  elements->setNullCount(1);
  auto offsets = makeBuffer<vector_size_t>({3, 0});
  auto sizes = makeBuffer<vector_size_t>({2, 2});
  auto vec = std::make_shared<ArrayVector>(
      pool_.get(), ARRAY(BIGINT()), nullptr, 2, offsets, sizes, elements);
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  EXPECT_EQ(array->null_count(), 0);
  ASSERT_EQ(*array->type(), *arrow::list(arrow::int64()));
  auto& listArray = static_cast<const arrow::ListArray&>(*array);
  validateOffsets(listArray, {0, 2, 4});
  ASSERT_EQ(*listArray.values()->type(), *arrow::int64());
  auto& values = static_cast<const arrow::Int64Array&>(*listArray.values());
  EXPECT_EQ(values.null_count(), 1);
  ASSERT_EQ(values.length(), 4);
  EXPECT_TRUE(values.IsNull(0));
  EXPECT_EQ(values.Value(1), 5);
  EXPECT_EQ(values.Value(2), 1);
  EXPECT_EQ(values.Value(3), 2);
}

TEST_F(ArrowBridgeArrayExportTest, arrayNested) {
  auto elements = vectorMaker_.flatVector<int64_t>({1, 2, 3, 4, 5});
  auto vec = ({
    auto inner = ({
      auto offsets = makeBuffer<vector_size_t>({0, 4, 2});
      auto sizes = makeBuffer<vector_size_t>({1, 1, 1});
      std::make_shared<ArrayVector>(
          pool_.get(), ARRAY(BIGINT()), nullptr, 3, offsets, sizes, elements);
    });
    auto offsets = makeBuffer<vector_size_t>({2, 0});
    auto sizes = makeBuffer<vector_size_t>({1, 1});
    std::make_shared<ArrayVector>(
        pool_.get(), ARRAY(inner->type()), nullptr, 2, offsets, sizes, inner);
  });
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(*array->type(), *arrow::list(arrow::list(arrow::int64())));
  auto& listArray = static_cast<const arrow::ListArray&>(*array);
  validateOffsets(listArray, {0, 1, 2});
  auto& inner = static_cast<const arrow::ListArray&>(*listArray.values());
  validateOffsets(inner, {0, 1, 2});
  auto& values = static_cast<const arrow::Int64Array&>(*inner.values());
  ASSERT_EQ(values.length(), 2);
  EXPECT_EQ(values.Value(0), 3);
  EXPECT_EQ(values.Value(1), 1);
}

TEST_F(ArrowBridgeArrayExportTest, mapSimple) {
  auto allOnes = [](vector_size_t) { return 1; };
  auto vec =
      vectorMaker_.mapVector<int64_t, int64_t>(2, allOnes, allOnes, allOnes);
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  EXPECT_EQ(array->null_count(), 0);
  ASSERT_EQ(*array->type(), *arrow::map(arrow::int64(), arrow::int64()));
  auto& mapArray = static_cast<const arrow::MapArray&>(*array);
  validateOffsets(mapArray, {0, 1, 2});
  auto& keys = static_cast<const arrow::Int64Array&>(*mapArray.keys());
  ASSERT_EQ(keys.length(), 2);
  EXPECT_EQ(keys.Value(0), 1);
  EXPECT_EQ(keys.Value(1), 1);
  auto& items = static_cast<const arrow::Int64Array&>(*mapArray.items());
  ASSERT_EQ(items.length(), 2);
  EXPECT_EQ(items.Value(0), 1);
  EXPECT_EQ(items.Value(1), 1);
}

TEST_F(ArrowBridgeArrayExportTest, mapNested) {
  auto vec = ({
    auto ident = [](vector_size_t i) { return i; };
    auto inner = ({
      auto keys = vectorMaker_.flatVector<int32_t>(5, ident);
      auto values = keys;
      auto offsets = makeBuffer<vector_size_t>({0, 4, 2});
      auto sizes = makeBuffer<vector_size_t>({1, 1, 1});
      auto type = MAP(INTEGER(), INTEGER());
      std::make_shared<MapVector>(
          pool_.get(), type, nullptr, 3, offsets, sizes, keys, values);
    });
    auto offsets = makeBuffer<vector_size_t>({2, 0});
    auto sizes = makeBuffer<vector_size_t>({1, 1});
    auto keys = vectorMaker_.flatVector<int32_t>(3, ident);
    auto type = MAP(INTEGER(), MAP(INTEGER(), INTEGER()));
    std::make_shared<MapVector>(
        pool_.get(), type, nullptr, 2, offsets, sizes, keys, inner);
  });
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  EXPECT_EQ(array->null_count(), 0);
  ASSERT_EQ(
      *array->type(),
      *arrow::map(arrow::int32(), arrow::map(arrow::int32(), arrow::int32())));
  auto& mapArray = static_cast<const arrow::MapArray&>(*array);
  validateOffsets(mapArray, {0, 1, 2});
  {
    auto& keys = static_cast<const arrow::Int32Array&>(*mapArray.keys());
    ASSERT_EQ(keys.length(), 2);
    EXPECT_EQ(keys.Value(0), 2);
    EXPECT_EQ(keys.Value(1), 0);
  }
  auto& inner = static_cast<const arrow::MapArray&>(*mapArray.items());
  validateOffsets(inner, {0, 1, 2});
  auto& keys = static_cast<const arrow::Int32Array&>(*inner.keys());
  ASSERT_EQ(keys.length(), 2);
  EXPECT_EQ(keys.Value(0), 2);
  EXPECT_EQ(keys.Value(1), 0);
  auto& values = static_cast<const arrow::Int32Array&>(*inner.items());
  ASSERT_EQ(values.length(), 2);
  EXPECT_EQ(values.Value(0), 2);
  EXPECT_EQ(values.Value(1), 0);
}

TEST_F(ArrowBridgeArrayExportTest, mapTimestamp) {
  const auto keys =
      vectorMaker_.flatVector<int32_t>(5, [](vector_size_t i) { return i; });
  auto values = vectorMaker_.flatVector<Timestamp>(
      5, [](vector_size_t row) { return Timestamp(row, row); });
  const auto offsets = makeBuffer<vector_size_t>({1, 4, 2});
  const auto sizes = makeBuffer<vector_size_t>({1, 1, 1});
  const auto type = MAP(INTEGER(), TIMESTAMP());
  auto vec = std::make_shared<MapVector>(
      pool_.get(), type, nullptr, 3, offsets, sizes, keys, values);
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  EXPECT_EQ(array->null_count(), 0);
  ASSERT_EQ(
      *array->type(),
      *arrow::map(arrow::int32(), arrow::timestamp(arrow::TimeUnit::NANO)));
  {
    auto& mapArray = static_cast<const arrow::MapArray&>(*array);
    auto& values = static_cast<const arrow::TimestampArray&>(*mapArray.items());
    ASSERT_EQ(values.length(), 3);
    EXPECT_EQ(values.Value(0), 1'000'000'001L);
    EXPECT_EQ(values.Value(1), 4'000'000'004L);
    EXPECT_EQ(values.Value(2), 2'000'000'002L);
  }

  // Nullable timestamp vector.
  values = vectorMaker_.flatVector<Timestamp>(
      5,
      [](vector_size_t row) { return Timestamp(row, row); },
      [](vector_size_t row) { return row % 2 == 1; });
  vec = std::make_shared<MapVector>(
      pool_.get(), type, nullptr, 3, offsets, sizes, keys, values);
  array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  EXPECT_EQ(array->null_count(), 0);
  ASSERT_EQ(
      *array->type(),
      *arrow::map(arrow::int32(), arrow::timestamp(arrow::TimeUnit::NANO)));
  {
    auto& mapArray = static_cast<const arrow::MapArray&>(*array);
    auto& values = static_cast<const arrow::TimestampArray&>(*mapArray.items());
    ASSERT_EQ(values.length(), 3);
    EXPECT_TRUE(values.IsNull(0));
    EXPECT_EQ(values.Value(1), 4'000'000'004L);
    EXPECT_EQ(values.Value(2), 2'000'000'002L);
  }
}

TEST_F(ArrowBridgeArrayExportTest, dictionarySimple) {
  auto vec = BaseVector::wrapInDictionary(
      nullptr,
      allocateIndices(3, pool_.get()),
      3,
      vectorMaker_.flatVector<int64_t>({1, 2, 3}));
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(*array->type(), *arrow::dictionary(arrow::int32(), arrow::int64()));
  auto& dict = static_cast<const arrow::DictionaryArray&>(*array);
  auto& indices = static_cast<const arrow::Int32Array&>(*dict.indices());
  ASSERT_EQ(indices.length(), 3);
  EXPECT_EQ(indices.Value(0), 0);
  EXPECT_EQ(indices.Value(1), 0);
  EXPECT_EQ(indices.Value(2), 0);
  auto& values = static_cast<const arrow::Int64Array&>(*dict.dictionary());
  ASSERT_EQ(values.length(), 3);
  EXPECT_EQ(values.Value(0), 1);
  EXPECT_EQ(values.Value(1), 2);
  EXPECT_EQ(values.Value(2), 3);
}

TEST_F(ArrowBridgeArrayExportTest, dictionaryNested) {
  auto vec = ({
    auto indices = makeBuffer<vector_size_t>({1, 2, 0});
    auto wrapped = vectorMaker_.flatVector<int64_t>({1, 2, 3});
    auto inner = BaseVector::wrapInDictionary(nullptr, indices, 3, wrapped);
    auto offsets = makeBuffer<vector_size_t>({2, 0});
    auto sizes = makeBuffer<vector_size_t>({1, 1});
    std::make_shared<ArrayVector>(
        pool_.get(), ARRAY(inner->type()), nullptr, 2, offsets, sizes, inner);
  });
  auto array = toArrow(vec, options_, pool_.get());
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(
      *array->type(),
      *arrow::list(arrow::dictionary(arrow::int32(), arrow::int64())));
  auto& list = static_cast<const arrow::ListArray&>(*array);
  validateOffsets(list, {0, 1, 2});
  auto& dict = static_cast<const arrow::DictionaryArray&>(*list.values());
  auto& indices = static_cast<const arrow::Int32Array&>(*dict.indices());
  ASSERT_EQ(indices.length(), 2);
  EXPECT_EQ(indices.Value(0), 0);
  EXPECT_EQ(indices.Value(1), 1);
  auto& values = static_cast<const arrow::Int64Array&>(*dict.dictionary());
  ASSERT_EQ(values.length(), 3);
  EXPECT_EQ(values.Value(0), 1);
  EXPECT_EQ(values.Value(1), 2);
  EXPECT_EQ(values.Value(2), 3);
}

TEST_F(ArrowBridgeArrayExportTest, constants) {
  testConstant((int64_t)987654321);
  testConstant((int32_t)1234);
  testConstant((float)44.3);
  testConstant(true);
  testConstant(StringView("inlined"));

  // Null constant.
  VectorPtr vector =
      BaseVector::createNullConstant(TINYINT(), 2048, pool_.get());
  testConstantVector<true, int8_t>(
      vector, std::vector<std::optional<int8_t>>{std::nullopt});
}

TEST_F(ArrowBridgeArrayExportTest, constantComplex) {
  auto innerArray =
      vectorMaker_.arrayVector<int64_t>({{1, 2, 3}, {4, 5}, {6, 7, 8, 9}});

  // Wrap around different indices.
  VectorPtr vector = BaseVector::wrapInConstant(100, 1, innerArray);
  testConstantVector<false, int64_t>(
      vector, std::vector<std::optional<int64_t>>{4, 5});

  vector = BaseVector::wrapInConstant(100, 0, innerArray);
  testConstantVector<false, int64_t>(
      vector, std::vector<std::optional<int64_t>>{1, 2, 3});
}

TEST_F(ArrowBridgeArrayExportTest, constantCrossValidate) {
  auto vector =
      BaseVector::createConstant(VARCHAR(), "hello", 100, pool_.get());
  auto array = toArrow(vector, options_, pool_.get());

  ASSERT_OK(array->ValidateFull());
  EXPECT_EQ(array->null_count(), 0);
  ASSERT_EQ(
      *array->type(), *arrow::run_end_encoded(arrow::int32(), arrow::utf8()));
  const auto& reeArray = static_cast<const arrow::RunEndEncodedArray&>(*array);

  const auto& runEnds = reeArray.run_ends();
  const auto& values = reeArray.values();

  ASSERT_EQ(*runEnds->type(), *arrow::int32());
  ASSERT_EQ(*values->type(), *arrow::utf8());

  const auto& valuesArray = static_cast<const arrow::StringArray&>(*values);
  const auto& runEndsArray = static_cast<const arrow::Int32Array&>(*runEnds);

  ASSERT_EQ(valuesArray.length(), 1);
  ASSERT_EQ(runEndsArray.length(), 1);

  EXPECT_EQ(valuesArray.GetString(0), std::string("hello"));
  EXPECT_EQ(runEndsArray.Value(0), 100);
}

class ArrowBridgeArrayImportTest : public ArrowBridgeArrayExportTest {
 protected:
  // Used by this base test class to import Arrow data and create Velox Vector.
  // Derived test classes should call the import function under test.
  virtual VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) = 0;

  virtual bool isViewer() const = 0;

  // Helper structure to hold buffers required by an ArrowArray.
  struct ArrowContextHolder {
    BufferPtr values;
    BufferPtr nulls;
    BufferPtr offsets;

    // Tests might not use the whole array.
    const void* buffers[3] = {nullptr, nullptr, nullptr};
    ArrowArray* children[10];
  };

  template <typename T>
  ArrowArray fillArrowArray(
      const std::vector<std::optional<T>>& inputValues,
      ArrowContextHolder& holder) {
    using TArrow = typename VeloxToArrowType<T>::type;
    int64_t length = inputValues.size();
    int64_t nullCount = 0;

    holder.values = AlignedBuffer::allocate<TArrow>(length, pool_.get());
    holder.nulls = AlignedBuffer::allocate<uint64_t>(length, pool_.get());

    auto rawValues = holder.values->asMutable<TArrow>();
    auto rawNulls = holder.nulls->asMutable<uint64_t>();

    for (size_t i = 0; i < length; ++i) {
      if (inputValues[i] == std::nullopt) {
        bits::setNull(rawNulls, i);
        nullCount++;
      } else {
        bits::clearNull(rawNulls, i);
        if constexpr (std::is_same_v<T, bool>) {
          bits::setBit(rawValues, i, *inputValues[i]);
        } else {
          rawValues[i] = *inputValues[i];
        }
      }
    }

    holder.buffers[0] = (length == 0) ? nullptr : (const void*)rawNulls;
    holder.buffers[1] = (length == 0) ? nullptr : (const void*)rawValues;
    return makeArrowArray(holder.buffers, 2, length, nullCount);
  }

  ArrowArray fillArrowArray(
      const std::vector<std::optional<std::string>>& inputValues,
      ArrowContextHolder& holder) {
    int64_t length = inputValues.size();
    int64_t nullCount = 0;

    // Calculate overall buffer size.
    int64_t bufferSize = 0;
    for (const auto& it : inputValues) {
      if (it.has_value()) {
        bufferSize += it->size();
      }
    }

    holder.nulls = AlignedBuffer::allocate<uint64_t>(length, pool_.get());
    holder.offsets = AlignedBuffer::allocate<int32_t>(length + 1, pool_.get());
    holder.values = AlignedBuffer::allocate<char>(bufferSize, pool_.get());

    auto rawNulls = holder.nulls->asMutable<uint64_t>();
    auto rawOffsets = holder.offsets->asMutable<int32_t>();
    auto rawValues = holder.values->asMutable<char>();
    *rawOffsets = 0;

    holder.buffers[2] = (length == 0) ? nullptr : (const void*)rawValues;
    holder.buffers[1] = (length == 0) ? nullptr : (const void*)rawOffsets;

    for (size_t i = 0; i < length; ++i) {
      if (inputValues[i] == std::nullopt) {
        bits::setNull(rawNulls, i);
        nullCount++;
        *(rawOffsets + 1) = *rawOffsets;
        ++rawOffsets;
      } else {
        bits::clearNull(rawNulls, i);
        const auto& val = *inputValues[i];

        std::memcpy(rawValues, val.data(), val.size());
        rawValues += val.size();
        *(rawOffsets + 1) = *rawOffsets + val.size();
        ++rawOffsets;
      }
    }

    holder.buffers[0] = (length == 0) ? nullptr : (const void*)rawNulls;
    return makeArrowArray(holder.buffers, 3, length, nullCount);
  }

  // Takes a vector with input data, generates an input ArrowArray and Velox
  // Vector (using vector maker). Then converts ArrowArray into Velox vector and
  // assert that both Velox vectors are semantically the same.
  template <typename TOutput, typename TInput = TOutput>
  void testArrowImport(
      const char* format,
      const std::vector<std::optional<TInput>>& inputValues) {
    ArrowContextHolder holder;
    auto arrowArray = fillArrowArray(inputValues, holder);

    auto arrowSchema = makeArrowSchema(format);
    auto output = importFromArrow(arrowSchema, arrowArray, pool_.get());
    if constexpr (
        std::is_same_v<TInput, int64_t> && std::is_same_v<TOutput, Timestamp>) {
      assertTimestampVectorContent(
          inputValues, output, arrowArray.null_count, format);
    } else if constexpr (
        std::is_same_v<TInput, int128_t> && std::is_same_v<TOutput, int64_t>) {
      assertShortDecimalVectorContent(
          inputValues, output, arrowArray.null_count);
    } else {
      assertVectorContent(inputValues, output, arrowArray.null_count);
    }

    // Buffer views are not reusable. Strings might need to create an additional
    // buffer, depending on the string sizes, in which case the buffers could be
    // reusable. So we don't check them in here.
    if constexpr (!std::is_same_v<TInput, std::string>) {
      EXPECT_FALSE(BaseVector::isVectorWritable(output));
    } else {
      size_t totalLength = 0;
      bool isInline = true;
      for (const auto& value : inputValues) {
        if (value.has_value()) {
          auto view = StringView(value.value());
          totalLength += view.size();
          isInline = isInline && view.isInline();
        }
      }
      totalLength = isInline ? 0 : totalLength;
      auto buffers =
          dynamic_cast<const FlatVector<StringView>*>(output->wrappedVector())
              ->stringBuffers();
      size_t realLength = 0;
      for (auto& buffer : buffers) {
        realLength += buffer->capacity();
      }
      EXPECT_EQ(totalLength, realLength);
    }
  }

  template <typename T>
  void assertVectorContent(
      const std::vector<std::optional<T>>& inputValues,
      const VectorPtr& convertedVector,
      size_t nullCount) {
    EXPECT_EQ((nullCount > 0), convertedVector->mayHaveNulls());
    EXPECT_EQ(nullCount, *convertedVector->getNullCount());
    EXPECT_EQ(inputValues.size(), convertedVector->size());

    auto expected = vectorMaker_.flatVectorNullable(inputValues);

    // Assert new vector contents.
    for (vector_size_t i = 0; i < convertedVector->size(); ++i) {
      ASSERT_TRUE(expected->equalValueAt(convertedVector.get(), i, i))
          << "at " << i << ": " << expected->toString(i) << " vs. "
          << convertedVector->toString(i);
    }
  }

  void testImportScalar() {
    testArrowImport<bool>("b", {});
    testArrowImport<bool>("b", {true});
    testArrowImport<bool>("b", {false});
    testArrowImport<bool>("b", {true, false, true});
    testArrowImport<bool>("b", {true, std::nullopt, true});

    testArrowImport<int8_t>("c", {});
    testArrowImport<int8_t>("c", {101});
    testArrowImport<int8_t>("c", {5, 4, 3, 1, 2});
    testArrowImport<int8_t>("c", {8, std::nullopt, std::nullopt});
    testArrowImport<int8_t>("c", {std::nullopt, std::nullopt});

    testArrowImport<int16_t>("s", {5, 4, 3, 1, 2});
    testArrowImport<int32_t>("i", {5, 4, 3, 1, 2});

    testArrowImport<int32_t>("tdD", {5, 4, 3, 1, 2});

    testArrowImport<int64_t>("l", {});
    testArrowImport<int64_t>("l", {std::nullopt});
    testArrowImport<int64_t>("l", {-99, 4, 318321631, 1211, -12});
    testArrowImport<int64_t>("l", {std::nullopt, 12345678, std::nullopt});
    testArrowImport<int64_t>("l", {std::nullopt, std::nullopt});

    testArrowImport<double>("g", {});
    testArrowImport<double>("g", {std::nullopt});
    testArrowImport<double>("g", {-99.9, 4.3, 31.1, 129.11, -12});
    testArrowImport<float>("f", {-99.9, 4.3, 31.1, 129.11, -12});

    for (const auto tsString : {"tss:", "tsm:", "tsu:", "tsn:"}) {
      testArrowImport<Timestamp, int64_t>(
          tsString, {0, std::nullopt, Timestamp::kMaxSeconds});
    }

    testArrowImport<int64_t, int128_t>(
        "d:5,2", {1, -1, 0, 12345, -12345, std::nullopt});
  }

  template <typename TOutput, typename TInput>
  void testImportWithoutNullsBuffer(
      std::vector<std::optional<TInput>> inputValues,
      const char* format) {
    auto length = inputValues.size();

    // Construct Arrow array without nulls value and nulls buffer
    ArrowContextHolder holder1;
    holder1.values = AlignedBuffer::allocate<TInput>(length, pool_.get());
    auto rawValues1 = holder1.values->asMutable<TInput>();
    for (size_t i = 0; i < length; ++i) {
      rawValues1[i] = *inputValues[i];
    }

    holder1.buffers[0] = nullptr;
    holder1.buffers[1] = (const void*)rawValues1;
    auto arrowArray1 = makeArrowArray(holder1.buffers, 2, length, 0);
    auto arrowSchema1 = makeArrowSchema(format);

    auto output = importFromArrow(arrowSchema1, arrowArray1, pool_.get());
    if constexpr (
        std::is_same_v<TInput, int64_t> && std::is_same_v<TOutput, Timestamp>) {
      assertTimestampVectorContent(
          inputValues, output, arrowArray1.null_count, format);
    } else {
      assertVectorContent(inputValues, output, arrowArray1.null_count);
    }

    // However, convert from an Arrow array without nulls buffer but non-zero
    // null count should fail
    ArrowContextHolder holder2;
    holder2.values = AlignedBuffer::allocate<TInput>(length, pool_.get());
    auto rawValues2 = holder2.values->asMutable<TInput>();
    for (size_t i = 0; i < length; ++i) {
      rawValues2[i] = *inputValues[i];
    }

    holder2.buffers[0] = nullptr;
    holder2.buffers[1] = (const void*)rawValues2;
    auto arrowSchema2 = makeArrowSchema(format);
    auto arrowArray2 = makeArrowArray(holder2.buffers, 2, length, 1);
    EXPECT_THROW(
        importFromArrow(arrowSchema2, arrowArray2, pool_.get()),
        VeloxUserError);
  }

  void testImportString() {
    testArrowImport<std::string>("u", {});
    testArrowImport<std::string>("u", {"single"});
    testArrowImport<std::string>(
        "u",
        {
            "hello world",
            "larger string which should not be inlined...",
            std::nullopt,
            "hello",
            "from",
            "the",
            "other",
            "side",
            std::nullopt,
            std::nullopt,
        });

    testArrowImport<std::string>(
        "z",
        {
            std::nullopt,
            "testing",
            "a",
            std::nullopt,
            "varbinary",
            "vector",
            std::nullopt,
        });
  }

 private:
  // Creates short decimals from int128 and asserts the content of actual vector
  // with the expected values.
  void assertShortDecimalVectorContent(
      const std::vector<std::optional<int128_t>>& expectedValues,
      const VectorPtr& actual,
      size_t nullCount) {
    std::vector<std::optional<int64_t>> decValues;
    decValues.reserve(expectedValues.size());
    for (const auto& value : expectedValues) {
      if (value) {
        decValues.emplace_back(static_cast<int64_t>(*value));
      } else {
        decValues.emplace_back(std::nullopt);
      }
    }
    assertVectorContent(decValues, actual, nullCount);
  }

  // Creates timestamp from bigint and asserts the content of actual vector with
  // the expected timestamp values.
  void assertTimestampVectorContent(
      const std::vector<std::optional<int64_t>>& expectedValues,
      const VectorPtr& actual,
      size_t nullCount,
      const char* format) {
    VELOX_USER_CHECK_GE(
        strlen(format), 3, "At least three characters are expected.");
    std::vector<std::optional<Timestamp>> tsValues;
    tsValues.reserve(expectedValues.size());
    for (const auto& value : expectedValues) {
      if (!value.has_value()) {
        tsValues.emplace_back(std::nullopt);
      } else {
        Timestamp ts;
        switch (format[2]) {
          case 's':
            ts = Timestamp(value.value(), 0);
            break;
          case 'm':
            ts = Timestamp::fromMillis(value.value());
            break;
          case 'u':
            ts = Timestamp::fromMicros(value.value());
            break;
          case 'n':
            ts = Timestamp::fromNanos(value.value());
            break;
          default:
            VELOX_UNREACHABLE();
        }
        tsValues.emplace_back(ts);
      }
    }
    assertVectorContent(tsValues, actual, nullCount);
  }

  void testImportRowFull() {
    // Manually create a ROW type.
    ArrowSchema arrowSchema;
    exportToArrow(
        ROW({"col1", "col2", "col3"}, {BIGINT(), DOUBLE(), VARCHAR()}),
        arrowSchema);

    // Some test data.
    std::vector<std::optional<int64_t>> col1 = {1, 2, 3, 4};
    std::vector<std::optional<double>> col2 = {99.9, 88.8, 77.7, std::nullopt};
    std::vector<std::optional<std::string>> col3 = {
        "my", "string", "column", "Longer string so it's not inlined."};

    // Create the 3 children arrays.
    ArrowContextHolder childHolder1;
    ArrowContextHolder childHolder2;
    ArrowContextHolder childHolder3;

    auto childArray1 = fillArrowArray(col1, childHolder1);
    auto childArray2 = fillArrowArray(col2, childHolder2);
    auto childArray3 = fillArrowArray(col3, childHolder3);

    // Create parent array and set the child pointers.
    ArrowContextHolder parentHolder;
    auto arrowArray = makeArrowArray(parentHolder.buffers, 0, col1.size(), 0);
    arrowArray.buffers[0] = nullptr;

    arrowArray.n_children = 3;
    arrowArray.children = parentHolder.children;
    arrowArray.children[0] = &childArray1;
    arrowArray.children[1] = &childArray2;
    arrowArray.children[2] = &childArray3;

    // Import and validate output.
    auto outputVector = importFromArrow(arrowSchema, arrowArray, pool_.get());
    auto rowVector = std::dynamic_pointer_cast<RowVector>(outputVector);

    EXPECT_TRUE(rowVector != nullptr);
    EXPECT_EQ(arrowArray.n_children, rowVector->childrenSize());
    EXPECT_EQ(arrowArray.length, rowVector->size());

    assertVectorContent(col1, rowVector->childAt(0), childArray1.null_count);
    assertVectorContent(col2, rowVector->childAt(1), childArray2.null_count);
    assertVectorContent(col3, rowVector->childAt(2), childArray3.null_count);

    if (isViewer()) {
      arrowArray.release(&arrowArray);
      arrowSchema.release(&arrowSchema);
    } else {
      EXPECT_EQ(arrowArray.release, nullptr);
      EXPECT_EQ(arrowSchema.release, nullptr);
    }
  }

  void testImportRowEmpty() {
    // Manually create a ROW type.
    ArrowSchema arrowSchema;
    exportToArrow(ROW({}), arrowSchema);

    // Create parent array and set the child pointers.
    ArrowContextHolder parentHolder;
    auto arrowArray = makeArrowArray(parentHolder.buffers, 0, 0, 0);

    // Import and validate output.
    auto outputVector = importFromArrow(arrowSchema, arrowArray, pool_.get());
    auto rowVector = std::dynamic_pointer_cast<RowVector>(outputVector);

    EXPECT_TRUE(rowVector != nullptr);
    EXPECT_EQ(0, rowVector->childrenSize());
    EXPECT_EQ(0, rowVector->size());

    if (isViewer()) {
      arrowArray.release(&arrowArray);
      arrowSchema.release(&arrowSchema);
    } else {
      EXPECT_EQ(arrowArray.release, nullptr);
      EXPECT_EQ(arrowSchema.release, nullptr);
    }
  }

 protected:
  void testImportRow() {
    testImportRowFull();
    testImportRowEmpty();
  }

 private:
  void toVeloxVector(const arrow::Array& array, VectorPtr& outVector) {
    ArrowSchema schema;
    ArrowArray data;
    ASSERT_OK(arrow::ExportType(*array.type(), &schema));
    ASSERT_OK(arrow::ExportArray(array, &data));
    outVector = importFromArrow(schema, data, pool_.get());
    if (isViewer()) {
      // These release calls just decrease the refcount; there are still
      // references to keep the data alive from the original Arrow array.
      schema.release(&schema);
      data.release(&data);
    } else {
      EXPECT_FALSE(schema.release);
      EXPECT_FALSE(data.release);
    }
  }

  template <typename F>
  void testArrowRoundTrip(const arrow::Array& array, F validateVector) {
    VectorPtr vec;
    toVeloxVector(array, vec);
    validateVector(*vec);

    ArrowSchema schema;
    ArrowArray data;
    velox::exportToArrow(vec, schema, options_);
    velox::exportToArrow(vec, data, pool_.get(), options_);
    ASSERT_OK_AND_ASSIGN(auto arrowType, arrow::ImportType(&schema));
    ASSERT_OK_AND_ASSIGN(auto array2, arrow::ImportArray(&data, arrowType));
    ASSERT_OK(array2->ValidateFull());
    EXPECT_TRUE(array2->Equals(array));
  }

 protected:
  void testImportArray() {
    auto vb = std::make_shared<arrow::Int32Builder>();
    arrow::ListBuilder lb(arrow::default_memory_pool(), vb);
    ASSERT_OK(lb.Append());
    ASSERT_OK(vb->Append(1));
    ASSERT_OK(lb.AppendNull());
    ASSERT_OK(lb.Append());
    ASSERT_OK(vb->Append(2));
    ASSERT_OK(vb->Append(3));
    ASSERT_OK(lb.AppendEmptyValue());
    ASSERT_OK_AND_ASSIGN(auto array, lb.Finish());
    testArrowRoundTrip(*array, [](const BaseVector& vec) {
      ASSERT_EQ(*vec.type(), *ARRAY(INTEGER()));
      EXPECT_EQ(vec.size(), 4);
    });
  }

  void testImportMap() {
    auto kb = std::make_shared<arrow::Int32Builder>(); // key builder
    auto ib = std::make_shared<arrow::Int32Builder>(); // item builder
    arrow::MapBuilder mb(arrow::default_memory_pool(), kb, ib);
    ASSERT_OK(mb.Append());
    ASSERT_OK(kb->Append(1));
    ASSERT_OK(ib->Append(2));
    ASSERT_OK(mb.AppendNull());
    ASSERT_OK(mb.Append());
    ASSERT_OK(kb->Append(2));
    ASSERT_OK(ib->Append(4));
    ASSERT_OK(kb->Append(3));
    ASSERT_OK(ib->Append(6));
    ASSERT_OK(mb.AppendEmptyValue());
    ASSERT_OK_AND_ASSIGN(auto array, mb.Finish());
    testArrowRoundTrip(*array, [](const BaseVector& vec) {
      ASSERT_EQ(*vec.type(), *MAP(INTEGER(), INTEGER()));
      EXPECT_EQ(vec.size(), 4);
    });
  }

  void testImportDictionary() {
    arrow::Dictionary32Builder<arrow::Int64Type> b;
    for (int i = 0; i < 60; ++i) {
      if (i % 7 == 0) {
        ASSERT_OK(b.AppendNull());
      } else {
        ASSERT_OK(b.Append(i % 11));
      }
    }
    ASSERT_OK_AND_ASSIGN(auto array, b.Finish());
    testArrowRoundTrip(*array, [](const BaseVector& vec) {
      ASSERT_EQ(*vec.type(), *BIGINT());
      EXPECT_EQ(vec.encoding(), VectorEncoding::Simple::DICTIONARY);
      EXPECT_EQ(vec.size(), 60);
    });
  }

  void testImportREE() {
    testImportREENoRuns();
    testImportREESingleRun();
    testImportREEMultipleRuns();
  }

  void testImportREENoRuns() {
    auto pool = arrow::default_memory_pool();

    arrow::RunEndEncodedBuilder reeInt32(
        pool,
        std::make_shared<arrow::Int32Builder>(pool),
        std::make_shared<arrow::Int32Builder>(pool),
        run_end_encoded(arrow::int32(), arrow::int32()));
    ASSERT_OK_AND_ASSIGN(auto array, reeInt32.Finish());

    VectorPtr vector;
    toVeloxVector(*array, vector);

    ASSERT_EQ(*vector->type(), *INTEGER());
    EXPECT_EQ(vector->encoding(), VectorEncoding::Simple::CONSTANT);
    EXPECT_EQ(vector->size(), 0);
  }

  void testImportREESingleRun() {
    auto pool = arrow::default_memory_pool();

    // Simple integer column.
    arrow::RunEndEncodedBuilder reeInt32(
        pool,
        std::make_shared<arrow::Int32Builder>(pool),
        std::make_shared<arrow::Int32Builder>(pool),
        run_end_encoded(arrow::int32(), arrow::int32()));
    ASSERT_OK(reeInt32.AppendScalar(*arrow::MakeScalar<int32_t>(123), 20));
    ASSERT_OK_AND_ASSIGN(auto array, reeInt32.Finish());

    validateImportREESingleRun(*array, INTEGER(), 20);

    // String column.
    arrow::RunEndEncodedBuilder reeString(
        pool,
        std::make_shared<arrow::Int32Builder>(pool),
        std::make_shared<arrow::StringBuilder>(pool),
        run_end_encoded(arrow::int32(), arrow::utf8()));
    ASSERT_OK(reeString.AppendScalar(*arrow::MakeScalar("bla"), 199));
    ASSERT_OK_AND_ASSIGN(array, reeString.Finish());

    validateImportREESingleRun(*array, VARCHAR(), 199);

    // Array/List.
    auto valuesBuilder = std::make_shared<arrow::FloatBuilder>(pool);
    auto listBuilder =
        std::make_shared<arrow::ListBuilder>(pool, valuesBuilder);
    ASSERT_OK(listBuilder->Append());
    ASSERT_OK(valuesBuilder->Append(1.1));
    ASSERT_OK(valuesBuilder->Append(2.2));
    ASSERT_OK(valuesBuilder->Append(3.3));
    ASSERT_OK(valuesBuilder->Append(4.4));
    ASSERT_OK_AND_ASSIGN(auto listArray, listBuilder->Finish());

    auto runEndBuilder = std::make_shared<arrow::Int32Builder>(pool);
    ASSERT_OK(runEndBuilder->Append(123));
    ASSERT_OK_AND_ASSIGN(auto runEndsArray, runEndBuilder->Finish());

    // Create a list containing [1.1, 2.2, 3.3, 4.4] and repeat it 123 times.
    ASSERT_OK_AND_ASSIGN(
        array, arrow::RunEndEncodedArray::Make(123, runEndsArray, listArray));
    validateImportREESingleRun(*array, ARRAY(REAL()), 123);
  }

  void validateImportREESingleRun(
      const arrow::Array& array,
      const TypePtr& expectedType,
      size_t expectedSize) {
    testArrowRoundTrip(array, [&](const BaseVector& vec) {
      ASSERT_EQ(*vec.type(), *expectedType);
      EXPECT_EQ(vec.encoding(), VectorEncoding::Simple::CONSTANT);
      EXPECT_EQ(vec.size(), expectedSize);
    });
  }

  void testImportREEMultipleRuns() {
    auto pool = arrow::default_memory_pool();

    // Simple integer column.
    arrow::RunEndEncodedBuilder reeInt32(
        pool,
        std::make_shared<arrow::Int32Builder>(pool),
        std::make_shared<arrow::Int32Builder>(pool),
        run_end_encoded(arrow::int32(), arrow::int32()));
    ASSERT_OK(reeInt32.AppendScalar(*arrow::MakeScalar<int32_t>(123), 20));
    ASSERT_OK(reeInt32.AppendScalar(*arrow::MakeScalar<int32_t>(321), 2));
    ASSERT_OK(reeInt32.AppendNulls(10));
    ASSERT_OK(reeInt32.AppendScalar(*arrow::MakeScalar<int32_t>(50), 30));
    ASSERT_OK_AND_ASSIGN(auto array, reeInt32.Finish());

    VectorPtr vector;
    toVeloxVector(*array, vector);

    ASSERT_EQ(*vector->type(), *INTEGER());
    EXPECT_EQ(vector->encoding(), VectorEncoding::Simple::DICTIONARY);
    EXPECT_EQ(vector->size(), 62);

    DecodedVector decoded(*vector);
    EXPECT_TRUE(decoded.mayHaveNulls());
    EXPECT_FALSE(decoded.isNullAt(0));
    EXPECT_EQ(decoded.valueAt<int32_t>(0), 123);
    EXPECT_EQ(decoded.valueAt<int32_t>(1), 123);
    EXPECT_EQ(decoded.valueAt<int32_t>(19), 123);
    EXPECT_EQ(decoded.valueAt<int32_t>(20), 321);
    EXPECT_EQ(decoded.valueAt<int32_t>(21), 321);
    EXPECT_TRUE(decoded.isNullAt(22));
    EXPECT_TRUE(decoded.isNullAt(31));
    EXPECT_EQ(decoded.valueAt<int32_t>(32), 50);
    EXPECT_EQ(decoded.valueAt<int32_t>(33), 50);
    EXPECT_EQ(decoded.valueAt<int32_t>(61), 50);
  }

  void testImportFailures() {
    ArrowSchema arrowSchema;
    ArrowArray arrowArray;

    const int32_t values[] = {1, 2, 3, 4};
    const void* buffers[] = {nullptr, values};

    // Unsupported:

    // Offset not yet supported.
    arrowSchema = makeArrowSchema("i");
    arrowArray = makeArrowArray(buffers, 2, 4, 0);
    arrowArray.offset = 1;
    EXPECT_THROW(
        importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);

    // Broken input.

    // Null release callback indicates a released structure and should be
    // error-ed out
    arrowSchema = makeArrowSchema("i");
    arrowArray = makeArrowArray(buffers, 2, 4, 0);
    arrowSchema.release = nullptr;
    EXPECT_THROW(
        importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);

    arrowSchema = makeArrowSchema("i");
    arrowArray = makeArrowArray(buffers, 2, 4, 0);
    arrowArray.release = nullptr;
    EXPECT_THROW(
        importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);

    // Expect two buffers.
    arrowSchema = makeArrowSchema("i");
    arrowArray = makeArrowArray(buffers, 2, 4, 0);
    arrowArray.n_buffers = 1;
    EXPECT_THROW(
        importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);

    // Can't have nulls without null buffer.
    arrowSchema = makeArrowSchema("i");
    arrowArray = makeArrowArray(buffers, 2, 4, 1);
    arrowArray.null_count = 1;
    EXPECT_THROW(
        importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);

    // Non-existing type.
    arrowSchema = makeArrowSchema("a");
    arrowArray = makeArrowArray(buffers, 2, 4, 0);
    EXPECT_THROW(
        importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);

    // Ensure the baseline works.
    arrowSchema = makeArrowSchema("i");
    arrowArray = makeArrowArray(buffers, 2, 4, 0);
    EXPECT_NO_THROW(importFromArrow(arrowSchema, arrowArray, pool_.get()));
  }

  ArrowOptions options_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

class ArrowBridgeArrayImportAsViewerTest : public ArrowBridgeArrayImportTest {
  bool isViewer() const override {
    return true;
  }

  VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) override {
    return facebook::velox::importFromArrowAsViewer(
        arrowSchema, arrowArray, pool);
  }
};

TEST_F(ArrowBridgeArrayImportAsViewerTest, scalar) {
  testImportScalar();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, without_nulls_buffer) {
  std::vector<std::optional<int64_t>> inputValues = {1, 2, 3, 4, 5};
  testImportWithoutNullsBuffer<int64_t>(inputValues, "l");
  testImportWithoutNullsBuffer<Timestamp>(inputValues, "tsn:");
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, string) {
  testImportString();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, row) {
  testImportRow();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, array) {
  testImportArray();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, map) {
  testImportMap();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, dictionary) {
  testImportDictionary();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, ree) {
  testImportREE();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, failures) {
  testImportFailures();
}

class ArrowBridgeArrayImportAsOwnerTest
    : public ArrowBridgeArrayImportAsViewerTest {
  bool isViewer() const override {
    return false;
  }

  VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) override {
    return facebook::velox::importFromArrowAsOwner(
        arrowSchema, arrowArray, pool);
  }
};

TEST_F(ArrowBridgeArrayImportAsOwnerTest, scalar) {
  testImportScalar();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, without_nulls_buffer) {
  std::vector<std::optional<int64_t>> inputValues = {1, 2, 3, 4, 5};
  testImportWithoutNullsBuffer<int64_t>(inputValues, "l");
  testImportWithoutNullsBuffer<Timestamp>(inputValues, "tsn:");
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, string) {
  testImportString();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, row) {
  testImportRow();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, array) {
  testImportArray();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, map) {
  testImportMap();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, dictionary) {
  testImportDictionary();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, ree) {
  testImportREE();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, failures) {
  testImportFailures();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, inputsMarkedReleased) {
  const int32_t values[] = {1, 2, 3, 4};
  const void* buffers[] = {nullptr, values};

  ArrowSchema arrowSchema = makeArrowSchema("i");
  ArrowArray arrowArray = makeArrowArray(buffers, 2, 4, 0);

  auto _ = importFromArrowAsOwner(arrowSchema, arrowArray, pool_.get());

  EXPECT_EQ(arrowSchema.release, nullptr);
  EXPECT_EQ(arrowArray.release, nullptr);
}

struct TestReleaseCalled {
  static bool schemaReleaseCalled;
  static bool arrayReleaseCalled;
  static void releaseSchema(ArrowSchema*) {
    schemaReleaseCalled = true;
  }
  static void releaseArray(ArrowArray*) {
    arrayReleaseCalled = true;
  }
};
bool TestReleaseCalled::schemaReleaseCalled = false;
bool TestReleaseCalled::arrayReleaseCalled = false;

TEST_F(ArrowBridgeArrayImportAsOwnerTest, releaseCalled) {
  const int32_t values[] = {1, 2, 3, 4};
  const void* buffers[] = {nullptr, values};

  ArrowSchema arrowSchema = makeArrowSchema("i");
  ArrowArray arrowArray = makeArrowArray(buffers, 2, 4, 0);

  TestReleaseCalled::schemaReleaseCalled = false;
  TestReleaseCalled::arrayReleaseCalled = false;
  arrowSchema.release = TestReleaseCalled::releaseSchema;
  arrowArray.release = TestReleaseCalled::releaseArray;

  // Create a Velox Vector from Arrow and then destruct it to trigger the
  // release callback calling
  { auto _ = importFromArrowAsOwner(arrowSchema, arrowArray, pool_.get()); }

  EXPECT_TRUE(TestReleaseCalled::schemaReleaseCalled);
  EXPECT_TRUE(TestReleaseCalled::arrayReleaseCalled);
}

} // namespace
} // namespace facebook::velox::test
