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

void mockSchemaRelease(ArrowSchema*) {}
void mockArrayRelease(ArrowArray*) {}

class ArrowBridgeArrayExportTest : public testing::Test {
 protected:
  template <typename T>
  void testFlatVector(const std::vector<std::optional<T>>& inputData) {
    const bool isString =
        std::is_same_v<T, StringView> or std::is_same_v<T, std::string>;

    auto flatVector = vectorMaker_.flatVectorNullable(inputData);
    ArrowArray arrowArray;
    exportToArrow(flatVector, arrowArray, pool_.get());

    size_t nullCount =
        std::count(inputData.begin(), inputData.end(), std::nullopt);
    EXPECT_EQ(inputData.size(), arrowArray.length);
    EXPECT_EQ(nullCount, arrowArray.null_count);
    EXPECT_EQ(0, arrowArray.offset);
    EXPECT_EQ(0, arrowArray.n_children);

    EXPECT_EQ(nullptr, arrowArray.children);
    EXPECT_EQ(nullptr, arrowArray.dictionary);

    // Validate array contents.
    if constexpr (isString) {
      validateStringArray(inputData, arrowArray);
    } else {
      validateNumericalArray(inputData, arrowArray);
    }

    arrowArray.release(&arrowArray);
    EXPECT_EQ(nullptr, arrowArray.release);
    EXPECT_EQ(nullptr, arrowArray.private_data);
  }

  template <typename T>
  void validateNumericalArray(
      const std::vector<std::optional<T>>& inputData,
      const ArrowArray& arrowArray) {
    ASSERT_EQ(2, arrowArray.n_buffers); // null and values buffers.

    const uint64_t* nulls = static_cast<const uint64_t*>(arrowArray.buffers[0]);
    const T* values = static_cast<const T*>(arrowArray.buffers[1]);

    if (arrowArray.null_count == 0) {
      EXPECT_EQ(nulls, nullptr);
    } else {
      EXPECT_NE(nulls, nullptr);
    }
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
              bits::isBitSet(reinterpret_cast<const uint64_t*>(values), i));
        } else {
          EXPECT_EQ(inputData[i], values[i]);
        }
      }
    }
  }

  template <typename T>
  void validateStringArray(
      const std::vector<std::optional<T>>& inputData,
      const ArrowArray& arrowArray) {
    ASSERT_EQ(3, arrowArray.n_buffers); // null, values, and offsets buffers.

    const uint64_t* nulls = static_cast<const uint64_t*>(arrowArray.buffers[0]);
    const char* values = static_cast<const char*>(arrowArray.buffers[2]);
    const int32_t* offsets = static_cast<const int32_t*>(arrowArray.buffers[1]);

    if (arrowArray.null_count == 0) {
      EXPECT_EQ(nulls, nullptr);
    } else {
      EXPECT_NE(nulls, nullptr);
    }
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

  // Boiler plate structures required by vectorMaker.
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  facebook::velox::test::VectorMaker vectorMaker_{execCtx_.pool()};
};

TEST_F(ArrowBridgeArrayExportTest, flatNotNull) {
  std::vector<int64_t> inputData = {1, 2, 3, 4, 5};
  ArrowArray arrowArray;
  {
    // Make sure that ArrowArray is correctly acquiring ownership, even after
    // the initial vector shared_ptr is gone.
    auto flatVector = vectorMaker_.flatVector(inputData);
    exportToArrow(flatVector, arrowArray, pool_.get());
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
  exportToArrow(vector, arrowArray, pool_.get());

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
  exportToArrow(vector, arrowArray, pool_.get());

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
  exportToArrow(vectorMaker_.rowVector({}), arrowArray, pool_.get());
  EXPECT_EQ(0, arrowArray.n_children);
  EXPECT_EQ(1, arrowArray.n_buffers);
  EXPECT_EQ(nullptr, arrowArray.children);

  arrowArray.release(&arrowArray);
}

TEST_F(ArrowBridgeArrayExportTest, unsupported) {
  ArrowArray arrowArray;
  VectorPtr vector;

  // Timestamps.
  vector = vectorMaker_.flatVectorNullable<Timestamp>({});
  EXPECT_THROW(exportToArrow(vector, arrowArray, pool_.get()), VeloxException);

  // Dates.
  vector = vectorMaker_.flatVectorNullable<Date>({});
  EXPECT_THROW(exportToArrow(vector, arrowArray, pool_.get()), VeloxException);

  // Arrays.
  vector = vectorMaker_.arrayVector<int64_t>({{1, 2, 3}, {4, 5}});
  EXPECT_THROW(exportToArrow(vector, arrowArray, pool_.get()), VeloxException);

  // Maps.
  auto lambda = [](vector_size_t /* row */) { return 1; };
  vector = vectorMaker_.mapVector<int64_t, int64_t>(2, lambda, lambda, lambda);
  EXPECT_THROW(exportToArrow(vector, arrowArray, pool_.get()), VeloxException);

  // Constant encoding.
  vector = BaseVector::createConstant(variant(10), 10, pool_.get());
  EXPECT_THROW(exportToArrow(vector, arrowArray, pool_.get()), VeloxException);

  // Dictionary encoding.
  BufferPtr indices = allocateIndices(3, pool_.get());
  vector = BaseVector::wrapInDictionary(
      BufferPtr(), indices, 3, vectorMaker_.flatVector<int64_t>({1, 2, 3}));
  EXPECT_THROW(exportToArrow(vector, arrowArray, pool_.get()), VeloxException);
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
    int64_t length = inputValues.size();
    int64_t nullCount = 0;

    holder.values = AlignedBuffer::allocate<T>(length, pool_.get());
    holder.nulls = AlignedBuffer::allocate<uint64_t>(length, pool_.get());

    auto rawValues = holder.values->asMutable<T>();
    auto rawNulls = holder.nulls->asMutable<uint64_t>();

    for (size_t i = 0; i < length; ++i) {
      if (inputValues[i] == std::nullopt) {
        bits::setNull(rawNulls, i);
        nullCount++;
      } else {
        bits::clearNull(rawNulls, i);
        if constexpr (std::is_same<T, bool>::value) {
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
  template <typename T>
  void testArrowImport(
      const char* format,
      const std::vector<std::optional<T>>& inputValues) {
    ArrowContextHolder holder;
    auto arrowArray = fillArrowArray(inputValues, holder);

    auto arrowSchema = makeArrowSchema(format);
    auto output = importFromArrow(arrowSchema, arrowArray, pool_.get());
    assertVectorContent(inputValues, output, arrowArray.null_count);

    // Buffer views are not reusable. Strings might need to create an additional
    // buffer, depending on the string sizes, in which case the buffers could be
    // reusable. So we don't check them in here.
    if constexpr (!std::is_same_v<T, std::string>) {
      EXPECT_FALSE(BaseVector::isReusableFlatVector(output));
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

    testArrowImport<int64_t>("l", {});
    testArrowImport<int64_t>("l", {std::nullopt});
    testArrowImport<int64_t>("l", {-99, 4, 318321631, 1211, -12});
    testArrowImport<int64_t>("l", {std::nullopt, 12345678, std::nullopt});
    testArrowImport<int64_t>("l", {std::nullopt, std::nullopt});

    testArrowImport<double>("g", {});
    testArrowImport<double>("g", {std::nullopt});
    testArrowImport<double>("g", {-99.9, 4.3, 31.1, 129.11, -12});
    testArrowImport<float>("f", {-99.9, 4.3, 31.1, 129.11, -12});
  }

  void testImportWithoutNullsBuffer() {
    std::vector<std::optional<int64_t>> inputValues = {1, 2, 3, 4, 5};
    auto length = inputValues.size();

    // Construct Arrow array without nulls value and nulls buffer
    ArrowContextHolder holder1;
    holder1.values = AlignedBuffer::allocate<int64_t>(length, pool_.get());
    auto rawValues1 = holder1.values->asMutable<int64_t>();
    for (size_t i = 0; i < length; ++i) {
      rawValues1[i] = *inputValues[i];
    }

    holder1.buffers[0] = nullptr;
    holder1.buffers[1] = (const void*)rawValues1;
    auto arrowArray1 = makeArrowArray(holder1.buffers, 2, length, 0);
    auto arrowSchema1 = makeArrowSchema("l");

    auto output = importFromArrow(arrowSchema1, arrowArray1, pool_.get());
    assertVectorContent(inputValues, output, arrowArray1.null_count);

    // However, convert from an Arrow array without nulls buffer but non-zero
    // null count should fail
    ArrowContextHolder holder2;
    holder2.values = AlignedBuffer::allocate<int64_t>(length, pool_.get());
    auto rawValues2 = holder2.values->asMutable<int64_t>();
    for (size_t i = 0; i < length; ++i) {
      rawValues2[i] = *inputValues[i];
    }

    holder2.buffers[0] = nullptr;
    holder2.buffers[1] = (const void*)rawValues2;
    auto arrowSchema2 = makeArrowSchema("l");
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

  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
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
  testImportWithoutNullsBuffer();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, string) {
  testImportString();
}

TEST_F(ArrowBridgeArrayImportAsViewerTest, row) {
  testImportRow();
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
  testImportWithoutNullsBuffer();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, string) {
  testImportString();
}

TEST_F(ArrowBridgeArrayImportAsOwnerTest, row) {
  testImportRow();
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
