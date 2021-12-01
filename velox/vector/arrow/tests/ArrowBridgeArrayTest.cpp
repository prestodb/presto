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
    auto flatVector = vectorMaker_.flatVectorNullable(inputData);
    ArrowArray arrowArray;
    exportToArrow(flatVector, arrowArray);

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

        // Boolean needs special treatment.
        if constexpr (std::is_same_v<T, bool>) {
          EXPECT_EQ(
              inputData[i],
              bits::isBitSet(reinterpret_cast<const uint64_t*>(values), i));
        } else {
          EXPECT_EQ(inputData[i], values[i]);
        }
      }
    }

    arrowArray.release(&arrowArray);
    EXPECT_EQ(nullptr, arrowArray.release);
    EXPECT_EQ(nullptr, arrowArray.private_data);
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
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
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
    exportToArrow(flatVector, arrowArray);
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

TEST_F(ArrowBridgeArrayExportTest, unsupported) {
  ArrowArray arrowArray;
  VectorPtr vector;

  // Strings.
  vector = vectorMaker_.flatVectorNullable<std::string>({});
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);

  // Timestamps.
  vector = vectorMaker_.flatVectorNullable<Timestamp>({});
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);

  // Dates.
  vector = vectorMaker_.flatVectorNullable<Date>({});
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);

  // Arrays.
  vector = vectorMaker_.arrayVector<int64_t>({{1, 2, 3}, {4, 5}});
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);

  // Maps.
  auto lambda = [](vector_size_t /* row */) { return 1; };
  vector = vectorMaker_.mapVector<int64_t, int64_t>(2, lambda, lambda, lambda);
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);

  // Row vectors.
  vector =
      vectorMaker_.rowVector({vectorMaker_.flatVector<int64_t>({1, 2, 3})});
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);

  // Constant encoding.
  vector = BaseVector::createConstant(variant(10), 10, pool_.get());
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);

  // Dictionary encoding.
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(3, pool_.get());
  vector = BaseVector::wrapInDictionary(
      BufferPtr(), indices, 3, vectorMaker_.flatVector<int64_t>({1, 2, 3}));
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);
}

class ArrowBridgeArrayImportTest : public ArrowBridgeArrayExportTest {
 protected:
  // Used by this base test class to import Arrow data and create Velox Vector.
  // Derived test classes should call the import function under test.
  virtual VectorPtr importFromArrow(
      ArrowSchema& arrowSchema,
      ArrowArray& arrowArray,
      memory::MemoryPool* pool) = 0;

  // Takes a vector with input data, generates an input ArrowArray and Velox
  // Vector (using vector maker). Then converts ArrowArray into Velox vector and
  // assert that both Velox vectors are semantically the same.
  template <typename T>
  void testArrowImport(
      const char* format,
      const std::vector<std::optional<T>>& inputValues) {
    int64_t length = inputValues.size();
    int64_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<T>(length, pool_.get());
    BufferPtr nulls = AlignedBuffer::allocate<uint64_t>(length, pool_.get());

    auto rawValues = values->asMutable<T>();
    auto rawNulls = nulls->asMutable<uint64_t>();

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

    const void* buffers[2];
    buffers[0] = (nullCount == 0) ? nullptr : (const void*)rawNulls;
    buffers[1] = (length == 0) ? nullptr : (const void*)rawValues;

    ArrowArray arrowArray = makeArrowArray(buffers, 2, length, nullCount);
    auto arrowSchema = makeArrowSchema(format);
    auto output = importFromArrow(arrowSchema, arrowArray, pool_.get());

    // Buffer views are not reusable.
    EXPECT_FALSE(BaseVector::isReusableFlatVector(output));

    EXPECT_EQ((nullCount > 0), output->mayHaveNulls());
    EXPECT_EQ(nullCount, *output->getNullCount());
    EXPECT_EQ(inputValues.size(), output->size());

    auto expected = vectorMaker_.flatVectorNullable<T>(inputValues);

    // Assert new vector contents.
    for (vector_size_t i = 0; i < length; ++i) {
      ASSERT_TRUE(expected->equalValueAt(output.get(), i, i))
          << "at " << i << ": " << expected->toString(i) << " vs. "
          << output->toString(i);
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

    // VARCHAR and VARBINARY not supported for now.
    EXPECT_THROW(
        testArrowImport<std::string>("u", {"hello world"}),
        std::invalid_argument);
    EXPECT_THROW(
        testArrowImport<std::string>("z", {"hello world"}),
        std::invalid_argument);
  }

  void testImportFailures() {
    ArrowSchema arrowSchema;
    ArrowArray arrowArray;

    const int32_t values[] = {1, 2, 3, 4};
    const void* buffers[] = {nullptr, values};

    // Unsupported:

    // Children not yet supported.
    arrowSchema = makeArrowSchema("i");
    arrowArray = makeArrowArray(buffers, 2, 4, 0);
    arrowArray.n_children = 1;
    EXPECT_THROW(
        importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);

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

TEST_F(ArrowBridgeArrayImportAsViewerTest, failures) {
  testImportFailures();
}

class ArrowBridgeArrayImportAsOwnerTest
    : public ArrowBridgeArrayImportAsViewerTest {
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
