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
static void mockRelease(ArrowSchema*) {}

class ArrowBridgeExportTest : public testing::Test {
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

  // Boiler plate structures required by vectorMaker.
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  facebook::velox::test::VectorMaker vectorMaker_{execCtx_.pool()};
};

TEST_F(ArrowBridgeExportTest, flatNotNull) {
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

TEST_F(ArrowBridgeExportTest, flatBool) {
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

TEST_F(ArrowBridgeExportTest, flatTinyint) {
  testFlatVector<int8_t>({
      1,
      std::numeric_limits<int8_t>::min(),
      std::nullopt,
      std::numeric_limits<int8_t>::max(),
      std::nullopt,
      4,
  });
}

TEST_F(ArrowBridgeExportTest, flatSmallint) {
  testFlatVector<int16_t>({
      std::numeric_limits<int16_t>::min(),
      1000,
      std::nullopt,
      std::numeric_limits<int16_t>::max(),
  });
}

TEST_F(ArrowBridgeExportTest, flatInteger) {
  testFlatVector<int32_t>({
      std::numeric_limits<int32_t>::min(),
      std::nullopt,
      std::numeric_limits<int32_t>::max(),
      std::numeric_limits<int32_t>::max(),
      std::nullopt,
      std::nullopt,
  });
}

TEST_F(ArrowBridgeExportTest, flatBigint) {
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

TEST_F(ArrowBridgeExportTest, flatReal) {
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

TEST_F(ArrowBridgeExportTest, flatDouble) {
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

TEST_F(ArrowBridgeExportTest, unsupported) {
  ArrowArray arrowArray;
  VectorPtr vector;

  // Strings.
  vector = vectorMaker_.flatVectorNullable<std::string>({});
  EXPECT_THROW(exportToArrow(vector, arrowArray), VeloxException);

  // Timestamps.
  vector = vectorMaker_.flatVectorNullable<Timestamp>({});
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

class ArrowBridgeImportTest : public ArrowBridgeExportTest {
 protected:
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

    ArrowArray arrowArray{
        .dictionary = nullptr,
        .n_children = 0,
        .children = nullptr,
        .offset = 0,
        .n_buffers = 2,
        .buffers = buffers,
        .null_count = nullCount,
        .length = length};
    ArrowSchema arrowSchema{.release = mockRelease, .format = format};
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

  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
};

TEST_F(ArrowBridgeImportTest, scalar) {
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

TEST_F(ArrowBridgeImportTest, failures) {
  ArrowSchema arrowSchema{.release = mockRelease, .format = "i"};

  const void* buffers[2];
  const int32_t values[] = {1, 2, 3, 4};
  buffers[0] = nullptr;
  buffers[1] = values;

  ArrowArray arrowArray{
      .dictionary = nullptr,
      .null_count = 0,
      .n_children = 0,
      .children = nullptr,
      .offset = 0,
      .n_buffers = 2,
      .buffers = buffers,
      .length = 4};

  // Unsupported:

  // Children not yet supported.
  arrowArray.n_children = 1;
  EXPECT_THROW(
      importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);
  arrowArray.n_children = 0;

  // Offset not yet supported.
  arrowArray.offset = 1;
  EXPECT_THROW(
      importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);
  arrowArray.offset = 0;

  // Broken input.

  // Expect two buffers.
  arrowArray.n_buffers = 1;
  EXPECT_THROW(
      importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);
  arrowArray.n_buffers = 2;

  // Can't have nulls without null buffer.
  arrowArray.null_count = 1;
  EXPECT_THROW(
      importFromArrow(arrowSchema, arrowArray, pool_.get()), VeloxUserError);
  arrowArray.null_count = 0;

  // Non-existing type.
  EXPECT_THROW(
      importFromArrow(
          ArrowSchema{.release = mockRelease, .format = "a"},
          arrowArray,
          pool_.get()),
      VeloxUserError);

  // Ensure the baseline works.
  EXPECT_NO_THROW(importFromArrow(arrowSchema, arrowArray, pool_.get()));
}

class ArrowBridgeSchemaExportTest : public ArrowBridgeExportTest {
 protected:
  void testScalarType(const TypePtr& type, const char* arrowFormat) {
    ArrowSchema arrowSchema;
    exportToArrow(type, arrowSchema);

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
    exportToArrow(type, arrowSchema);

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

TEST_F(ArrowBridgeSchemaExportTest, scalar) {
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

TEST_F(ArrowBridgeSchemaExportTest, nested) {
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

TEST_F(ArrowBridgeSchemaExportTest, unsupported) {
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

class ArrowBridgeSchemaImportTest : public ArrowBridgeExportTest {
 protected:
  TypePtr testSchemaImport(const char* format) {
    ArrowSchema arrowSchema{.release = mockRelease, .format = format};
    auto type = importFromArrow(arrowSchema);
    arrowSchema.release(&arrowSchema);
    return type;
  }

  TypePtr testSchemaImportComplex(
      const char* mainFormat,
      const std::vector<const char*>& childrenFormat,
      const std::vector<const char*>& colNames = {}) {
    std::vector<ArrowSchema> schemas;
    std::vector<ArrowSchema*> schemaPtrs;

    schemas.resize(childrenFormat.size());
    schemaPtrs.resize(childrenFormat.size());

    for (size_t i = 0; i < childrenFormat.size(); ++i) {
      schemas[i] = ArrowSchema{
          .release = mockRelease,
          .format = childrenFormat[i],
          .name = colNames.size() > i ? colNames[i] : nullptr,
      };
      schemaPtrs[i] = &schemas[i];
    }

    ArrowSchema mainSchema{
        .release = mockRelease,
        .format = mainFormat,
        .n_children = (int64_t)schemaPtrs.size(),
        .children = schemaPtrs.data(),
    };
    auto type = importFromArrow(mainSchema);
    mainSchema.release(&mainSchema);
    return type;
  }
};

TEST_F(ArrowBridgeSchemaImportTest, scalar) {
  EXPECT_EQ(*BOOLEAN(), *testSchemaImport("b"));
  EXPECT_EQ(*TINYINT(), *testSchemaImport("c"));
  EXPECT_EQ(*SMALLINT(), *testSchemaImport("s"));
  EXPECT_EQ(*INTEGER(), *testSchemaImport("i"));
  EXPECT_EQ(*BIGINT(), *testSchemaImport("l"));
  EXPECT_EQ(*REAL(), *testSchemaImport("f"));
  EXPECT_EQ(*DOUBLE(), *testSchemaImport("g"));

  EXPECT_EQ(*VARCHAR(), *testSchemaImport("u"));
  EXPECT_EQ(*VARCHAR(), *testSchemaImport("U"));
  EXPECT_EQ(*VARBINARY(), *testSchemaImport("z"));
  EXPECT_EQ(*VARBINARY(), *testSchemaImport("Z"));

  // Temporal.
  EXPECT_EQ(*TIMESTAMP(), *testSchemaImport("ttn"));
}

TEST_F(ArrowBridgeSchemaImportTest, complexTypes) {
  // Array.
  EXPECT_EQ(*ARRAY(BIGINT()), *testSchemaImportComplex("+L", {"l"}));
  EXPECT_EQ(*ARRAY(TIMESTAMP()), *testSchemaImportComplex("+L", {"ttn"}));
  EXPECT_EQ(*ARRAY(VARCHAR()), *testSchemaImportComplex("+L", {"U"}));

  // Map.
  EXPECT_EQ(
      *MAP(VARCHAR(), BOOLEAN()), *testSchemaImportComplex("+m", {"U", "b"}));
  EXPECT_EQ(
      *MAP(SMALLINT(), REAL()), *testSchemaImportComplex("+m", {"s", "f"}));

  // Row/struct.
  EXPECT_EQ(
      *ROW({SMALLINT(), REAL()}), *testSchemaImportComplex("+s", {"s", "f"}));
  EXPECT_EQ(
      *ROW({SMALLINT(), REAL(), VARCHAR(), BOOLEAN()}),
      *testSchemaImportComplex("+s", {"s", "f", "u", "b"}));

  // Named
  EXPECT_EQ(
      *ROW({"col1", "col2"}, {SMALLINT(), REAL()}),
      *testSchemaImportComplex("+s", {"s", "f"}, {"col1", "col2"}));
}

TEST_F(ArrowBridgeSchemaImportTest, unsupported) {
  EXPECT_THROW(testSchemaImport("n"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("C"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("S"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("I"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("L"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("e"), VeloxUserError);

  EXPECT_THROW(testSchemaImport("d:19,10"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("w:42"), VeloxUserError);

  EXPECT_THROW(testSchemaImport("tdD"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("tdm"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("tts"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("ttm"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("tDs"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("tiM"), VeloxUserError);

  EXPECT_THROW(testSchemaImport("+"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+l"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+b"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+z"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+u"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+w"), VeloxUserError);
}

class ArrowBridgeSchemaTest : public ArrowBridgeExportTest {
 protected:
  void roundtripTest(const TypePtr& inputType) {
    ArrowSchema arrowSchema;
    exportToArrow(inputType, arrowSchema);
    auto outputType = importFromArrow(arrowSchema);
    arrowSchema.release(&arrowSchema);
    EXPECT_EQ(*inputType, *outputType);
  }
};

TEST_F(ArrowBridgeSchemaTest, roundtrip) {
  roundtripTest(BOOLEAN());
  roundtripTest(VARCHAR());
  roundtripTest(REAL());
  roundtripTest(ARRAY(DOUBLE()));
  roundtripTest(ARRAY(ARRAY(ARRAY(ARRAY(VARBINARY())))));
  roundtripTest(MAP(VARCHAR(), REAL()));
  roundtripTest(MAP(VARCHAR(), ARRAY(BOOLEAN())));
  roundtripTest(MAP(VARCHAR(), ARRAY(MAP(ARRAY(BIGINT()), BOOLEAN()))));
  roundtripTest(ROW({VARBINARY(), TINYINT(), SMALLINT()}));
  roundtripTest(ROW({VARBINARY(), ROW({DOUBLE(), VARBINARY()}), SMALLINT()}));
  roundtripTest(ROW({
      ARRAY(VARBINARY()),
      MAP(REAL(), ARRAY(DOUBLE())),
      ROW({"a", "b"}, {DOUBLE(), VARBINARY()}),
      INTEGER(),
  }));
}

} // namespace
