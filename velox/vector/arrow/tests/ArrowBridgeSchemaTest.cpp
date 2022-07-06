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

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "velox/common/base/Nulls.h"
#include "velox/vector/arrow/Bridge.h"

namespace {

using namespace facebook::velox;
static void mockRelease(ArrowSchema*) {}

class ArrowBridgeSchemaExportTest : public testing::Test {
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

    verifyNestedType(type, &arrowSchema);

    arrowSchema.release(&arrowSchema);
    EXPECT_EQ(nullptr, arrowSchema.release);
    EXPECT_EQ(nullptr, arrowSchema.private_data);
  }

  void verifyNestedType(const TypePtr& type, ArrowSchema* schema) {
    if (type->kind() == TypeKind::ARRAY) {
      EXPECT_EQ(std::string{"+l"}, std::string{schema->format});
    } else if (type->kind() == TypeKind::MAP) {
      EXPECT_EQ(std::string{"+m"}, std::string{schema->format});
      ASSERT_EQ(schema->n_children, 1);
      schema = schema->children[0];
    } else if (type->kind() == TypeKind::ROW) {
      EXPECT_EQ(std::string{"+s"}, std::string{schema->format});
    }
    // Scalar type.
    else {
      EXPECT_EQ(nullptr, schema->children);
    }
    ASSERT_EQ(type->size(), schema->n_children);

    // Recurse down the children.
    for (size_t i = 0; i < type->size(); ++i) {
      verifyNestedType(type->childAt(i), schema->children[i]);

      // If this is a rowType, assert that the children returned with the
      // correct name set.
      if (auto rowType = std::dynamic_pointer_cast<const RowType>(type)) {
        EXPECT_EQ(rowType->nameOf(i), std::string(schema->children[i]->name));
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
        .release = mockRelease,
        .private_data = nullptr,
    };
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
  testScalarType(DATE(), "tdD");
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

class ArrowBridgeSchemaImportTest : public ArrowBridgeSchemaExportTest {
 protected:
  TypePtr testSchemaImport(const char* format) {
    auto arrowSchema = makeArrowSchema(format);
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
      schemas[i] = makeArrowSchema(childrenFormat[i]);
      if (colNames.size() > i) {
        schemas[i].name = colNames[i];
      }
      schemaPtrs[i] = &schemas[i];
    }

    auto mainSchema = makeArrowSchema(mainFormat);
    if (strcmp(mainFormat, "+m") == 0) {
      // Arrow wraps key and value in a struct.
      auto child = makeArrowSchema("+s");
      auto children = &child;
      child.n_children = schemaPtrs.size();
      child.children = schemaPtrs.data();
      mainSchema.n_children = 1;
      mainSchema.children = &children;
      return importFromArrow(mainSchema);
    } else {
      mainSchema.n_children = (int64_t)schemaPtrs.size();
      mainSchema.children = schemaPtrs.data();
      return importFromArrow(mainSchema);
    }
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
  EXPECT_EQ(*DATE(), *testSchemaImport("tdD"));
}

TEST_F(ArrowBridgeSchemaImportTest, complexTypes) {
  // Array.
  EXPECT_EQ(*ARRAY(BIGINT()), *testSchemaImportComplex("+l", {"l"}));
  EXPECT_EQ(*ARRAY(TIMESTAMP()), *testSchemaImportComplex("+l", {"ttn"}));
  EXPECT_EQ(*ARRAY(DATE()), *testSchemaImportComplex("+l", {"tdD"}));
  EXPECT_EQ(*ARRAY(VARCHAR()), *testSchemaImportComplex("+l", {"U"}));

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

  EXPECT_THROW(testSchemaImport("tdm"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("tts"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("ttm"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("tDs"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("tiM"), VeloxUserError);

  EXPECT_THROW(testSchemaImport("+"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+L"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+b"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+z"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+u"), VeloxUserError);
  EXPECT_THROW(testSchemaImport("+w"), VeloxUserError);
}

class ArrowBridgeSchemaTest : public testing::Test {
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

TEST_F(ArrowBridgeSchemaTest, validateInArrow) {
  const std::pair<TypePtr, std::shared_ptr<arrow::DataType>> kTypes[] = {
      {BOOLEAN(), arrow::boolean()},
      {VARCHAR(), arrow::utf8()},
      {ARRAY(DOUBLE()), arrow::list(arrow::float64())},
      {ARRAY(ARRAY(DOUBLE())), arrow::list(arrow::list(arrow::float64()))},
      {MAP(VARCHAR(), REAL()), arrow::map(arrow::utf8(), arrow::float32())},
      {ROW({"c0", "c1"}, {VARCHAR(), BIGINT()}),
       arrow::struct_(
           {arrow::field("c0", arrow::utf8()),
            arrow::field("c1", arrow::int64())})},
  };
  for (auto& [tv, ta] : kTypes) {
    VLOG(1) << "Validating conversion between " << tv->toString() << " and "
            << ta->ToString();
    ArrowSchema schema;
    exportToArrow(tv, schema);
    ASSERT_OK_AND_ASSIGN(auto actual, arrow::ImportType(&schema));
    ASSERT_FALSE(schema.release);
    EXPECT_EQ(*actual, *ta);
  }
}

} // namespace
