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

#include <fmt/core.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>
#include <tuple>

#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

using namespace facebook::velox::test;

namespace {

template <typename T>
struct FuncPrimitivesTest {
  template <typename TOut>
  bool call(TOut& out, const int64_t& n) {
    out.template set_null_at<0>();
    out.template get_writer_at<1>() = n;
    out.template get_writer_at<2>() = n * 2;
    return true;
  }
};

class RowWriterTest : public functions::test::FunctionBaseTest {
 public:
  VectorPtr prepareResult(const TypePtr& rowType, vector_size_t size = 1) {
    VectorPtr result;
    BaseVector::ensureWritable(
        SelectivityVector(size), rowType, this->execCtx_.pool(), result);
    return result;
  }

  template <typename... T>
  void testE2EPrimitive(const std::string& testFunctionName) {
    auto functionName = "row_writer_test_" + testFunctionName;
    registerFunction<FuncPrimitivesTest, Row<T...>, int64_t>({functionName});

    auto result = evaluate(
        fmt::format("{}(c0)", functionName),
        makeRowVector(
            {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})}));

    using types = std::tuple<T...>;

    // First row is always null.
    auto vector1 = makeFlatVector<std::tuple_element_t<0, types>>(10);
    for (auto i = 0; i < 10; i++) {
      vector1->setNull(i, true);
    }

    // Second row is set to n.
    auto vector2 = makeFlatVector<std::tuple_element_t<1, types>>(10);
    for (auto i = 0; i < 10; i++) {
      vector2->set(i, i + 1);
    }

    // Third row is set to n*2.
    auto vector3 = makeFlatVector<std::tuple_element_t<2, types>>(10);
    for (auto i = 0; i < 10; i++) {
      vector3->set(i, (i + 1) * 2);
    }

    assertEqualVectors(makeRowVector({vector1, vector2, vector3}), result);
  }

  struct TestWriter {
    VectorPtr result;
    std::unique_ptr<VectorWriter<Row<int64_t, int64_t, int64_t>>> writer =
        std::make_unique<VectorWriter<Row<int64_t, int64_t, int64_t>>>();
  };

  TestWriter makeTestWriter() {
    TestWriter writer;

    writer.result = prepareResult(makeRowType({BIGINT(), BIGINT(), BIGINT()}));
    writer.writer->init(*writer.result->as<RowVector>());
    writer.writer->setOffset(0);
    return writer;
  }

  VectorPtr nullEntry() {
    return vectorMaker_.flatVectorNullable<int64_t>({std::nullopt});
  }
};

TEST_F(RowWriterTest, setNullAt) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& rowWriter = vectorWriter->current();

  rowWriter.set_null_at<0>();
  rowWriter.set_null_at<1>();
  rowWriter.set_null_at<2>();

  vectorWriter->commit();

  auto expected = makeRowVector({nullEntry(), nullEntry(), nullEntry()});

  assertEqualVectors(result, expected);
}

TEST_F(RowWriterTest, getWriterAt) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& rowWriter = vectorWriter->current();

  rowWriter.get_writer_at<0>() = 1;
  rowWriter.set_null_at<1>();
  rowWriter.get_writer_at<2>() = 3;

  vectorWriter->commit();

  auto expected = makeRowVector(
      {vectorMaker_.flatVector<int64_t>({1}),
       nullEntry(),
       vectorMaker_.flatVector<int64_t>({3})},
      [](auto row) { return row == 1; });

  assertEqualVectors(expected, result);
}

TEST_F(RowWriterTest, e2ePrimitives) {
  testE2EPrimitive<int8_t, int8_t, int8_t>("row_writer_f_int8");
  testE2EPrimitive<int16_t, int16_t, int16_t>("row_writer_f_int16");
  testE2EPrimitive<int32_t, int32_t, int32_t>("row_writer_f_int32");
  testE2EPrimitive<int64_t, int64_t, int64_t>("row_writer_f_int64");
  testE2EPrimitive<float, float, float>("row_writer_f_float");
  testE2EPrimitive<double, double, double>("row_writer_f_double");
  testE2EPrimitive<bool, bool, bool>("row_writer_f_bool");
  testE2EPrimitive<double, float, bool>("row_writer_f_mix1");
  testE2EPrimitive<int32_t, int64_t, double>("row_writer_f_mix2");
}

TEST_F(RowWriterTest, timeStamp) {
  auto result = prepareResult(makeRowType({TIMESTAMP()}), 2);

  VectorWriter<Row<Timestamp>> vectorWriter;
  vectorWriter.init(*result->as<RowVector>());

  {
    vectorWriter.setOffset(0);
    auto& rowWriter = vectorWriter.current();
    rowWriter.get_writer_at<0>() = Timestamp::fromMillis(11);
    vectorWriter.commit();
  }

  {
    vectorWriter.setOffset(1);
    auto& rowWriter = vectorWriter.current();
    rowWriter.set_null_at<0>();
    vectorWriter.commit();
  }

  auto expected = vectorMaker_.flatVectorNullable<Timestamp>(
      {{Timestamp::fromMillis(11), std::nullopt}});
  assertEqualVectors(result, makeRowVector({expected}));
}

TEST_F(RowWriterTest, varChar) {
  auto result = prepareResult(makeRowType({VARCHAR()}), 2);

  VectorWriter<Row<Varchar>> vectorWriter;
  vectorWriter.init(*result->as<RowVector>());

  {
    vectorWriter.setOffset(0);
    auto& rowWriter = vectorWriter.current();
    rowWriter.get_writer_at<0>().append("hi");
    vectorWriter.commit();
  }

  {
    vectorWriter.setOffset(1);
    auto& rowWriter = vectorWriter.current();
    rowWriter.set_null_at<0>();
    vectorWriter.commit();
  }

  auto expected =
      vectorMaker_.flatVectorNullable<StringView>({{"hi"_sv, std::nullopt}});
  assertEqualVectors(result, makeRowVector({expected}));
}

TEST_F(RowWriterTest, varBinary) {
  auto result = prepareResult(makeRowType({VARBINARY()}), 2);

  VectorWriter<Row<Varchar>> vectorWriter;
  vectorWriter.init(*result->as<RowVector>());

  {
    vectorWriter.setOffset(0);
    auto& rowWriter = vectorWriter.current();
    rowWriter.get_writer_at<0>().append("hi");
    vectorWriter.commit();
  }

  {
    vectorWriter.setOffset(1);
    auto& rowWriter = vectorWriter.current();
    rowWriter.set_null_at<0>();
    vectorWriter.commit();
  }

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(result->size());
  decoded.decode(*result, rows);
  VectorReader<Row<Varbinary>> reader(&decoded);
  ASSERT_EQ(exec::get<0>(reader[0]), "hi"_sv);
  ASSERT_EQ(exec::get<0>(reader[1]), std::nullopt);
}

TEST_F(RowWriterTest, assignToTuple) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& rowWriter = vectorWriter->current();

  rowWriter = std::make_tuple(std::nullopt, std::optional<int64_t>(), 2);

  vectorWriter->commit();

  auto expected = makeRowVector(
      {nullEntry(), nullEntry(), vectorMaker_.flatVector<int64_t>({2})});

  assertEqualVectors(result, expected);
}

TEST_F(RowWriterTest, execGet) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& rowWriter = vectorWriter->current();

  exec::get<0>(rowWriter) = std::nullopt;
  exec::get<1>(rowWriter) = std::optional<int64_t>();
  exec::get<2>(rowWriter) = 2;

  vectorWriter->commit();

  auto expected = makeRowVector(
      {nullEntry(), nullEntry(), vectorMaker_.flatVector<int64_t>({2})});

  assertEqualVectors(result, expected);
}

TEST_F(RowWriterTest, nested) {
  // Output is row(row(int, string), double).
  auto result = prepareResult(
      makeRowType({makeRowType({BIGINT(), VARCHAR()}), DOUBLE()}));

  VectorWriter<Row<Row<int64_t, Varchar>, double>> vectorWriter;
  vectorWriter.init(*result->as<RowVector>());

  vectorWriter.setOffset(0);
  auto& outerRowWriter = vectorWriter.current();

  auto& innerRowWriter = outerRowWriter.get_writer_at<0>();
  innerRowWriter.get_writer_at<0>() = 1;
  innerRowWriter.get_writer_at<1>().append("hi");

  outerRowWriter.get_writer_at<1>() = 1.1;
  vectorWriter.commit();

  auto expected1 = vectorMaker_.flatVectorNullable<int64_t>({{1}});
  auto expected2 = vectorMaker_.flatVectorNullable<StringView>({{"hi"_sv}});
  auto expected3 = vectorMaker_.flatVectorNullable<double>({{1.1}});
  assertEqualVectors(
      result,
      makeRowVector({makeRowVector({expected1, expected2}), expected3}));
}

TEST_F(RowWriterTest, rowOfArray) {
  auto result = prepareResult(makeRowType({ARRAY(BIGINT())}));

  VectorWriter<Row<Array<int64_t>>> vectorWriter;
  vectorWriter.init(*result->as<RowVector>());

  vectorWriter.setOffset(0);

  auto& rowWriter = vectorWriter.current();
  auto& arrayWriter = rowWriter.get_writer_at<0>();
  arrayWriter.push_back(1);
  arrayWriter.push_back(2);
  arrayWriter.push_back(3);

  vectorWriter.commit();

  auto expected = vectorMaker_.arrayVector<int64_t>({{1, 2, 3}});
  assertEqualVectors(result, makeRowVector({expected}));
}

TEST_F(RowWriterTest, arrayOfRows) {
  auto result = prepareResult(ARRAY(makeRowType({BIGINT(), BIGINT()})));

  VectorWriter<Array<Row<int64_t, int64_t>>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());

  vectorWriter.setOffset(0);

  auto& arrayWriter = vectorWriter.current();
  {
    auto& rowWriter = arrayWriter.add_item();
    rowWriter.set_null_at<0>();
    rowWriter.get_writer_at<1>() = 1;
  }
  arrayWriter.add_null();
  {
    auto& rowWriter = arrayWriter.add_item();
    rowWriter.set_null_at<1>();
    rowWriter.get_writer_at<0>() = 1;
  }

  vectorWriter.commit();

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(result->size());
  decoded.decode(*result, rows);
  VectorReader<Array<Row<int64_t, int64_t>>> reader(&decoded);

  auto arrayView = reader[0];
  ASSERT_EQ(arrayView.size(), 3);

  ASSERT_TRUE(arrayView[0].has_value());
  ASSERT_FALSE(arrayView[1].has_value());
  ASSERT_TRUE(arrayView[2].has_value());

  auto row0 = arrayView[0].value();
  ASSERT_FALSE(exec::get<0>(row0).has_value());
  ASSERT_EQ(exec::get<1>(row0).value(), 1);

  auto row2 = arrayView[2].value();
  ASSERT_EQ(exec::get<0>(row2).value(), 1);
  ASSERT_FALSE(exec::get<1>(row2).has_value());
}

TEST_F(RowWriterTest, commitNull) {
  auto result = prepareResult(ARRAY(makeRowType({BIGINT(), BIGINT()})), 2);

  VectorWriter<Array<Row<int64_t, int64_t>>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());

  {
    vectorWriter.setOffset(0);

    auto& arrayWriter = vectorWriter.current();
    {
      auto& rowWriter = arrayWriter.add_item();
      rowWriter.set_null_at<0>();
      rowWriter.get_writer_at<1>() = 1;
    }

    arrayWriter.add_null();

    {
      auto& rowWriter = arrayWriter.add_item();
      rowWriter.set_null_at<1>();
      rowWriter.get_writer_at<0>() = 1;
    }

    // The three items added to the array shall not be committed and shall be
    // disregarded when writing the next row.
    vectorWriter.commitNull();
  }

  {
    vectorWriter.setOffset(1);
    auto& arrayWriter = vectorWriter.current();
    {
      auto& rowWriter = arrayWriter.add_item();
      rowWriter.set_null_at<0>();
      rowWriter.get_writer_at<1>() = 1;
    }
    vectorWriter.commit();
  }

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(result->size());
  decoded.decode(*result, rows);
  VectorReader<Array<Row<int64_t, int64_t>>> reader(&decoded);

  ASSERT_FALSE(reader.isSet(0));
  ASSERT_TRUE(reader.isSet(1));
  auto arrayView = reader[1];
  ASSERT_EQ(arrayView.size(), 1);

  ASSERT_TRUE(arrayView[0].has_value());
  auto row0 = arrayView[0].value();

  ASSERT_FALSE(exec::get<0>(row0).has_value());
  ASSERT_EQ(exec::get<1>(row0).value(), 1);
}

TEST_F(RowWriterTest, copyFromSimple) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& rowWriter = vectorWriter->current();

  rowWriter.copy_from(
      std::make_tuple(std::nullopt, std::optional<int64_t>(), 2));

  vectorWriter->commit();

  auto expected = makeRowVector(
      {nullEntry(), nullEntry(), vectorMaker_.flatVector<int64_t>({2})});

  assertEqualVectors(result, expected);
}

TEST_F(RowWriterTest, copyFromRowOfArrays) {
  auto result = prepareResult(
      makeRowType({ARRAY(BIGINT()), ARRAY(DOUBLE()), ARRAY(BOOLEAN())}));

  VectorWriter<Row<Array<int64_t>, Array<double>, Array<bool>>> vectorWriter;
  vectorWriter.init(*result->as<RowVector>());

  vectorWriter.setOffset(0);

  auto& rowWriter = vectorWriter.current();

  std::vector<std::optional<int64_t>> expected1 = {1, std::nullopt, 2};
  std::vector<std::optional<double>> expected2 = {
      0.7, std::nullopt, std::nullopt};
  std::vector<std::optional<bool>> expected3 = {std::nullopt, true, false};

  rowWriter.copy_from(std::make_tuple(expected1, expected2, expected3));

  vectorWriter.commit();

  assertEqualVectors(
      result,
      makeRowVector({
          makeNullableArrayVector(
              std::vector<std::vector<std::optional<int64_t>>>{expected1}),
          makeNullableArrayVector(
              std::vector<std::vector<std::optional<double>>>{expected2}),
          makeNullableArrayVector(
              std::vector<std::vector<std::optional<bool>>>{expected3}),
      }));
}

TEST_F(RowWriterTest, copyFromArrayOfRow) {
  auto result = prepareResult(ARRAY(makeRowType({BIGINT(), BIGINT()})));

  VectorWriter<Array<Row<int64_t, int64_t>>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());
  vectorWriter.setOffset(0);

  auto& rowWriter = vectorWriter.current();

  std::vector<std::tuple<int64_t, int64_t>> data{{2, 1}, {1, 2}};
  rowWriter.copy_from(data);
  vectorWriter.commit();

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(result->size());
  decoded.decode(*result, rows);
  VectorReader<Array<Row<int64_t, int64_t>>> reader(&decoded);

  auto arrayView = reader[0];
  ASSERT_EQ(arrayView.size(), 2);

  ASSERT_TRUE(arrayView[0].has_value());
  ASSERT_TRUE(arrayView[1].has_value());

  auto row0 = arrayView[0].value();
  ASSERT_EQ(exec::get<0>(row0).value(), 2);
  ASSERT_EQ(exec::get<1>(row0).value(), 1);

  auto row1 = arrayView[1].value();
  ASSERT_EQ(exec::get<0>(row1).value(), 1);
  ASSERT_EQ(exec::get<1>(row1).value(), 2);
}

// Make sure nested vectors are resized to actual size after writing.
TEST_F(RowWriterTest, finishPostSize) {
  using out_t = Row<Array<int32_t>, Map<int64_t, int64_t>>;

  auto result = prepareResult(CppToType<out_t>::create());

  exec::VectorWriter<out_t> vectorWriter;
  vectorWriter.init(*result->as<RowVector>());
  vectorWriter.setOffset(0);

  auto& rowWriter = vectorWriter.current();
  auto& arrayWriter = rowWriter.get_writer_at<0>();
  auto& mapWriter = rowWriter.get_writer_at<1>();

  arrayWriter.resize(10);
  mapWriter.resize(11);

  vectorWriter.commit();
  vectorWriter.finish();

  ASSERT_EQ(
      result->as<RowVector>()
          ->childAt(0)
          ->as<ArrayVector>()
          ->elements()
          ->size(),
      10);

  ASSERT_EQ(
      result->as<RowVector>()->childAt(1)->as<MapVector>()->mapKeys()->size(),
      11);

  ASSERT_EQ(
      result->as<RowVector>()->childAt(1)->as<MapVector>()->mapValues()->size(),
      11);
}

} // namespace
} // namespace facebook::velox::exec
