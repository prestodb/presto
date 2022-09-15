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
#include "velox/duckdb/functions/DuckFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

class BaseDuckTest : public functions::test::FunctionBaseTest {
 public:
  void SetUp() override {
    duckdb::registerDuckdbFunctions();
    FunctionBaseTest::SetUp();
  }

 protected:
  template <typename IN>
  VectorPtr createFlatVector(
      const std::shared_ptr<const Type>& type,
      const std::vector<IN>& input) {
    auto column = BaseVector::create(type, input.size(), execCtx_.pool());
    auto data = column->template as<FlatVector<IN>>()->mutableRawValues();
    for (auto i = 0; i < input.size(); ++i) {
      data[i] = input[i];
    }
    return column;
  }

  template <typename IN>
  VectorPtr createDictionaryVector(
      VectorPtr flatVector,
      const std::vector<size_t>& indices) {
    // set up the indices buffer
    BufferPtr indicesBuffer =
        AlignedBuffer::allocate<vector_size_t>(indices.size(), execCtx_.pool());
    auto rawIndices = indicesBuffer->asMutable<vector_size_t>();
    for (auto i = 0; i < indices.size(); i++) {
      rawIndices[i] = indices[i];
    }
    return BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indicesBuffer, indices.size(), flatVector);
  }

  template <typename IN>
  VectorPtr
  createConstantVector(VectorPtr flatVector, size_t index, size_t length = 1) {
    return BaseVector::wrapInConstant(length, index, flatVector);
  }

  template <typename IN, typename OUT>
  void runFlatDuckTest(
      const std::shared_ptr<const Type>& type,
      const std::string& query,
      const std::vector<IN>& input,
      const std::vector<OUT>& output) {
    // Create input by using the passed input vector
    auto column = createFlatVector<IN>(type, input);

    // Call evaluate to run the query on the created input
    auto result = evaluate<SimpleVector<OUT>>(query, makeRowVector({column}));
    for (auto i = 0; i < output.size(); i++) {
      ASSERT_EQ(result->valueAt(i), output[i]);
    }

    // now we run the same test, but pass some nulls
    for (auto i = 0; i < output.size(); i++) {
      if (i % 3 == 0) {
        column->setNull(i, true);
      }
    }
    auto result2 = evaluate<SimpleVector<OUT>>(query, makeRowVector({column}));
    for (auto i = 0; i < output.size(); i++) {
      if (i % 3 == 0) {
        ASSERT_EQ(result2->isNullAt(i), true);
      } else {
        ASSERT_EQ(result2->valueAt(i), output[i]);
      }
    }
  }

  template <typename IN, typename OUT>
  void runDictionaryDuckTest(
      const std::shared_ptr<const Type>& type,
      const std::string& query,
      const std::vector<size_t>& indices,
      const std::vector<IN>& input,
      const std::vector<OUT>& output) {
    auto flatVector = createFlatVector<IN>(type, input);
    auto dictionary = createDictionaryVector<IN>(flatVector, indices);

    // Call evaluate to run the query on the created input
    auto result =
        evaluate<SimpleVector<OUT>>(query, makeRowVector({dictionary}));
    for (auto i = 0; i < output.size(); i++) {
      ASSERT_EQ(result->valueAt(i), output[i]);
    }
  }

  template <typename IN, typename OUT>
  void runConstantDuckTest(
      const std::shared_ptr<const Type>& type,
      const std::string& query,
      const std::vector<IN>& input,
      const std::vector<OUT>& output) {
    auto flatVector = createFlatVector<IN>(type, input);
    for (size_t i = 0; i < input.size(); i++) {
      auto constantVector = createConstantVector<IN>(flatVector, i, 1);
      // Call evaluate to run the query on the created input
      auto result =
          evaluate<SimpleVector<OUT>>(query, makeRowVector({constantVector}));
      ASSERT_EQ(result->valueAt(0), output[i]);

      // now pass a null
      flatVector->setNull(i, true);
      auto constantVector2 = createConstantVector<IN>(flatVector, i, 1);
      auto result2 =
          evaluate<SimpleVector<OUT>>(query, makeRowVector({constantVector2}));
      ASSERT_EQ(result2->isNullAt(0), true);
    }
  }

  template <typename IN, typename OUT>
  void runDuckTest(
      const std::shared_ptr<const Type>& type,
      const std::string& query,
      const std::vector<IN>& input,
      const std::vector<OUT>& output) {
    // run a simple flat vector test using the full input/output
    runFlatDuckTest<IN, OUT>(type, query, input, output);

    // run a dictionary test using (1) only the even indices, (2) only the odd
    // indices
    std::vector<size_t> evenIndices, oddIndices;
    std::vector<OUT> evenOutput, oddOutput;
    for (auto i = 0; i < input.size(); i++) {
      if (i % 2 == 0) {
        evenIndices.push_back(i);
        evenOutput.push_back(output[i]);
      } else {
        oddIndices.push_back(i);
        oddOutput.push_back(output[i]);
      }
    }
    runDictionaryDuckTest<IN, OUT>(type, query, evenIndices, input, evenOutput);
    runDictionaryDuckTest<IN, OUT>(type, query, oddIndices, input, oddOutput);

    // run constant duck tests
    runConstantDuckTest<IN, OUT>(type, query, input, output);
  }

  template <typename OUT>
  void compareResult(VectorPtr result, const std::vector<OUT>& output) {
    SelectivityVector sel(result->size());
    sel.setAll();
    DecodedVector decoded;
    decoded.decode(*result, sel, false);
    auto indices = decoded.indices();
    auto data = decoded.data<OUT>();
    for (size_t i = 0; i < output.size(); i++) {
      ASSERT_EQ(data[indices[i]], output[i]);
    }
  }

  template <typename IN1, typename IN2, typename OUT>
  void runDecodedDuckTestBinary(
      const std::shared_ptr<const Type>& type1,
      const std::shared_ptr<const Type>& type2,
      const std::string& query,
      const std::vector<IN1>& input1,
      const std::vector<IN2>& input2,
      const std::vector<OUT>& output) {
    // flat test
    auto flat1 = createFlatVector<IN1>(type1, input1);
    auto flat2 = createFlatVector<IN2>(type2, input2);
    auto result = evaluate<BaseVector>(query, makeRowVector({flat1, flat2}));
    compareResult<OUT>(result, output);

    // dictionary test
    std::vector<size_t> evenIndices, oddIndices;
    std::vector<IN1> evenIn;
    std::vector<IN2> oddIn;
    std::vector<OUT> evenOutput, oddOutput;
    for (auto i = 0; i < input1.size(); i++) {
      if (i % 2 == 0) {
        evenIndices.push_back(i);
        evenIn.push_back(input1[i]);
        evenOutput.push_back(output[i]);
      } else {
        oddIndices.push_back(i);
        oddIn.push_back(input2[i]);
        oddOutput.push_back(output[i]);
      }
    }
    // run scenario 1
    auto s1Input1 = createFlatVector<IN1>(type1, evenIn);
    auto s1Input2Flat = createFlatVector<IN2>(type2, input2);
    auto s1Input2 = createDictionaryVector<IN2>(s1Input2Flat, evenIndices);
    auto result2 =
        evaluate<BaseVector>(query, makeRowVector({s1Input1, s1Input2}));
    compareResult<OUT>(result2, evenOutput);
    // run scenario 2
    auto s2Input1Flat = createFlatVector<IN1>(type1, input1);
    auto s2Input1 = createDictionaryVector<IN2>(s2Input1Flat, oddIndices);
    auto s2Input2 = createFlatVector<IN2>(type2, oddIn);
    auto result3 =
        evaluate<BaseVector>(query, makeRowVector({s2Input1, s2Input2}));
    compareResult<OUT>(result3, oddOutput);
  }

  template <typename IN1, typename IN2, typename OUT>
  void runFlatDuckTestBinary(
      const std::shared_ptr<const Type>& type1,
      const std::shared_ptr<const Type>& type2,
      const std::string& query,
      const std::vector<IN1>& input1,
      const std::vector<IN2>& input2,
      const std::vector<OUT>& output) {
    auto flat1 = createFlatVector<IN1>(type1, input1);
    auto flat2 = createFlatVector<IN2>(type2, input2);
    auto result =
        evaluate<SimpleVector<OUT>>(query, makeRowVector({flat1, flat2}));
    for (size_t i = 0; i < result->size(); i++) {
      ASSERT_EQ(result->valueAt(i), output[i]);
    }
    // now we run the same test, but we set some nulls
    for (auto i = 0; i < input1.size(); i++) {
      if (i % 3 == 0) {
        flat1->setNull(i, true);
      } else if (i % 3 == 1) {
        flat2->setNull(i, true);
      }
    }
    auto result2 =
        evaluate<SimpleVector<OUT>>(query, makeRowVector({flat1, flat2}));
    for (size_t i = 0; i < result2->size(); i++) {
      if (i % 3 == 0 || i % 3 == 1) {
        ASSERT_EQ(result2->isNullAt(i), true);
      } else {
        ASSERT_EQ(result2->valueAt(i), output[i]);
      }
    }
  }

  template <typename IN1, typename IN2, typename OUT>
  void runConstantDuckTestBinary(
      const std::shared_ptr<const Type>& type1,
      const std::shared_ptr<const Type>& type2,
      const std::string& query,
      const std::vector<IN1>& input1,
      const std::vector<IN2>& input2,
      const std::vector<OUT>& output) {
    auto flat1 = createFlatVector<IN1>(type1, input1);
    auto flat2 = createFlatVector<IN2>(type2, input2);
    auto increment = input1.size() >= 20 ? input1.size() / 20 : 1;
    for (size_t i = 0; i < input1.size(); i += increment) {
      auto constantVector1 = createConstantVector<IN1>(flat1, i, 1);
      auto constantVector2 = createConstantVector<IN2>(flat2, i, 1);
      // Call evaluate to run the query on the created input
      auto result = evaluate<SimpleVector<OUT>>(
          query, makeRowVector({constantVector1, constantVector2}));
      ASSERT_EQ(result->valueAt(0), output[i]);

      // now pass a null
      flat1->setNull(i, true);
      constantVector1 = createConstantVector<IN1>(flat1, i, 1);
      auto result2 = evaluate<SimpleVector<OUT>>(
          query, makeRowVector({constantVector1, constantVector2}));
      ASSERT_EQ(result2->isNullAt(0), true);
    }
  }

  template <typename IN1, typename IN2, typename OUT>
  void runDictionaryDuckTestBinary(
      const std::shared_ptr<const Type>& type1,
      const std::shared_ptr<const Type>& type2,
      const std::string& query,
      const std::vector<IN1>& input1,
      const std::vector<IN2>& input2,
      const std::vector<OUT>& output) {
    ASSERT_EQ(input1.size(), input2.size());
    ASSERT_EQ(input1.size(), output.size());
    // we are going to run two tests:
    // (1) input1 is a flat vector, input2 is a dictionary vector
    // (2) input1 is a dictionary vector, input2 is a flat vector
    // we are going to run (1) with even indices, and (2) with odd indices
    std::vector<size_t> evenIndices, oddIndices;
    std::vector<IN1> evenIn;
    std::vector<IN2> oddIn;
    std::vector<OUT> evenOutput, oddOutput;
    for (auto i = 0; i < input1.size(); i++) {
      if (i % 2 == 0) {
        evenIndices.push_back(i);
        evenIn.push_back(input1[i]);
        evenOutput.push_back(output[i]);
      } else {
        oddIndices.push_back(i);
        oddIn.push_back(input2[i]);
        oddOutput.push_back(output[i]);
      }
    }
    // run scenario 1
    auto s1Input1 = createFlatVector<IN1>(type1, evenIn);
    auto s1Input2Flat = createFlatVector<IN2>(type2, input2);
    auto s1Input2 = createDictionaryVector<IN2>(s1Input2Flat, evenIndices);

    auto result =
        evaluate<SimpleVector<OUT>>(query, makeRowVector({s1Input1, s1Input2}));
    ASSERT_EQ(result->size(), evenOutput.size());
    for (size_t i = 0; i < result->size(); i++) {
      ASSERT_EQ(result->valueAt(i), evenOutput[i]);
    }
    // run scenario 2
    auto s2Input1Flat = createFlatVector<IN1>(type1, input1);
    auto s2Input1 = createDictionaryVector<IN1>(s2Input1Flat, oddIndices);
    auto s2Input2 = createFlatVector<IN2>(type2, oddIn);

    auto result2 =
        evaluate<SimpleVector<OUT>>(query, makeRowVector({s2Input1, s2Input2}));
    ASSERT_EQ(result2->size(), oddOutput.size());
    for (size_t i = 0; i < result2->size(); i++) {
      ASSERT_EQ(result2->valueAt(i), oddOutput[i]);
    }
  }

  template <typename IN1, typename IN2, typename OUT>
  void runDuckTestBinary(
      const std::shared_ptr<const Type>& type1,
      const std::shared_ptr<const Type>& type2,
      const std::string& query,
      const std::vector<IN1>& input1,
      const std::vector<IN2>& input2,
      const std::vector<OUT>& output) {
    runFlatDuckTestBinary<IN1, IN2, OUT>(
        type1, type2, query, input1, input2, output);
    runConstantDuckTestBinary<IN1, IN2, OUT>(
        type1, type2, query, input1, input2, output);
    runDictionaryDuckTestBinary<IN1, IN2, OUT>(
        type1, type2, query, input1, input2, output);
  }
};

TEST_F(BaseDuckTest, mod) {
  // test different overloads
  runDuckTest<int8_t, int8_t>(
      TINYINT(),
      "duckdb_mod(c0, CAST(2 AS TINYINT))",
      {0, 1, 2, 3, 4},
      {0, 1, 0, 1, 0});

  runDuckTest<int16_t, int16_t>(
      SMALLINT(),
      "duckdb_mod(c0, CAST(2 AS SMALLINT))",
      {0, 1, 2, 3, 4},
      {0, 1, 0, 1, 0});

  runDuckTest<int32_t, int32_t>(
      INTEGER(),
      "duckdb_mod(c0, CAST(2 AS INTEGER))",
      {0, 1, 2, 3, 4},
      {0, 1, 0, 1, 0});

  runDuckTest<int64_t, int64_t>(
      BIGINT(),
      "duckdb_mod(c0, CAST(2 AS BIGINT))",
      {0, 1, 2, 3, 4},
      {0, 1, 0, 1, 0});

  runDuckTest<double, double>(
      DOUBLE(),
      "duckdb_mod(c0, CAST(3.1 AS DOUBLE))",
      {0, 1.0, 4.2},
      {0, 1.0, 1.1});
}

TEST_F(BaseDuckTest, modBinary) {
  runDuckTestBinary<int32_t, int32_t, int32_t>(
      INTEGER(),
      INTEGER(),
      "duckdb_mod(c0, c1)",
      {0, 1, 2, 27, 4},
      {2, 3, 7, 3, 3},
      {0, 1, 2, 0, 1});

  runDuckTestBinary<int32_t, int16_t, int32_t>(
      INTEGER(),
      SMALLINT(),
      "duckdb_mod(c0, c1)",
      {0, 1, 2, 27, 4},
      {2, 3, 7, 3, 3},
      {0, 1, 2, 0, 1});
}

TEST_F(BaseDuckTest, modCaseTest) {
  runDecodedDuckTestBinary<int32_t, int32_t, int32_t>(
      INTEGER(),
      INTEGER(),
      "if(c0 < 1, c1%cast(2 as integer), c1%cast(3 as integer))",
      {0, 1, 1, 1, 0},
      {6, 4, 7, 26, 26},
      {0, 1, 1, 2, 0});
  runDecodedDuckTestBinary<int32_t, int32_t, int32_t>(
      INTEGER(),
      INTEGER(),
      "if(c0 < 1, duckdb_mod(c1, cast(2 as integer)), duckdb_mod(c1, cast(3 as integer)))",
      {0, 0, 1, 1, 0},
      {6, 3, 7, 26, 26},
      {0, 1, 1, 2, 0});
}

TEST_F(BaseDuckTest, modAutoCast) {
  // test auto cast in function
  // (int32_t, int8_t) gets cast to int32_t
  runDuckTest<int8_t, int32_t>(
      TINYINT(),
      "duckdb_mod(c0, CAST(2 AS INTEGER))",
      {0, 1, 2, 3, 4},
      {0, 1, 0, 1, 0});

  // (int8_t, int32_t) gets cast to int32_t
  runDuckTest<int32_t, int32_t>(
      INTEGER(),
      "duckdb_mod(c0, CAST(2 AS TINYINT))",
      {0, 1, 2, 3, 4},
      {0, 1, 0, 1, 0});
}

TEST_F(BaseDuckTest, bigMix) {
  // test mix of velox functions and duckdb functions
  runDuckTestBinary<double, double, double>(
      DOUBLE(),
      DOUBLE(),
      "duckdb_floor(duckdb_mod(cast(ceil(c0) as integer), cast(floor(c1) as integer)) + 0.5)",
      {1.2, 4.7, 39.8, 88, 300},
      {1.2, 2.1, 7.3, 23.22, 101.77},
      {0, 1, 5, 19, 98});
}

TEST_F(BaseDuckTest, largeVector) {
  static constexpr int largeVectorSize = 2000;
  std::vector<int32_t> input1, input2, expectedOutput;
  input1.reserve(largeVectorSize);
  input2.reserve(largeVectorSize);
  expectedOutput.reserve(largeVectorSize);
  for (auto i = 0; i < largeVectorSize; i++) {
    int32_t i1 = i;
    int32_t i2 = 1 + ((i * 397) % 7);
    int32_t out = i1 % i2;
    input1.push_back(i1);
    input2.push_back(i2);
    expectedOutput.push_back(out);
  }
  // large vector test
  runDuckTestBinary<int32_t, int32_t, int32_t>(
      INTEGER(),
      INTEGER(),
      "duckdb_mod(c0, c1)",
      input1,
      input2,
      expectedOutput);

  // large vector + auto cast
  std::vector<int16_t> input2I16;
  for (auto& entry : input2) {
    input2I16.push_back(entry);
  }

  runDuckTestBinary<int32_t, int16_t, int32_t>(
      INTEGER(),
      SMALLINT(),
      "duckdb_mod(c0, c1)",
      input1,
      input2I16,
      expectedOutput);

  // large vector + string
  std::vector<StringView> input1String, input2String, expectedOutputString;
  input1String.reserve(largeVectorSize);
  input2String.reserve(largeVectorSize);
  expectedOutputString.reserve(largeVectorSize);
  for (size_t i = 0; i < largeVectorSize; i++) {
    auto str1 = std::to_string(input1[i]);
    auto str2 = std::to_string(input2[i]);
    input1String.push_back(StringView(str1));
    input2String.push_back(StringView(str2));
    expectedOutputString.push_back(StringView(str1 + str2));
  }
  runDuckTestBinary<StringView, StringView, StringView>(
      VARCHAR(),
      VARCHAR(),
      "duckdb_concat(c0, c1)",
      input1String,
      input2String,
      expectedOutputString);

  // large vector + timestamp output
  std::vector<int64_t> input1Ms, input2Ms;
  std::vector<Timestamp> expectedOutputMs;
  input1Ms.reserve(largeVectorSize);
  input2Ms.reserve(largeVectorSize);
  expectedOutputMs.reserve(largeVectorSize);
  for (auto i = 0; i < largeVectorSize; i++) {
    auto i1 = (uint64_t(i) * 69785498754657) % 719067487;
    auto i2 = (uint64_t(i) * 90786983836899) % 719067417;
    input1Ms.push_back(i1);
    input2Ms.push_back(i2);
    expectedOutputMs.push_back(Timestamp(i1 + i2, 0));
  }
  runDuckTestBinary<int64_t, int64_t, Timestamp>(
      BIGINT(),
      BIGINT(),
      "duckdb_epoch_ms((c0 + c1) * 1000)",
      input1Ms,
      input2Ms,
      expectedOutputMs);
}

TEST_F(BaseDuckTest, length) {
  std::vector<StringView> input{
      StringView("hello"),
      StringView("🦆"),
      StringView("🦆🐝"),
      StringView("wüwee"),
      StringView("verylongnoninlinedstring")};
  runDuckTest<StringView, int64_t>(
      VARCHAR(), "duckdb_length(c0)", input, {5, 1, 2, 5, 24});
}

TEST_F(BaseDuckTest, reverse) {
  std::vector<StringView> input{
      StringView("hello"),
      StringView("🦆"),
      StringView("🦆🐝"),
      StringView("wüwee"),
      StringView("verylongnoninlinedstring")};
  std::vector<StringView> output{
      StringView("olleh"),
      StringView("🦆"),
      StringView("🐝🦆"),
      StringView("eewüw"),
      StringView("gnirtsdenilninongnolyrev")};
  runDuckTest<StringView, StringView>(
      VARCHAR(), "duckdb_reverse(c0)", input, output);
}

TEST_F(BaseDuckTest, uCase) {
  std::vector<StringView> input{
      StringView("hello"),
      StringView("🦆"),
      StringView("🦆🐝"),
      StringView("wüwee"),
      StringView("verylongnoninlinedstring")};
  std::vector<StringView> output{
      StringView("HELLO"),
      StringView("🦆"),
      StringView("🦆🐝"),
      StringView("WÜWEE"),
      StringView("VERYLONGNONINLINEDSTRING")};
  runDuckTest<StringView, StringView>(
      VARCHAR(), "duckdb_ucase(c0)", input, output);
}

TEST_F(BaseDuckTest, concat) {
  std::vector<StringView> input1{
      StringView("hello"),
      StringView("🦆"),
      StringView("🦆🐝"),
      StringView("wüwee"),
      StringView("verylongnoninlinedstring")};
  std::vector<StringView> input2{
      StringView("ab"),
      StringView("🐝"),
      StringView("🐝🐝🐝"),
      StringView("verylongnoninlinedstring"),
      StringView("xxx")};
  std::vector<StringView> output{
      StringView("helloab"),
      StringView("🦆🐝"),
      StringView("🦆🐝🐝🐝🐝"),
      StringView("wüweeverylongnoninlinedstring"),
      StringView("verylongnoninlinedstringxxx")};
  runDuckTestBinary<StringView, StringView, StringView>(
      VARCHAR(), VARCHAR(), "duckdb_concat(c0, c1)", input1, input2, output);
}

TEST_F(BaseDuckTest, stringMix) {
  std::vector<StringView> input{
      StringView("hello"),
      StringView("🦆"),
      StringView("🦆🐝"),
      StringView("wüwee"),
      StringView("verylongnoninlinedstring")};
  runDuckTest<StringView, int64_t>(
      VARCHAR(), "duckdb_length(duckdb_ucase(c0))", input, {5, 1, 2, 5, 24});
  runDuckTest<StringView, int64_t>(
      VARCHAR(),
      "duckdb_length(duckdb_concat(duckdb_ucase(c0), 'hello'))",
      input,
      {10, 6, 7, 10, 29});
  runDuckTest<StringView, int64_t>(
      VARCHAR(),
      "duckdb_length(duckdb_concat(duckdb_ucase(c0), c0))",
      input,
      {10, 2, 4, 10, 48});
  // concat can take any number of parameters
  runDuckTest<StringView, int64_t>(
      VARCHAR(),
      "duckdb_length(duckdb_concat(duckdb_ucase(c0), 'hello', ' 🦆'))",
      input,
      {12, 8, 9, 12, 31});
}

TEST_F(BaseDuckTest, year) {
  std::vector<Timestamp> input{
      Timestamp(1602161331, 0),
      Timestamp(719067487, 0),
      Timestamp(719067487, 0)};
  runDuckTest<Timestamp, int64_t>(
      TIMESTAMP(), "duckdb_year(c0)", input, {2020, 1992, 1992});
}

TEST_F(BaseDuckTest, epochMs) {
  runDuckTest<int64_t, Timestamp>(
      BIGINT(),
      "duckdb_epoch_ms(c0)",
      {0, 1574802684123},
      {Timestamp(0, 0), Timestamp(1574802684, 123000000)});
}
