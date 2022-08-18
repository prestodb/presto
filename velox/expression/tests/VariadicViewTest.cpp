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

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "velox/expression/VectorReaders.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using facebook::velox::exec::LocalDecodedVector;

template <bool returnsOptionalValues>
class VariadicViewTest : public functions::test::FunctionBaseTest {
 protected:
  using ViewType = exec::VariadicView<returnsOptionalValues, int64_t>;

  // What value to use for NULL in the test data.  If the view type is
  // not returnsOptionalValues, we use 0 as an arbitrary value.
  std::optional<int64_t> nullValue =
      returnsOptionalValues ? std::nullopt : std::make_optional(0);
  std::vector<std::vector<std::optional<int64_t>>> bigIntVectors = {
      {nullValue, nullValue, nullValue},
      {0, 1, 2},
      {99, 98, nullValue},
      {101, nullValue, 102},
      {nullValue, 10001, 12345676},
      {nullValue, nullValue, 3},
      {nullValue, 4, nullValue},
      {5, nullValue, nullValue},
  };

  ViewType read(exec::VectorReader<Variadic<int64_t>>& reader, size_t offset) {
    if constexpr (returnsOptionalValues) {
      return reader[offset];
    } else {
      return reader.readNullFree(offset);
    }
  }

  virtual void testItem(int row, int arg, typename ViewType::Element item) = 0;

  void testVariadicView(const std::vector<VectorPtr>& additionalVectors = {}) {
    std::vector<VectorPtr> vectors(
        additionalVectors.begin(), additionalVectors.end());
    for (const auto& vector : bigIntVectors) {
      vectors.emplace_back(makeNullableFlatVector(vector));
    }
    SelectivityVector rows(vectors[0]->size());
    EvalCtx ctx(&execCtx_);
    std::vector<std::optional<LocalDecodedVector>> args;
    for (const auto& vector : vectors) {
      args.emplace_back(LocalDecodedVector(&ctx, *vector, rows));
    }

    size_t startIndex = additionalVectors.size();
    VectorReader<Variadic<int64_t>> reader(args, startIndex);

    for (auto row = 0; row < vectors[0]->size(); ++row) {
      auto variadicView = read(reader, row);
      auto arg = 0;

      // Test iterate loop.
      for (auto item : variadicView) {
        testItem(row, arg, item);
        arg++;
      }
      ASSERT_EQ(arg, bigIntVectors.size());

      // Test iterate loop explicit begin & end.
      auto it = variadicView.begin();
      arg = 0;
      while (it != variadicView.end()) {
        testItem(row, arg, *it);
        arg++;
        ++it;
      }
      ASSERT_EQ(arg, bigIntVectors.size());

      // Test index based loop.
      for (arg = 0; arg < variadicView.size(); arg++) {
        testItem(row, arg, variadicView[arg]);
      }
      ASSERT_EQ(arg, bigIntVectors.size());

      // Test loop iterator with <.
      arg = 0;
      for (auto it2 = variadicView.begin(); it2 < variadicView.end(); it2++) {
        testItem(row, arg, *it2);
        arg++;
      }
      ASSERT_EQ(arg, bigIntVectors.size());

      // Test loop iteration in reverse with post decrement
      arg = bigIntVectors.size() - 1;
      for (it = variadicView.end() - 1; it >= variadicView.begin(); it--) {
        testItem(row, arg, *it);
        arg--;
      }
      // This is unintuitive but because we decrement after accessing each
      // element, since j starts as one less than the number of readers, it
      // should finish at -1.
      ASSERT_EQ(arg, -1);

      // Test iterate with pre decrement
      it = variadicView.end() - 1;
      arg = bigIntVectors.size() - 1;
      while (it >= variadicView.begin()) {
        testItem(row, arg, *it);
        arg--;
        --it;
      }
      // This is unintuitive but because we decrement after accessing each
      // element, since j starts as one less than the number of readers, it
      // should finish at -1.
      ASSERT_EQ(arg, -1);
    }
  }

  void iteratorDifferenceTest() {
    std::vector<VectorPtr> vectors;
    for (const auto& vector : bigIntVectors) {
      vectors.emplace_back(makeNullableFlatVector(vector));
    }
    SelectivityVector rows(vectors[0]->size());
    EvalCtx ctx(&execCtx_);
    std::vector<std::optional<LocalDecodedVector>> args;
    for (const auto& vector : vectors) {
      args.emplace_back(LocalDecodedVector(&ctx, *vector, rows));
    }

    VectorReader<Variadic<int64_t>> reader(args, 0);

    for (auto row = 0; row < vectors[0]->size(); row++) {
      auto variadicView = read(reader, row);
      auto it = variadicView.begin();

      for (int j = 0; j < variadicView.size(); j++) {
        auto it2 = variadicView.begin();
        for (int k = 0; k <= j; k++) {
          ASSERT_EQ(it - it2, j - k);
          ASSERT_EQ(it2 - it, k - j);
          it2++;
        }
        it++;
      }
    }
  }

  void iteratorAdditionTest() {
    std::vector<VectorPtr> vectors;
    for (const auto& vector : bigIntVectors) {
      vectors.emplace_back(makeNullableFlatVector(vector));
    }
    SelectivityVector rows(vectors[0]->size());
    EvalCtx ctx(&execCtx_);
    std::vector<std::optional<LocalDecodedVector>> args;
    for (const auto& vector : vectors) {
      args.emplace_back(LocalDecodedVector(&ctx, *vector, rows));
    }

    VectorReader<Variadic<int64_t>> reader(args, 0);

    for (auto row = 0; row < vectors[0]->size(); row++) {
      auto variadicView = read(reader, row);
      auto it = variadicView.begin();

      for (int j = 0; j < variadicView.size(); j++) {
        auto it2 = variadicView.begin();
        for (int k = 0; k < variadicView.size(); k++) {
          ASSERT_EQ(it, it2 + (j - k));
          ASSERT_EQ(it, (j - k) + it2);
          auto it3 = it2;
          it3 += j - k;
          ASSERT_EQ(it, it3);
          it2++;
        }
        it++;
      }
    }
  }

  void iteratorSubtractionTest() {
    std::vector<VectorPtr> vectors;
    for (const auto& vector : bigIntVectors) {
      vectors.emplace_back(makeNullableFlatVector(vector));
    }
    SelectivityVector rows(vectors[0]->size());
    EvalCtx ctx(&execCtx_);
    std::vector<std::optional<LocalDecodedVector>> args;
    for (const auto& vector : vectors) {
      args.emplace_back(LocalDecodedVector(&ctx, *vector, rows));
    }

    VectorReader<Variadic<int64_t>> reader(args, 0);

    for (auto row = 0; row < vectors[0]->size(); row++) {
      auto variadicView = read(reader, row);
      auto it = variadicView.begin();

      for (int j = 0; j < variadicView.size(); j++) {
        auto it2 = variadicView.begin();
        for (int k = 0; k < variadicView.size(); k++) {
          ASSERT_EQ(it, it2 - (k - j));
          auto it3 = it2;
          it3 -= k - j;
          ASSERT_EQ(it, it3);
          it2++;
        }
        it++;
      }
    }
  }

  void iteratorSubscriptTest() {
    std::vector<VectorPtr> vectors;
    for (const auto& vector : bigIntVectors) {
      vectors.emplace_back(makeNullableFlatVector(vector));
    }
    SelectivityVector rows(vectors[0]->size());
    EvalCtx ctx(&execCtx_);
    std::vector<std::optional<LocalDecodedVector>> args;
    for (const auto& vector : vectors) {
      args.emplace_back(LocalDecodedVector(&ctx, *vector, rows));
    }

    VectorReader<Variadic<int64_t>> reader(args, 0);

    for (auto row = 0; row < vectors[0]->size(); row++) {
      auto variadicView = read(reader, row);
      auto it = variadicView.begin();

      for (int j = 0; j < variadicView.size(); j++) {
        auto it2 = variadicView.begin();
        for (int k = 0; k < variadicView.size(); k++) {
          ASSERT_EQ(*it, it2[j - k]);
          it2++;
        }
        it++;
      }
    }
  }
};

class NullableVariadicViewTest : public VariadicViewTest<true> {
 public:
  void testItem(int row, int arg, typename ViewType::Element item) override {
    // Test has_value.
    ASSERT_EQ(bigIntVectors[arg][row].has_value(), item.has_value());

    // Test bool implicit cast.
    ASSERT_EQ(bigIntVectors[arg][row].has_value(), static_cast<bool>(item));

    if (bigIntVectors[arg][row].has_value()) {
      // Test * operator.
      ASSERT_EQ(bigIntVectors[arg][row].value(), *item);

      // Test value().
      ASSERT_EQ(bigIntVectors[arg][row].value(), item.value());
    }
    // Test == with std::optional
    ASSERT_EQ(item, bigIntVectors[arg][row]);
  }
};

class NullFreeVariadicViewTest : public VariadicViewTest<false> {
  void testItem(int row, int arg, typename ViewType::Element item) override {
    ASSERT_EQ(item, bigIntVectors[arg][row]);
  }
};

TEST_F(NullableVariadicViewTest, variadicInt) {
  testVariadicView();
}

TEST_F(NullableVariadicViewTest, variadicIntMoreArgs) {
  // Test accessing Variadic args when there are other args before it.
  testVariadicView(
      {makeNullableFlatVector(std::vector<std::optional<int64_t>>{-1, -2, -3}),
       makeNullableFlatVector(
           std::vector<std::optional<int64_t>>{-4, std::nullopt, -6}),
       makeNullableFlatVector(std::vector<std::optional<int64_t>>{
           std::nullopt, std::nullopt, std::nullopt})});
}

TEST_F(NullableVariadicViewTest, notNullContainer) {
  std::vector<VectorPtr> vectors;
  for (const auto& vector : bigIntVectors) {
    vectors.emplace_back(makeNullableFlatVector(vector));
  }
  SelectivityVector rows(vectors[0]->size());
  EvalCtx ctx(&execCtx_);
  std::vector<std::optional<LocalDecodedVector>> args;
  for (const auto& vector : vectors) {
    args.emplace_back(LocalDecodedVector(&ctx, *vector, rows));
  }

  VectorReader<Variadic<int64_t>> reader(args, 0);

  for (auto row = 0; row < vectors[0]->size(); ++row) {
    auto variadicView = reader[row];
    int arg = 0;
    for (auto value : variadicView.skipNulls()) {
      while (arg < bigIntVectors.size() &&
             bigIntVectors[arg][row] == std::nullopt) {
        arg++;
      }
      ASSERT_EQ(value, bigIntVectors[arg][row].value());
      arg++;
    }
  }
}

TEST_F(NullableVariadicViewTest, iteratorDifference) {
  iteratorDifferenceTest();
}

TEST_F(NullableVariadicViewTest, iteratorAddition) {
  iteratorAdditionTest();
}

TEST_F(NullableVariadicViewTest, iteratorSubtraction) {
  iteratorSubtractionTest();
}

TEST_F(NullableVariadicViewTest, iteratorSubscript) {
  iteratorSubscriptTest();
}

TEST_F(NullFreeVariadicViewTest, variadicInt) {
  testVariadicView();
}

TEST_F(NullFreeVariadicViewTest, variadicIntMoreArgs) {
  // Test accessing Variadic args when there are other args before it.
  testVariadicView(
      {makeNullableFlatVector(std::vector<std::optional<int64_t>>{-1, -2, -3}),
       makeNullableFlatVector(
           std::vector<std::optional<int64_t>>{-4, std::nullopt, -6}),
       makeNullableFlatVector(std::vector<std::optional<int64_t>>{
           std::nullopt, std::nullopt, std::nullopt})});
}

TEST_F(NullFreeVariadicViewTest, iteratorDifference) {
  iteratorDifferenceTest();
}

TEST_F(NullFreeVariadicViewTest, iteratorAddition) {
  iteratorAdditionTest();
}

TEST_F(NullFreeVariadicViewTest, iteratorSubtraction) {
  iteratorSubtractionTest();
}

TEST_F(NullFreeVariadicViewTest, iteratorSubscript) {
  iteratorSubscriptTest();
}

const auto null = "null"_sv;
const auto callPrefix = "call "_sv;
const auto callNullablePrefix = "callNullable "_sv;
const auto callAsciiPrefix = "callAscii "_sv;

void writeInputToOutput(
    StringWriter<>& out,
    const VariadicView<true, Varchar>* inputs) {
  for (const auto& input : *inputs) {
    out += input.has_value() ? input.value() : null;
  }
}

// Function that uses a Variadic Type (it's essentially concat).
template <typename T>
struct VariadicArgsReaderFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& out,
      const arg_type<Variadic<Varchar>>& inputs) {
    writeInputToOutput(out, &inputs);

    return true;
  }
};

TEST_F(NullableVariadicViewTest, basic) {
  registerFunction<VariadicArgsReaderFunction, Varchar, Variadic<Varchar>>(
      {"variadic_args_reader_func"});

  auto arg1 = makeFlatVector<StringView>({"a"_sv, "b"_sv, "c"_sv});
  auto arg2 = makeFlatVector<StringView>({"d"_sv, "e"_sv, "f"_sv});
  auto arg3 = makeFlatVector<StringView>({"x"_sv, "y"_sv, "z"_sv});
  auto result = evaluate<FlatVector<StringView>>(
      "variadic_args_reader_func(c0, c1, c2)",
      makeRowVector({arg1, arg2, arg3}));

  ASSERT_EQ(result->valueAt(0).getString(), "adx");
  ASSERT_EQ(result->valueAt(1).getString(), "bey");
  ASSERT_EQ(result->valueAt(2).getString(), "cfz");
}

TEST_F(NullableVariadicViewTest, noArgs) {
  registerFunction<VariadicArgsReaderFunction, Varchar, Variadic<Varchar>>(
      {"variadic_args_reader_func"});

  auto result = evaluate<SimpleVector<StringView>>(
      "variadic_args_reader_func()", makeRowVector(ROW({}), 3));

  ASSERT_EQ(result->valueAt(0).getString(), "");
  ASSERT_EQ(result->valueAt(1).getString(), "");
  ASSERT_EQ(result->valueAt(2).getString(), "");
}

TEST_F(NullableVariadicViewTest, withNull) {
  registerFunction<VariadicArgsReaderFunction, Varchar, Variadic<Varchar>>(
      {"variadic_args_reader_func"});

  // There are nulls in at least one of the args under the VariadicArgs in the
  // first two rows, so those should return null due to default null behavior.
  // The third row doesn't have nulls so it should be computed as usual.
  auto arg1 =
      makeNullableFlatVector<StringView>({std::nullopt, "b"_sv, "c"_sv});
  auto arg2 =
      makeNullableFlatVector<StringView>({"d"_sv, std::nullopt, "f"_sv});
  auto arg3 = makeFlatVector<StringView>({"x"_sv, "y"_sv, "z"_sv});
  auto result = evaluate<FlatVector<StringView>>(
      "variadic_args_reader_func(c0, c1, c2)",
      makeRowVector({arg1, arg2, arg3}));

  ASSERT_TRUE(result->isNullAt(0));
  ASSERT_TRUE(result->isNullAt(1));
  ASSERT_FALSE(result->isNullAt(2));
  ASSERT_EQ(result->valueAt(2).getString(), "cfz");
}

// Function that uses a Variadic Type and doesn't have default null behavior.
// Again, it's essentially concat, but uses the string "NULL" for nulls.
// It also prefixes the result with call or callNullable, depending on which
// version of call is executed.
template <typename T>
struct VariadicArgsReaderWithNullsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& out,
      const arg_type<Varchar>& first,
      const arg_type<Variadic<Varchar>>& inputs) {
    out += callPrefix;
    out += first;
    writeInputToOutput(out, &inputs);

    return true;
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<Varchar>& out,
      const arg_type<Varchar>* first,
      const arg_type<Variadic<Varchar>>* inputs) {
    out += callNullablePrefix;
    out += first ? *first : null;
    writeInputToOutput(out, inputs);

    return true;
  }
};

TEST_F(NullableVariadicViewTest, callNullable) {
  registerFunction<
      VariadicArgsReaderWithNullsFunction,
      Varchar,
      Varchar,
      Variadic<Varchar>>({"variadic_args_reader_with_nulls_func"});

  // The first argument, which is not part of the VariadicArgs, has a null
  // so callNullable should be called.
  auto arg1 =
      makeNullableFlatVector<StringView>({std::nullopt, "b"_sv, "c"_sv});
  auto arg2 =
      makeNullableFlatVector<StringView>({"d"_sv, std::nullopt, "f"_sv});
  auto arg3 =
      makeNullableFlatVector<StringView>({"x"_sv, "y"_sv, std::nullopt});
  auto result = evaluate<FlatVector<StringView>>(
      "variadic_args_reader_with_nulls_func(c0, c1, c2)",
      makeRowVector({arg1, arg2, arg3}));

  ASSERT_EQ(result->valueAt(0).getString(), "callNullable nulldx");
  ASSERT_EQ(result->valueAt(1).getString(), "callNullable bnully");
  ASSERT_EQ(result->valueAt(2).getString(), "callNullable cfnull");
}

TEST_F(
    NullableVariadicViewTest,
    variadicArgsReaderCallNullableNullVariadicArgs) {
  registerFunction<
      VariadicArgsReaderWithNullsFunction,
      Varchar,
      Varchar,
      Variadic<Varchar>>({"variadic_args_reader_with_nulls_func"});

  // The first argument, which is not part of the VariadicArgs, does not have
  // nulls.  callNullable should be called anyway since there are nulls in the
  // VariadicArgs.
  auto arg1 = makeNullableFlatVector<StringView>({"a"_sv, "b"_sv, "c"_sv});
  auto arg2 =
      makeNullableFlatVector<StringView>({"d"_sv, std::nullopt, "f"_sv});
  auto arg3 =
      makeNullableFlatVector<StringView>({"x"_sv, "y"_sv, std::nullopt});
  auto result = evaluate<FlatVector<StringView>>(
      "variadic_args_reader_with_nulls_func(c0, c1, c2)",
      makeRowVector({arg1, arg2, arg3}));

  ASSERT_EQ(result->valueAt(0).getString(), "callNullable adx");
  ASSERT_EQ(result->valueAt(1).getString(), "callNullable bnully");
  ASSERT_EQ(result->valueAt(2).getString(), "callNullable cfnull");
}

TEST_F(NullableVariadicViewTest, callNullableNoNulls) {
  registerFunction<
      VariadicArgsReaderWithNullsFunction,
      Varchar,
      Varchar,
      Variadic<Varchar>>({"variadic_args_reader_with_nulls_func"});

  // There are no nulls in the arguments, so call should be called.
  auto arg1 = makeNullableFlatVector<StringView>({"a"_sv, "b"_sv, "c"_sv});
  auto arg2 = makeNullableFlatVector<StringView>({"d"_sv, "e"_sv, "f"_sv});
  auto arg3 = makeNullableFlatVector<StringView>({"x"_sv, "y"_sv, "z"_sv});
  auto result = evaluate<FlatVector<StringView>>(
      "variadic_args_reader_with_nulls_func(c0, c1, c2)",
      makeRowVector({arg1, arg2, arg3}));

  ASSERT_EQ(result->valueAt(0).getString(), "call adx");
  ASSERT_EQ(result->valueAt(1).getString(), "call bey");
  ASSERT_EQ(result->valueAt(2).getString(), "call cfz");
}

// Function that uses a Variadic Type and doesn't supports callAscii (though
// the behavior isn't really any different).
// It prefixes the result with call or callAscii, depending on which
// version of call is executed.
template <typename T>
struct VariadicArgsReaderWithAsciiFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& out,
      const arg_type<Variadic<Varchar>>& inputs) {
    out += callPrefix;
    writeInputToOutput(out, &inputs);

    return true;
  }

  FOLLY_ALWAYS_INLINE bool callAscii(
      out_type<Varchar>& out,
      const arg_type<Variadic<Varchar>>& inputs) {
    out += callAsciiPrefix;
    writeInputToOutput(out, &inputs);

    return true;
  }
};

TEST_F(NullableVariadicViewTest, callNonAscii) {
  registerFunction<
      VariadicArgsReaderWithAsciiFunction,
      Varchar,
      Variadic<Varchar>>({"variadic_args_reader_with_ascii_func"});

  // Some of the input arguments have non-ACII characters so call should be
  // called.
  auto arg1 = makeFlatVector<StringView>({"à"_sv, "b"_sv, "c"_sv});
  auto arg2 = makeFlatVector<StringView>({"d"_sv, "ê"_sv, "f"_sv});
  auto arg3 = makeFlatVector<StringView>({"x"_sv, "y"_sv, "ζ"_sv});
  auto result = evaluate<FlatVector<StringView>>(
      "variadic_args_reader_with_ascii_func(c0, c1, c2)",
      makeRowVector({arg1, arg2, arg3}));

  ASSERT_EQ(result->valueAt(0).getString(), "call àdx");
  ASSERT_EQ(result->valueAt(1).getString(), "call bêy");
  ASSERT_EQ(result->valueAt(2).getString(), "call cfζ");
}

TEST_F(NullableVariadicViewTest, callAscii) {
  registerFunction<
      VariadicArgsReaderWithAsciiFunction,
      Varchar,
      Variadic<Varchar>>({"variadic_args_reader_with_ascii_func"});

  // All of the input arguments are ASCII so callAscii should be
  // called.
  auto arg1 = makeFlatVector<StringView>({"a"_sv, "b"_sv, "c"_sv});
  auto arg2 = makeFlatVector<StringView>({"d"_sv, "e"_sv, "f"_sv});
  auto arg3 = makeFlatVector<StringView>({"x"_sv, "y"_sv, "z"_sv});
  auto result = evaluate<FlatVector<StringView>>(
      "variadic_args_reader_with_ascii_func(c0, c1, c2)",
      makeRowVector({arg1, arg2, arg3}));

  ASSERT_EQ(result->valueAt(0).getString(), "callAscii adx");
  ASSERT_EQ(result->valueAt(1).getString(), "callAscii bey");
  ASSERT_EQ(result->valueAt(2).getString(), "callAscii cfz");
}
