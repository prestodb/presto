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

#include <glog/logging.h>
#include <optional>
#include "velox/common/base/CompareFlags.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/VectorReaders.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {
DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return &decoder;
}

class GenericViewTest : public functions::test::FunctionBaseTest {
 protected:
  using array_data_t =
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>;

  array_data_t arrayData1 = {
      {{}},
      {{{{std::nullopt}}}},
      {{std::nullopt, 1}},
      {{std::nullopt, std::nullopt, std::nullopt}},
      {{0, 1, 2, 4}},
      {{99, 98}},
      {{101, std::nullopt}},
      {{10001, 12345676, std::nullopt}}, // after this index different
      {{std::nullopt, 1}},
      {{std::nullopt, 2}},
      {{std::nullopt, 3, std::nullopt}},
      {{0, 1, 2, 4}},
      {{99, 100}},
      {{101, std::nullopt, 22}},
      {{10001, 12345676, std::nullopt, 101}},
  };

  array_data_t arrayData2 = {
      {{}},
      {{{{std::nullopt}}}},
      {{std::nullopt, 1}},
      {{std::nullopt, std::nullopt, std::nullopt}},
      {{0, 1, 2, 4}},
      {{99, 98}},
      {{101, std::nullopt}},
      {{10001, 12345676, std::nullopt, 1}}, // after this index different
      {{2, 1}},
      {{std::nullopt, 3}},
      {{std::nullopt, 3, std::nullopt, 1}},
      {{0, 1, 2, 4, 5}},
      {{99, 100, 12}},
      {{1011, std::nullopt, 22}},
      {{10001, 1, std::nullopt, 101}},
  };

  template <typename DataT>
  void testEqual(
      const VectorPtr& vector1,
      const VectorPtr& vector2,
      const DataT& data1,
      const DataT& data2) {
    DecodedVector decoded1;
    DecodedVector decoded2;
    exec::VectorReader<Any> reader1(decode(decoded1, *vector1));
    exec::VectorReader<Any> reader2(decode(decoded2, *vector2));

    for (auto i = 0; i < vector1->size(); i++) {
      ASSERT_EQ(data1[i].has_value(), reader1.isSet(i));
      ASSERT_EQ(data2[i].has_value(), reader2.isSet(i));
      if (data1[i].has_value() && data2[i].has_value()) {
        ASSERT_EQ(
            data1[i].value() == data2[i].value(), reader1[i] == reader2[i]);
      }
    }
  }

  void testHash(const VectorPtr& vector) {
    DecodedVector decoded;
    exec::VectorReader<Any> reader(decode(decoded, *vector));
    for (auto i = 0; i < vector->size(); i++) {
      if (reader.isSet(i)) {
        ASSERT_EQ(
            std::hash<exec::GenericView>{}(reader[i]), vector->hashValueAt(i));
        ASSERT_EQ(reader[i].hash(), vector->hashValueAt(i));
      }
    }
  }

  template <typename B>
  void testHasGeneric(bool expected) {
    ASSERT_EQ(exec::HasGeneric<B>::value(), expected)
        << CppToType<B>::create()->toString();
  }

  template <typename B>
  void testAllGenericExceptTop(bool expected) {
    ASSERT_EQ(exec::AllGenericExceptTop<B>::value(), expected);
  }
};

TEST_F(GenericViewTest, primitive) {
  std::vector<std::optional<int64_t>> data1 = {
      1, 2, std::nullopt, 1, std::nullopt, 5, 6};
  std::vector<std::optional<int64_t>> data2 = {
      2, 2, 1, std::nullopt, std::nullopt, 5, 7};

  auto vector1 = vectorMaker_.flatVectorNullable<int64_t>(data1);
  auto vector2 = vectorMaker_.flatVectorNullable<int64_t>(data2);
  testEqual(vector1, vector2, data1, data2);
  testHash(vector1);
}

TEST_F(GenericViewTest, compare) {
  std::vector<std::optional<int64_t>> data = {1, 2, std::nullopt, 1};

  auto vector = vectorMaker_.flatVectorNullable<int64_t>(data);
  DecodedVector decoded;
  exec::VectorReader<Any> reader(decode(decoded, *vector));
  CompareFlags flags;
  ASSERT_EQ(reader[0].compare(reader[0], flags).value(), 0);
  ASSERT_EQ(reader[0].compare(reader[3], flags).value(), 0);

  ASSERT_NE(reader[0].compare(reader[1], flags).value(), 0);
  ASSERT_NE(reader[0].compare(reader[2], flags).value(), 0);

  flags.stopAtNull = true;
  ASSERT_FALSE(reader[0].compare(reader[2], flags).has_value());
  ASSERT_TRUE(reader[0].compare(reader[1], flags).has_value());
}

// Test reader<Generic> where generic elements are arrays<ints>
TEST_F(GenericViewTest, arrayOfInt) {
  auto vector1 = vectorMaker_.arrayVectorNullable(arrayData1);
  auto vector2 = vectorMaker_.arrayVectorNullable(arrayData2);
  testEqual(vector1, vector2, arrayData1, arrayData2);
  testHash(vector1);
}

// Test reader<Array<Generic>> where generic elements are ints.
TEST_F(GenericViewTest, arrayOfGeneric) {
  auto vector1 = vectorMaker_.arrayVectorNullable(arrayData1);
  auto vector2 = vectorMaker_.arrayVectorNullable(arrayData2);

  DecodedVector decoded1;
  DecodedVector decoded2;
  exec::VectorReader<Array<Any>> reader1(decode(decoded1, *vector1));
  exec::VectorReader<Array<Any>> reader2(decode(decoded2, *vector2));

  // Reader will return std::vector<std::optional<Generic>> like object.
  for (auto i = 0; i < vector1->size(); i++) {
    auto arrayView1 = reader1[i];
    auto arrayView2 = reader2[i];

    // Test comparing generics nested in ArrayView.
    for (auto j = 0; j < arrayView1.size(); j++) {
      auto generic1 = arrayView1[j];

      for (auto k = 0; k < arrayView2.size(); k++) {
        auto generic2 = arrayView2[k];

        ASSERT_EQ(
            generic1 == generic2,
            arrayData1[i].value()[j] == arrayData2[i].value()[k]);
      }
    }
  }
}

template <typename T>
struct CompareFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename G>
  void call(bool& out, const G& a, const G& b) {
    out = (a == b);
  }
};

TEST_F(GenericViewTest, e2eCompareInts) {
  registerFunction<CompareFunc, bool, Any, Any>({"func1"});
  registerFunction<CompareFunc, bool, Generic<T1>, Generic<T2>>({"func2"});
  registerFunction<CompareFunc, bool, Generic<T1>, Generic<T1>>({"func3"});

  auto vector1 = vectorMaker_.arrayVectorNullable(arrayData1);
  auto vector2 = vectorMaker_.arrayVectorNullable(arrayData2);

  auto result1 = evaluate<FlatVector<bool>>(
      "func1(c0, c1)", makeRowVector({vector1, vector2}));
  auto result2 = evaluate<FlatVector<bool>>(
      "func2(c0, c1)", makeRowVector({vector1, vector2}));
  auto result3 = evaluate<FlatVector<bool>>(
      "func3(c0, c1)", makeRowVector({vector1, vector2}));

  for (auto i = 0; i < arrayData1.size(); i++) {
    ASSERT_EQ(result1->valueAt(i), i <= 6) << "error at index:" << i;
    ASSERT_EQ(result2->valueAt(i), i <= 6) << "error at index:" << i;
    ASSERT_EQ(result3->valueAt(i), i <= 6) << "error at index:" << i;
  }
}

// Add hash(arg1) + hash(arg2).
template <typename T>
struct HashFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename G>
  void call(int64_t& out, const G& a, const G& b) {
    out = a.hash() + b.hash();
  }
};

TEST_F(GenericViewTest, e2eHashAddSameType) {
  registerFunction<HashFunc, int64_t, Generic<T1>, Generic<T1>>(
      {"add_hash_same_type"});

  auto vector1 = vectorMaker_.flatVector<int64_t>({1, 2, 3});
  auto vector2 = vectorMaker_.flatVector<int64_t>({4, 5, 6});
  auto vectorDouble = vectorMaker_.flatVector<double>({4, 5, 6});

  auto result1 = evaluate<FlatVector<int64_t>>(
      "add_hash_same_type(c0, c1)", makeRowVector({vector1, vector2}));

  for (auto i = 0; i < 3; i++) {
    ASSERT_EQ(
        result1->valueAt(i), vector1->hashValueAt(i) + vector2->hashValueAt(i));
  }

  // All arguments expected to be of the same type.
  VELOX_ASSERT_THROW(
      evaluate<FlatVector<int64_t>>(
          "add_hash_same_type(c0, c1)", makeRowVector({vector1, vectorDouble})),
      "Scalar function signature is not supported: add_hash_same_type(BIGINT, DOUBLE). Supported signatures: (__user_T1,__user_T1) -> bigint.");
}

TEST_F(GenericViewTest, e2eHashDifferentTypes) {
  registerFunction<HashFunc, int64_t, Generic<T1>, Generic<T2>>(
      {"add_hash_diff_type"});
  registerFunction<HashFunc, int64_t, Any, Any>({"add_hash_diff_type2"});

  auto vectorInt = vectorMaker_.flatVector<int64_t>({1, 2, 3});
  auto vectorDouble = vectorMaker_.flatVector<double>({4, 5, 6});

  auto result1 = evaluate<FlatVector<int64_t>>(
      "add_hash_diff_type(c0, c1)", makeRowVector({vectorInt, vectorDouble}));
  auto result2 = evaluate<FlatVector<int64_t>>(
      "add_hash_diff_type2(c0, c1)", makeRowVector({vectorInt, vectorDouble}));

  for (auto i = 0; i < 3; i++) {
    ASSERT_EQ(
        result1->valueAt(i),
        vectorInt->hashValueAt(i) + vectorDouble->hashValueAt(i));
    ASSERT_EQ(
        result2->valueAt(i),
        vectorInt->hashValueAt(i) + vectorDouble->hashValueAt(i));
  }
}

// Add hash(arg1) + hash(arg2) + hash(arg3)... etc.
template <typename T>
struct HashAllArgs {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename G>
  void call(int64_t& out, const G& args) {
    out = 0;
    for (auto arg : args) {
      out += arg.value().hash();
    }
  }
};

TEST_F(GenericViewTest, e2eHashVariadicSameType) {
  registerFunction<HashAllArgs, int64_t, Variadic<Generic<T1>>>(
      {"hash_all_args"});

  auto vectorInt1 = vectorMaker_.flatVector<int64_t>({1, 2, 3});
  auto vectorInt2 = vectorMaker_.flatVector<int64_t>({1, 2, 3});
  auto vectorDouble = vectorMaker_.flatVector<double>({4, 5, 6});

  auto result1 = evaluate<FlatVector<int64_t>>(
      "hash_all_args(c0, c1)", makeRowVector({vectorInt1, vectorInt2}));

  for (auto i = 0; i < 3; i++) {
    ASSERT_EQ(
        result1->valueAt(i),
        vectorInt1->hashValueAt(i) + vectorInt2->hashValueAt(i));
  }

  // All arguments expected to be of the same type.
  VELOX_ASSERT_THROW(
      evaluate<FlatVector<int64_t>>(
          "hash_all_args(c0, c1, c2)",
          makeRowVector({vectorInt1, vectorInt2, vectorDouble})),
      "Scalar function signature is not supported: hash_all_args(BIGINT, BIGINT, DOUBLE). Supported signatures: (__user_T1...) -> bigint.");
}

TEST_F(GenericViewTest, e2eHashVariadicAnyType) {
  registerFunction<HashAllArgs, int64_t, Variadic<Any>>({"func1"});
  registerFunction<HashAllArgs, int64_t, Variadic<Generic<AnyType>>>({"func2"});

  auto vectorInt1 = vectorMaker_.flatVector<int64_t>({1, 2, 3});
  auto vectorInt2 = vectorMaker_.flatVector<int64_t>({1, 2, 3});
  auto vectorDouble = vectorMaker_.flatVector<double>({4, 5, 6});

  auto result1 = evaluate<FlatVector<int64_t>>(
      "func1(c0, c1, c2)",
      makeRowVector({vectorInt1, vectorInt2, vectorDouble}));
  auto result2 = evaluate<FlatVector<int64_t>>(
      "func2(c0, c1, c2)",
      makeRowVector({vectorInt1, vectorInt2, vectorDouble}));

  for (auto i = 0; i < 3; i++) {
    ASSERT_EQ(
        result1->valueAt(i),
        vectorInt1->hashValueAt(i) + vectorInt2->hashValueAt(i) +
            vectorDouble->hashValueAt(i));
    ASSERT_EQ(
        result2->valueAt(i),
        vectorInt1->hashValueAt(i) + vectorInt2->hashValueAt(i) +
            vectorDouble->hashValueAt(i));
  }
}

TEST_F(GenericViewTest, castToInt) {
  std::vector<std::optional<int64_t>> data = {1, 2, std::nullopt, 1};

  auto vector = vectorMaker_.flatVectorNullable<int64_t>(data);
  DecodedVector decoded;
  exec::VectorReader<Any> reader(decode(decoded, *vector));
  ASSERT_EQ(reader[0].castTo<int64_t>(), 1);
  ASSERT_EQ(reader[0].tryCastTo<int64_t>().value(), 1);

  ASSERT_EQ(reader[1].castTo<int64_t>(), 2);
  ASSERT_EQ(reader[1].tryCastTo<int64_t>().value(), 2);
}

TEST_F(GenericViewTest, castToArrayViewOfGeneric) {
  VectorPtr vector = vectorMaker_.arrayVectorNullable(arrayData1);

  DecodedVector decoded;
  exec::VectorReader<Any> reader(decode(decoded, *vector));

  auto generic = reader[4]; //    {{0, 1, 2, 4}}
  ASSERT_EQ(generic.kind(), TypeKind::ARRAY);

  // Test cast to.
  {
    auto arrayView = generic.castTo<Array<Any>>();
    auto i = 0;
    for (auto genericItem : arrayView) {
      if (genericItem.has_value()) {
        ASSERT_EQ(genericItem.value().kind(), TypeKind::BIGINT);
        auto val = genericItem.value().castTo<int64_t>();
        ASSERT_EQ(val, arrayData1[4].value()[i].value());
        i++;
      }
    }
  }

  // Test try cast to.
  {
    auto arrayView = generic.tryCastTo<Array<Any>>().value();

    auto i = 0;
    for (auto genericItem : arrayView) {
      if (genericItem.has_value()) {
        ASSERT_EQ(genericItem.value().kind(), TypeKind::BIGINT);

        auto val = genericItem.value().tryCastTo<int64_t>().value();
        ASSERT_EQ(val, arrayData1[4].value()[i].value());
        i++;
      }
    }
  }
}

TEST_F(GenericViewTest, tryCastTo) {
  DecodedVector decoded;

  { // Reader for vector of bigint.
    auto vector = vectorMaker_.flatVectorNullable<int64_t>({1});
    exec::VectorReader<Any> reader(decode(decoded, *vector));

    ASSERT_FALSE(reader[0].tryCastTo<int8_t>().has_value());
    ASSERT_FALSE(reader[0].tryCastTo<float>().has_value());
    ASSERT_FALSE(reader[0].tryCastTo<double>().has_value());
    ASSERT_FALSE(reader[0].tryCastTo<Array<Any>>().has_value());
    ASSERT_FALSE((reader[0].tryCastTo<Map<Any, Any>>().has_value()));
    ASSERT_FALSE(reader[0].tryCastTo<Row<Any>>().has_value());

    ASSERT_EQ(reader[0].tryCastTo<int64_t>().value(), 1);
  }

  { // Reader for vector of array(bigint).
    auto arrayVector = vectorMaker_.arrayVectorNullable(arrayData1);
    exec::VectorReader<Any> reader(decode(decoded, *arrayVector));

    ASSERT_FALSE(reader[0].tryCastTo<int8_t>().has_value());
    ASSERT_FALSE(reader[0].tryCastTo<float>().has_value());
    ASSERT_FALSE(reader[0].tryCastTo<double>().has_value());
    ASSERT_FALSE((reader[0].tryCastTo<Map<Any, Any>>().has_value()));

    ASSERT_TRUE(reader[0].tryCastTo<Array<Any>>().has_value());
  }
}

TEST_F(GenericViewTest, validMultiCast) {
  DecodedVector decoded;

  { // Reader for vector of array(bigint).
    auto arrayVector = vectorMaker_.arrayVectorNullable(arrayData1);
    exec::VectorReader<Any> reader(decode(decoded, *arrayVector));

    ASSERT_EQ(
        reader[0].tryCastTo<Array<Any>>().value().size(),
        arrayData1[0].value().size());
    ASSERT_EQ(
        reader[0].tryCastTo<Array<int64_t>>().value().size(),
        arrayData1[0].value().size());
  }
}

TEST_F(GenericViewTest, invalidMultiCast) {
  DecodedVector decoded;

  { // Reader for vector of map(bigint, bigint).
    auto arrayVector = makeMapVector<int64_t, int64_t>({{{1, 2}, {3, 4}}});
    exec::VectorReader<Any> reader(decode(decoded, *arrayVector));

    // valid.
    ASSERT_EQ((reader[0].castTo<Map<Any, Any>>().size()), 2);
    // valid.
    ASSERT_EQ((reader[0].castTo<Map<int64_t, int64_t>>().size()), 2);
    // valid.
    ASSERT_EQ((reader[0].castTo<Map<Any, int64_t>>().size()), 2);

    // Will throw since Map<Any, int64_t> and  Map<int64_t, Any> not
    // allowed togother.
    EXPECT_THROW(
        (reader[0].castTo<Map<int64_t, Any>>().size()), VeloxUserError);
  }
}

TEST_F(GenericViewTest, castToMap) {
  using map_type = std::vector<std::pair<int64_t, std::optional<int64_t>>>;

  map_type map1 = {};
  map_type map2 = {{1, 4}, {3, 3}, {4, std::nullopt}};

  std::vector<map_type> mapsData = {map1, map2};

  auto mapVector = makeMapVector<int64_t, int64_t>(mapsData);

  DecodedVector decoded;
  exec::VectorReader<Any> reader(decode(decoded, *mapVector));

  {
    auto generic = reader[0];
    auto map = generic.tryCastTo<Map<Any, Any>>();
    ASSERT_TRUE(map.has_value());
    auto mapView = map.value();
    ASSERT_EQ(mapView.size(), 0);
  }

  {
    auto generic = reader[1];
    auto mapView = generic.castTo<Map<Any, Any>>();
    ASSERT_EQ(mapView.size(), 3);
    ASSERT_EQ(mapView.begin()->first.castTo<int64_t>(), 1);
    ASSERT_EQ(mapView.begin()->second.value().castTo<int64_t>(), 4);
  }
}

// A function that convert a variaidic number of inputs that can have
// any type to string written using castTo.
template <typename T>
struct ToStringFuncCastTo {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Used to print nullable items of maps, rows and arrays.
  template <typename TItem>
  void printItem(out_type<Varchar>& out, const TItem& item) {
    if (item.has_value()) {
      print(out, *item);
    } else {
      out += "null";
    }
  }

  template <typename TMapView>
  void printMap(out_type<Varchar>& out, const TMapView& mapView) {
    out += "map(";
    for (auto [key, value] : mapView) {
      out += "<";
      print(out, key);
      out += ",";
      printItem(out, value);
      out += ">,";
    }
    out += ")";
  }

  template <typename TArrayView>
  void printArray(out_type<Varchar>& out, const TArrayView& arrayView) {
    out += "array(";
    for (const auto& item : arrayView) {
      printItem(out, item);
      out += ", ";
    }
    out += ")";
  }

  template <typename TRowView>
  void printRow(out_type<Varchar>& out, const TRowView& rowView) {
    out += "row(";
    printItem(out, exec::get<0>(rowView));
    out += ", ";
    printItem(out, exec::get<1>(rowView));
    out += ")";
  }

  void print(out_type<Varchar>& out, const arg_type<Any>& arg) {
    // Note: VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH can be used to simplify
    // iterating over all primitive types.
    switch (arg.kind()) {
      case TypeKind::BIGINT:
        out += std::to_string(arg.template castTo<int64_t>());
        break;
      case TypeKind::DOUBLE:
        out += std::to_string(arg.template castTo<double>());
        break;
      case TypeKind::BOOLEAN:
        out += std::to_string(arg.template castTo<bool>());
        break;
      case TypeKind::VARCHAR:
        out += arg.template castTo<Varchar>();
        break;
      case TypeKind::ARRAY: {
        if (*arg.type() == *ARRAY(BIGINT())) {
          // Special handling for array<int64_t> usually this is faster than
          // going through Array<Any>> and casting every element.
          auto arrayView = arg.template castTo<Array<int64_t>>();
          out += "array<int>(";
          for (auto item : arrayView) {
            if (item.has_value()) {
              out += std::to_string(item.value());
            } else {
              out += "null";
            }
            out += ", ";
          }
          out += ")";
        } else {
          auto arrayView = arg.template castTo<Array<Any>>();
          printArray(out, arrayView);
        }
        break;
      }
      case TypeKind::MAP: {
        auto mapView = arg.template castTo<Map<Any, Any>>();
        printMap(out, mapView);
        break;
      }
      case TypeKind::ROW: {
        auto rowSize = arg.type()->asRow().size();
        VELOX_CHECK(rowSize == 2, "print only supports rows of width 2");
        auto rowView = arg.template castTo<Row<Any, Any>>();
        printRow(out, rowView);
        break;
      }
      default:
        VELOX_UNREACHABLE("not supported");
    }
  }

  void call(out_type<Varchar>& out, const arg_type<Variadic<Any>>& args) {
    auto i = 0;
    for (const auto& arg : args) {
      out += "arg " + std::to_string(i++) + " : ";
      printItem(out, arg);
      out += "\n";
    }
  }
};

TEST_F(GenericViewTest, castE2E) {
  registerFunction<ToStringFuncCastTo, Varchar, Variadic<Any>>(
      {"to_string_cast"});

  auto test = [&](const std::string& args, const std::string& expected) {
    auto result = evaluate<SimpleVector<StringView>>(
        fmt::format("to_string_cast({})", args),
        makeRowVector({makeFlatVector<int64_t>(1)}));
    ASSERT_EQ(result->valueAt(0).str(), expected);
  };
  test("row_constructor(1,2)", "arg 0 : row(1, 2)\n");

  test(
      "row_constructor(array_constructor(1,2,3),true)",
      "arg 0 : row(array<int>(1, 2, 3, ), 1)\n");

  test(
      "'hi', array_constructor(array_constructor(1.2, 1.4)), 1",
      "arg 0 : hi\narg 1 : array(array(1.200000, 1.400000, ), )\narg 2 : 1\n");

  test(
      "1.3, map(array_constructor(1), array_constructor(null))",
      "arg 0 : 1.300000\narg 1 : map(<1,null>,)\n");
}

// A function that convert a variaidic number of inputs that can have
// any type to string written using tryCastTo.
template <typename T>
struct ToStringFuncTryCastTo {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Uses to print nullable items of maps, rows and arrays.
  template <typename TItem>
  void printItem(out_type<Varchar>& out, const TItem& item) {
    if (item.has_value()) {
      print(out, *item);
    } else {
      out += "null";
    }
  }

  template <typename TMapView>
  void printMap(out_type<Varchar>& out, const TMapView& mapView) {
    out += "map(";
    for (auto [key, value] : mapView) {
      out += "<";
      print(out, key);
      out += ",";
      printItem(out, value);
      out += ">,";
    }
    out += ")";
  }

  template <typename TArrayView>
  void printArray(out_type<Varchar>& out, const TArrayView& arrayView) {
    out += "array(";
    for (const auto& item : arrayView) {
      printItem(out, item);
      out += ", ";
    }
    out += ")";
  }

  template <typename TRowView>
  void printRow(out_type<Varchar>& out, const TRowView& rowView) {
    out += "row(";
    printItem(out, exec::get<0>(rowView));
    out += ", ";
    printItem(out, exec::get<1>(rowView));
    out += ")";
  }

  void print(out_type<Varchar>& out, const arg_type<Any>& arg) {
    if (auto bigIntValue = arg.template tryCastTo<int64_t>()) {
      out += std::to_string(*bigIntValue);
    } else if (auto doubleValue = arg.template tryCastTo<double>()) {
      out += std::to_string(*doubleValue);
    } else if (auto boolValue = arg.template tryCastTo<bool>()) {
      out += std::to_string(*boolValue);
    } else if (auto arrayView = arg.template tryCastTo<Array<int64_t>>()) {
      // Special handling for array<int64_t> usually this is faster than going
      // through Array<Any>> and casting every element.
      out += "array<int>(";
      for (auto item : *arrayView) {
        if (item.has_value()) {
          out += std::to_string(item.value());
        } else {
          out += "null";
        }
        out += ", ";
      }
      out += ")";
    } else if (auto arrayView = arg.template tryCastTo<Array<Any>>()) {
      printArray(out, *arrayView);
    } else if (auto mapView = arg.template tryCastTo<Map<Any, Any>>()) {
      printMap(out, *mapView);
    } else if (auto stringView = arg.template tryCastTo<Varchar>()) {
      out += *stringView;
    } else if (auto rowView = arg.template tryCastTo<Row<Any, Any>>()) {
      printRow(out, *rowView);
    } else {
      VELOX_UNREACHABLE("type not supported in this function");
    }
  }

  void call(out_type<Varchar>& out, const arg_type<Variadic<Any>>& args) {
    auto i = 0;
    for (const auto& arg : args) {
      out += "arg " + std::to_string(i++) + " : ";
      printItem(out, arg);
      out += "\n";
    }
  }
};

TEST_F(GenericViewTest, tryCastE2E) {
  registerFunction<ToStringFuncTryCastTo, Varchar, Variadic<Any>>(
      {"to_string_try_cast"});

  auto test = [&](const std::string& args, const std::string& expected) {
    auto result = evaluate<SimpleVector<StringView>>(
        fmt::format("to_string_try_cast({})", args),
        makeRowVector({makeFlatVector<int64_t>(1)}));
    ASSERT_EQ(result->valueAt(0).str(), expected);
  };
  test("row_constructor(1,2)", "arg 0 : row(1, 2)\n");

  test(
      "row_constructor(array_constructor(1,2,3),true)",
      "arg 0 : row(array<int>(1, 2, 3, ), 1)\n");

  test(
      "'hi', array_constructor(array_constructor(1.2, 1.4)), 1",
      "arg 0 : hi\narg 1 : array(array(1.200000, 1.400000, ), )\narg 2 : 1\n");

  test(
      "1.3, map(array_constructor(1), array_constructor(null))",
      "arg 0 : 1.300000\narg 1 : map(<1,null>,)\n");
}

template <typename T>
struct ArrayHasDuplicateFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  bool call(bool& out, const arg_type<Array<Any>>& input) {
    std::unordered_set<arg_type<Any>> set;
    for (auto item : input) {
      if (!item.has_value()) {
        // Return null if null is encountered.
        return false;
      }

      if (set.count(*item)) {
        // Item already exisits.
        out = true;
        return true;
      }
      set.insert(*item);
    }
    out = false;
    return true;
  }
};

TEST_F(GenericViewTest, hasDuplicate) {
  registerFunction<ArrayHasDuplicateFunc, bool, Array<Any>>(
      {"has_duplicate_func"});

  auto test = [&](const std::string& arg, bool expected) {
    auto result = evaluate<SimpleVector<bool>>(
        fmt::format("has_duplicate_func(array_constructor({}))", arg),
        makeRowVector({makeFlatVector<int64_t>(1)}));
    ASSERT_EQ(result->valueAt(0), expected);
  };

  test("1,2,3,4,5", false);
  test("1,2,3,4,4", true);

  test("'what','no'", false);
  test("'what','what'", true);

  // Nested array.
  test(
      "array_constructor(1,2,3),array_constructor(1,2), array_constructor(1)",
      false);
  test(
      "array_constructor(1,2,3),array_constructor(1), array_constructor(1)",
      true);
}

TEST_F(GenericViewTest, hasGenericTest) {
  testHasGeneric<int64_t>(false);
  testHasGeneric<Array<int64_t>>(false);
  testHasGeneric<Map<int64_t, int64_t>>(false);
  testHasGeneric<Row<int64_t, int64_t>>(false);
  testHasGeneric<Row<>>(false);

  testHasGeneric<Any>(true);
  testHasGeneric<Array<Any>>(true);
  testHasGeneric<Map<int64_t, Any>>(true);
  testHasGeneric<Map<Any, int64_t>>(true);
  testHasGeneric<Row<Any, int64_t>>(true);
  testHasGeneric<Row<int64_t, Any>>(true);
}

TEST_F(GenericViewTest, allGenericExceptTop) {
  testAllGenericExceptTop<int64_t>(false);
  testAllGenericExceptTop<Array<int64_t>>(false);
  testAllGenericExceptTop<Map<int64_t, int64_t>>(false);
  testAllGenericExceptTop<Row<int64_t, int64_t>>(false);
  testAllGenericExceptTop<Any>(false);
  testAllGenericExceptTop<Map<int64_t, Any>>(false);
  testAllGenericExceptTop<Map<Any, int64_t>>(false);
  testAllGenericExceptTop<Row<Any, int64_t>>(false);
  testAllGenericExceptTop<Row<int64_t, Any>>(false);

  testAllGenericExceptTop<Array<Any>>(true);
  testAllGenericExceptTop<Map<Any, Any>>(true);
  testAllGenericExceptTop<Row<Any, Any>>(true);
  testAllGenericExceptTop<Row<>>(true);
  testAllGenericExceptTop<Row<Any>>(true);
}

} // namespace
} // namespace facebook::velox
