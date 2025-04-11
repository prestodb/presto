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
#include "velox/type/Type.h"
#include <sstream>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/CppToType.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/TypeEncodingUtil.h"
#include "velox/type/tests/utils/CustomTypesForTesting.h"

using namespace facebook;
using namespace facebook::velox;

namespace {
void testTypeSerde(const TypePtr& type) {
  Type::registerSerDe();

  auto copy = velox::ISerializable::deserialize<Type>(
      velox::ISerializable::serialize(type));

  ASSERT_EQ(type->toString(), copy->toString());
  ASSERT_EQ(*type, *copy);
}
} // namespace

TEST(TypeTest, constructorThrow) {
  EXPECT_NO_THROW(RowType({"a", "b"}, {VARCHAR(), INTEGER()}));

  EXPECT_THROW(RowType({"a"}, {VARCHAR(), INTEGER()}), VeloxRuntimeError);
  VELOX_ASSERT_THROW(
      RowType({"a"}, {VARCHAR(), INTEGER()}),
      "Mismatch names/types sizes: "
      "[names: {'a'}, types: {VARCHAR, INTEGER}]");

  EXPECT_THROW(RowType({"a", "b"}, {}), VeloxRuntimeError);
  VELOX_ASSERT_THROW(
      RowType({"a", "b"}, {}),
      "Mismatch names/types sizes: "
      "[names: {'a', 'b'}, types: { }]");

  EXPECT_THROW(RowType({"a", "b"}, {VARCHAR(), nullptr}), VeloxRuntimeError);
  VELOX_ASSERT_THROW(
      RowType({"a", "b"}, {VARCHAR(), nullptr}),
      "Child types cannot be null: "
      "[names: {'a', 'b'}, types: {VARCHAR, NULL}]");
}

TEST(TypeTest, array) {
  const auto arrayType = ARRAY(ARRAY(ARRAY(INTEGER())));
  ASSERT_EQ("ARRAY<ARRAY<ARRAY<INTEGER>>>", arrayType->toString());
  ASSERT_EQ(arrayType->size(), 1);
  EXPECT_STREQ(arrayType->kindName(), "ARRAY");
  ASSERT_EQ(arrayType->isPrimitiveType(), false);
  EXPECT_STREQ(arrayType->elementType()->kindName(), "ARRAY");
  ASSERT_EQ(arrayType->childAt(0)->toString(), "ARRAY<ARRAY<INTEGER>>");
  VELOX_ASSERT_USER_THROW(
      arrayType->childAt(1), "Array type should have only one child");

  EXPECT_STREQ(arrayType->name(), "ARRAY");
  ASSERT_EQ(arrayType->parameters().size(), 1);
  ASSERT_TRUE(arrayType->parameters()[0].kind == TypeParameterKind::kType);
  ASSERT_EQ(*arrayType->parameters()[0].type, *arrayType->childAt(0));
  ASSERT_EQ(approximateTypeEncodingwidth(arrayType), 4);

  ASSERT_EQ(
      *arrayType, *getType("ARRAY", {TypeParameter(ARRAY(ARRAY(INTEGER())))}));

  testTypeSerde(arrayType);
}

TEST(TypeTest, integer) {
  const auto int0 = INTEGER();
  ASSERT_EQ(int0->toString(), "INTEGER");
  ASSERT_EQ(int0->size(), 0);
  VELOX_ASSERT_THROW(int0->childAt(0), "scalar type has no children");
  ASSERT_EQ(int0->kind(), TypeKind::INTEGER);
  EXPECT_STREQ(int0->kindName(), "INTEGER");
  ASSERT_EQ(int0->begin(), int0->end());
  ASSERT_EQ(approximateTypeEncodingwidth(int0), 1);

  testTypeSerde(int0);
}

TEST(TypeTest, hugeint) {
  ASSERT_EQ(getType("HUGEINT", {}), HUGEINT());
}

TEST(TypeTest, timestamp) {
  const auto t0 = TIMESTAMP();
  ASSERT_EQ(t0->toString(), "TIMESTAMP");
  ASSERT_EQ(t0->size(), 0);
  VELOX_ASSERT_THROW(t0->childAt(0), "scalar type has no children");
  ASSERT_EQ(t0->kind(), TypeKind::TIMESTAMP);
  EXPECT_STREQ(t0->kindName(), "TIMESTAMP");
  ASSERT_EQ(t0->begin(), t0->end());
  ASSERT_EQ(approximateTypeEncodingwidth(t0), 1);

  testTypeSerde(t0);
}

TEST(TypeTest, timestampToString) {
  Timestamp epoch(0, 0);
  EXPECT_EQ(epoch.toString(), "1970-01-01T00:00:00.000000000");

  Timestamp beforeEpoch(-1, 890);
  EXPECT_EQ(beforeEpoch.toString(), "1969-12-31T23:59:59.000000890");

  Timestamp year2100(4123638000, 123456789);
  EXPECT_EQ(year2100.toString(), "2100-09-03T07:00:00.123456789");

  Timestamp wayBeforeEpoch(-9999999999, 987654321);
  EXPECT_EQ(wayBeforeEpoch.toString(), "1653-02-10T06:13:21.987654321");
}

TEST(TypeTest, timestampComparison) {
  Timestamp t1(1000, 100);
  Timestamp t1Copy(1000, 100);

  Timestamp t1lessNanos(1000, 99);
  Timestamp t1MoreNanos(1000, 101);

  Timestamp t1lessSeconds(-1000, 10000);
  Timestamp t1MoreSeconds(1001, 0);

  EXPECT_EQ(t1, t1Copy);
  EXPECT_EQ(t1Copy, t1);

  EXPECT_NE(t1, t1lessNanos);
  EXPECT_NE(t1, t1MoreNanos);

  EXPECT_LT(t1, t1MoreNanos);
  EXPECT_LT(t1, t1MoreSeconds);

  EXPECT_LE(t1, t1Copy);
  EXPECT_LE(t1, t1MoreNanos);
  EXPECT_LE(t1, t1MoreSeconds);

  EXPECT_GT(t1, t1lessNanos);
  EXPECT_GT(t1, t1lessSeconds);

  EXPECT_GE(t1, t1Copy);
  EXPECT_GE(t1, t1lessNanos);
  EXPECT_GE(t1, t1lessSeconds);
}

TEST(TypeTest, date) {
  const auto date = DATE();
  ASSERT_EQ(date->toString(), "DATE");
  ASSERT_EQ(date->size(), 0);
  VELOX_ASSERT_THROW(date->childAt(0), "scalar type has no children");
  ASSERT_EQ(date->kind(), TypeKind::INTEGER);
  EXPECT_STREQ(date->kindName(), "INTEGER");
  ASSERT_EQ(date->begin(), date->end());

  ASSERT_TRUE(date->kindEquals(INTEGER()));
  ASSERT_NE(*date, *INTEGER());
  ASSERT_FALSE(date->equivalent(*INTEGER()));
  ASSERT_FALSE(INTEGER()->equivalent(*date));
  ASSERT_EQ(approximateTypeEncodingwidth(date), 1);

  testTypeSerde(date);
}

TEST(TypeTest, intervalDayTime) {
  const auto interval = INTERVAL_DAY_TIME();
  ASSERT_EQ(interval->toString(), "INTERVAL DAY TO SECOND");
  ASSERT_EQ(interval->size(), 0);
  VELOX_ASSERT_THROW(interval->childAt(0), "scalar type has no children");
  ASSERT_EQ(interval->kind(), TypeKind::BIGINT);
  EXPECT_STREQ(interval->kindName(), "BIGINT");
  ASSERT_EQ(interval->begin(), interval->end());
  ASSERT_EQ(approximateTypeEncodingwidth(interval), 1);

  EXPECT_TRUE(interval->kindEquals(BIGINT()));
  ASSERT_NE(*interval, *BIGINT());
  ASSERT_FALSE(interval->equivalent(*BIGINT()));
  ASSERT_FALSE(BIGINT()->equivalent(*interval));

  const int64_t millis = kMillisInDay * 5 + kMillisInHour * 4 +
      kMillisInMinute * 6 + kMillisInSecond * 7 + 98;
  ASSERT_EQ("5 04:06:07.098", INTERVAL_DAY_TIME()->valueToString(millis));

  testTypeSerde(interval);
}

TEST(TypeTest, intervalYearMonth) {
  const auto interval = INTERVAL_YEAR_MONTH();
  ASSERT_EQ(interval->toString(), "INTERVAL YEAR TO MONTH");
  ASSERT_EQ(interval->size(), 0);
  VELOX_ASSERT_THROW(interval->childAt(0), "scalar type has no children");
  ASSERT_EQ(interval->kind(), TypeKind::INTEGER);
  EXPECT_STREQ(interval->kindName(), "INTEGER");
  ASSERT_EQ(interval->begin(), interval->end());
  ASSERT_EQ(approximateTypeEncodingwidth(interval), 1);

  EXPECT_TRUE(interval->kindEquals(INTEGER()));
  ASSERT_NE(*interval, *INTEGER());
  ASSERT_FALSE(interval->equivalent(*INTEGER()));
  EXPECT_FALSE(INTEGER()->equivalent(*interval));

  int32_t month = kMonthInYear * 2 + 1;
  ASSERT_EQ("2-1", INTERVAL_YEAR_MONTH()->valueToString(month));

  month = kMonthInYear * -2 + -1;
  ASSERT_EQ("-2-1", INTERVAL_YEAR_MONTH()->valueToString(month));

  ASSERT_EQ(
      "-178956970-8",
      INTERVAL_YEAR_MONTH()->valueToString(
          std::numeric_limits<int32_t>::min()));

  testTypeSerde(interval);
}

TEST(TypeTest, unknown) {
  const auto type = UNKNOWN();
  ASSERT_EQ(type->toString(), "UNKNOWN");
  ASSERT_EQ(type->size(), 0);
  EXPECT_THROW(type->childAt(0), std::invalid_argument);
  ASSERT_EQ(type->kind(), TypeKind::UNKNOWN);
  EXPECT_STREQ(type->kindName(), "UNKNOWN");
  ASSERT_EQ(type->begin(), type->end());
  ASSERT_TRUE(type->isComparable());
  ASSERT_TRUE(type->isOrderable());
  ASSERT_EQ(approximateTypeEncodingwidth(type), 1);

  testTypeSerde(type);
}

TEST(TypeTest, shortDecimal) {
  const auto shortDecimal = DECIMAL(10, 5);
  ASSERT_EQ(shortDecimal->toString(), "DECIMAL(10, 5)");
  ASSERT_EQ(shortDecimal->size(), 0);
  VELOX_ASSERT_THROW(shortDecimal->childAt(0), "scalar type has no children");
  ASSERT_EQ(shortDecimal->kind(), TypeKind::BIGINT);
  ASSERT_EQ(shortDecimal->begin(), shortDecimal->end());
  ASSERT_EQ(approximateTypeEncodingwidth(shortDecimal), 1);

  ASSERT_EQ(*DECIMAL(10, 5), *shortDecimal);
  ASSERT_NE(*DECIMAL(9, 5), *shortDecimal);
  ASSERT_NE(*DECIMAL(10, 4), *shortDecimal);

  VELOX_ASSERT_THROW(
      DECIMAL(0, 0), "Precision of decimal type must be at least 1");

  EXPECT_STREQ(shortDecimal->name(), "DECIMAL");
  ASSERT_EQ(shortDecimal->parameters().size(), 2);
  ASSERT_TRUE(
      shortDecimal->parameters()[0].kind == TypeParameterKind::kLongLiteral);
  ASSERT_EQ(shortDecimal->parameters()[0].longLiteral.value(), 10);
  ASSERT_TRUE(
      shortDecimal->parameters()[1].kind == TypeParameterKind::kLongLiteral);
  ASSERT_EQ(shortDecimal->parameters()[1].longLiteral.value(), 5);

  ASSERT_EQ(
      *shortDecimal,
      *getType(
          "DECIMAL",
          {
              TypeParameter(10),
              TypeParameter(5),
          }));

  testTypeSerde(shortDecimal);
}

TEST(TypeTest, longDecimal) {
  const auto longDecimal = DECIMAL(30, 5);
  ASSERT_EQ(longDecimal->toString(), "DECIMAL(30, 5)");
  ASSERT_EQ(longDecimal->size(), 0);
  VELOX_ASSERT_THROW(longDecimal->childAt(0), "scalar type has no children");
  ASSERT_EQ(longDecimal->kind(), TypeKind::HUGEINT);
  ASSERT_EQ(longDecimal->begin(), longDecimal->end());
  ASSERT_EQ(*DECIMAL(30, 5), *longDecimal);
  ASSERT_NE(*DECIMAL(9, 5), *longDecimal);
  ASSERT_NE(*DECIMAL(30, 3), *longDecimal);
  VELOX_ASSERT_THROW(
      DECIMAL(39, 5), "Precision of decimal type must not exceed 38");
  VELOX_ASSERT_THROW(
      DECIMAL(25, 26), "Scale of decimal type must not exceed its precision");

  ASSERT_EQ(approximateTypeEncodingwidth(longDecimal), 1);
  EXPECT_STREQ(longDecimal->name(), "DECIMAL");
  ASSERT_EQ(longDecimal->parameters().size(), 2);
  EXPECT_TRUE(
      longDecimal->parameters()[0].kind == TypeParameterKind::kLongLiteral);
  ASSERT_EQ(longDecimal->parameters()[0].longLiteral.value(), 30);
  ASSERT_TRUE(
      longDecimal->parameters()[1].kind == TypeParameterKind::kLongLiteral);
  ASSERT_EQ(longDecimal->parameters()[1].longLiteral.value(), 5);

  ASSERT_EQ(
      *longDecimal,
      *getType(
          "DECIMAL",
          {
              TypeParameter(30),
              TypeParameter(5),
          }));

  testTypeSerde(longDecimal);
}

TEST(TypeTest, dateToString) {
  EXPECT_EQ(DATE()->toString(0), "1970-01-01");

  // 50 years after epoch
  EXPECT_EQ(DATE()->toString(18262), "2020-01-01");

  EXPECT_EQ(DATE()->toString(-5), "1969-12-27");

  // 50 years before epoch
  EXPECT_EQ(DATE()->toString(-18262), "1920-01-02");

  // Trying a very large -integer for boundary checks. Such values are tested in
  // ExpressionFuzzer.
  // Since we use int64 for the intermediate conversion of days to ms,
  // the large -ve value remains valid. However, gmtime uses int32
  // for the number of years, so the eventual results might look like garbage.
  // However, they are consistent with presto java so keeping the same
  // implementation.
  EXPECT_EQ(DATE()->toString(-1855961014), "-5079479-05-03");
}

TEST(TypeTest, parseStringToDate) {
  auto parseDate = [](const std::string& dateStr) {
    return DATE()->toDays(dateStr);
  };

  // Epoch.
  EXPECT_EQ(parseDate("1970-01-01"), 0);

  // 50 years after epoch.
  EXPECT_EQ(parseDate("2020-01-01"), 18262);

  // Before epoch.
  EXPECT_EQ(parseDate("1969-12-27"), -5);

  // 50 years before epoch.
  EXPECT_EQ(parseDate("1920-01-02"), -18262);

  // Century before epoch.
  EXPECT_EQ(parseDate("1812-04-15"), -57604);

  // Century after epoch.
  EXPECT_EQ(parseDate("2135-11-09"), 60577);
}

TEST(TypeTest, dateFormat) {
  auto parseDate = [](const std::string& dateStr) {
    return DATE()->toString(DATE()->toDays(dateStr));
  };

  EXPECT_EQ(fmt::format("{}", parseDate("2015-12-24")), "2015-12-24");
  EXPECT_EQ(fmt::format("{}", parseDate("1970-01-01")), "1970-01-01");
  EXPECT_EQ(fmt::format("{}", parseDate("2000-03-10")), "2000-03-10");
  EXPECT_EQ(fmt::format("{}", parseDate("1945-05-20")), "1945-05-20");
  EXPECT_EQ(fmt::format("{}", parseDate("2135-11-09")), "2135-11-09");
  EXPECT_EQ(fmt::format("{}", parseDate("1812-04-15")), "1812-04-15");
}

TEST(TypeTest, map) {
  const auto mapType = MAP(INTEGER(), ARRAY(BIGINT()));
  ASSERT_EQ(mapType->toString(), "MAP<INTEGER,ARRAY<BIGINT>>");
  ASSERT_EQ(mapType->size(), 2);
  ASSERT_EQ(mapType->childAt(0)->toString(), "INTEGER");
  ASSERT_EQ(mapType->childAt(1)->toString(), "ARRAY<BIGINT>");
  VELOX_ASSERT_USER_THROW(
      mapType->childAt(2), "Map type should have only two children");
  ASSERT_EQ(mapType->kind(), TypeKind::MAP);
  EXPECT_STREQ(mapType->kindName(), "MAP");
  int32_t num = 0;
  for (auto& i : *mapType) {
    if (num == 0) {
      ASSERT_EQ(i->toString(), "INTEGER");
    } else if (num == 1) {
      ASSERT_EQ(i->toString(), "ARRAY<BIGINT>");
    } else {
      FAIL();
    }
    ++num;
  }
  ASSERT_EQ(num, 2);

  EXPECT_STREQ(mapType->name(), "MAP");
  ASSERT_EQ(mapType->parameters().size(), 2);
  for (auto i = 0; i < 2; ++i) {
    ASSERT_TRUE(mapType->parameters()[i].kind == TypeParameterKind::kType);
    ASSERT_EQ(*mapType->parameters()[i].type, *mapType->childAt(i));
  }

  ASSERT_EQ(
      *mapType,
      *getType(
          "MAP",
          {
              TypeParameter(INTEGER()),
              TypeParameter(ARRAY(BIGINT())),
          }));

  ASSERT_EQ(approximateTypeEncodingwidth(mapType), 4);

  testTypeSerde(mapType);
}

TEST(TypeTest, row) {
  VELOX_ASSERT_THROW(ROW({{"a", nullptr}}), "Child types cannot be null");
  auto row0 = ROW({{"a", INTEGER()}, {"b", ROW({{"a", BIGINT()}})}});
  auto rowInner = row0->childAt(1);
  ASSERT_EQ(row0->toString(), "ROW<a:INTEGER,b:ROW<a:BIGINT>>");
  ASSERT_EQ(row0->size(), 2);
  ASSERT_EQ(rowInner->size(), 1);
  EXPECT_STREQ(row0->childAt(0)->kindName(), "INTEGER");
  EXPECT_STREQ(row0->findChild("a")->kindName(), "INTEGER");
  ASSERT_EQ(row0->nameOf(0), "a");
  ASSERT_EQ(row0->nameOf(1), "b");
  VELOX_ASSERT_THROW(row0->nameOf(4), "");
  VELOX_ASSERT_USER_THROW(row0->findChild("not_exist"), "Field not found");
  // TODO: expected case behavior?:
  VELOX_ASSERT_THROW(
      row0->findChild("A"), "Field not found: A. Available fields are: a, b.");
  ASSERT_EQ(row0->childAt(1)->toString(), "ROW<a:BIGINT>");
  ASSERT_EQ(row0->findChild("b")->toString(), "ROW<a:BIGINT>");
  ASSERT_EQ(row0->findChild("b")->asRow().findChild("a")->toString(), "BIGINT");
  ASSERT_TRUE(row0->containsChild("a"));
  ASSERT_TRUE(row0->containsChild("b"));
  ASSERT_FALSE(row0->containsChild("c"));
  int32_t seen = 0;
  for (auto& i : *row0) {
    if (seen == 0) {
      EXPECT_STREQ("INTEGER", i->kindName());
    } else if (seen == 1) {
      EXPECT_EQ("ROW<a:BIGINT>", i->toString());
      int32_t seen2 = 0;
      for (auto& j : *i) {
        ASSERT_EQ(j->toString(), "BIGINT");
        seen2++;
      }
      ASSERT_EQ(seen2, 1);
    }
    seen++;
  }
  ASSERT_EQ(seen, 2);
  ASSERT_EQ(approximateTypeEncodingwidth(row0), 2);

  EXPECT_STREQ(row0->name(), "ROW");
  ASSERT_EQ(row0->parameters().size(), 2);
  for (auto i = 0; i < 2; ++i) {
    ASSERT_TRUE(row0->parameters()[i].kind == TypeParameterKind::kType);
    ASSERT_EQ(*row0->parameters()[i].type, *row0->childAt(i));
  }

  const auto row1 =
      ROW({{"a,b", INTEGER()}, {R"(my "column")", ROW({{"#1", BIGINT()}})}});
  ASSERT_EQ(
      row1->toString(),
      R"(ROW<"a,b":INTEGER,"my ""column""":ROW<"#1":BIGINT>>)");
  ASSERT_EQ(row1->nameOf(0), "a,b");
  ASSERT_EQ(row1->nameOf(1), R"(my "column")");
  ASSERT_EQ(row1->childAt(1)->toString(), R"(ROW<"#1":BIGINT>)");
  ASSERT_EQ(approximateTypeEncodingwidth(row1), 2);

  const auto row2 = ROW({{"", INTEGER()}});
  ASSERT_EQ(row2->toString(), R"(ROW<"":INTEGER>)");
  ASSERT_EQ(row2->nameOf(0), "");
  ASSERT_EQ(approximateTypeEncodingwidth(row2), 1);

  VELOX_ASSERT_THROW(createScalarType(TypeKind::ROW), "not a scalar type");
  VELOX_ASSERT_THROW(
      createType(TypeKind::ROW, {}), "Not supported for kind: ROW");

  testTypeSerde(row0);
  testTypeSerde(row1);
  testTypeSerde(row2);
  testTypeSerde(rowInner);
}

TEST(TypeTest, serdeCache) {
  std::vector<std::string> names;
  names.reserve(100);
  for (auto i = 0; i < 100; ++i) {
    names.push_back(fmt::format("c{}", i));
  }
  std::vector<TypePtr> types(100, REAL());
  auto rowType = ROW(std::move(names), std::move(types));

  auto& cache = serializedTypeCache();
  ASSERT_FALSE(cache.isEnabled());

  testTypeSerde(rowType);
  ASSERT_EQ(0, cache.size());

  cache.enable();
  SCOPE_EXIT {
    cache.disable();
    cache.clear();
    deserializedTypeCache().clear();
  };

  folly::dynamic serializedType;
  for (auto i = 0; i < 10; ++i) {
    serializedType = rowType->serialize();
    ASSERT_EQ(1, cache.size());
  }

  auto serializedCache = cache.serialize();

  ASSERT_EQ(0, deserializedTypeCache().size());
  deserializedTypeCache().deserialize(serializedCache);
  ASSERT_EQ(1, deserializedTypeCache().size());

  auto copy = velox::ISerializable::deserialize<Type>(serializedType);
  ASSERT_EQ(rowType->toString(), copy->toString());
  ASSERT_EQ(*rowType, *copy);
}

TEST(TypeTest, emptyRow) {
  auto row = ROW({});
  testTypeSerde(row);
}

TEST(TypeTest, rowParametersMultiThreaded) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (int i = 0; i < 20'000; ++i) {
    auto name = fmt::format("c{}", i);
    names.push_back(name);
    types.push_back(ROW({name}, {BIGINT()}));
  }
  auto type = ROW(std::move(names), std::move(types));
  constexpr int kNumThreads = 72;
  const std::vector<TypeParameter>* parameters[kNumThreads];
  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i] { parameters[i] = &type->parameters(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (int i = 1; i < kNumThreads; ++i) {
    ASSERT_TRUE(parameters[i] == parameters[0]);
  }
  ASSERT_EQ(parameters[0]->size(), type->size());
  for (int i = 0; i < parameters[0]->size(); ++i) {
    ASSERT_TRUE((*parameters[0])[i].type.get() == type->childAt(i).get());
  }
}

TEST(TypeTest, rowHashKindMultiThreaded) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (int i = 0; i < 20'000; ++i) {
    auto name = fmt::format("c{}", i);
    names.push_back(name);
    types.push_back(ROW({name}, {BIGINT()}));
  }
  auto type = ROW(std::move(names), std::move(types));
  constexpr int kNumThreads = 72;
  size_t hashes[kNumThreads];
  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i] { hashes[i] = type->hashKind(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (int i = 1; i < kNumThreads; ++i) {
    ASSERT_TRUE(hashes[i] == hashes[0]);
  }
}

class Foo {};
class Bar {};

TEST(TypeTest, opaque) {
  const auto foo = OpaqueType::create<Foo>();
  VELOX_ASSERT_THROW(
      approximateTypeEncodingwidth(foo), "Unsupported type: OPAQUE<Foo>");
  const auto bar = OpaqueType::create<Bar>();
  // Names currently use typeid which is not stable across platforms. We'd
  // need to change it later if we start serializing opaque types, e.g. we can
  // ask user to "register" the name for the type explicitly.
  ASSERT_NE(std::string::npos, foo->toString().find("OPAQUE<"));
  ASSERT_NE(std::string::npos, foo->toString().find("Foo"));
  ASSERT_EQ(foo->size(), 0);
  VELOX_ASSERT_THROW(foo->childAt(0), "OpaqueType type has no children");
  EXPECT_STREQ(foo->kindName(), "OPAQUE");
  ASSERT_EQ(foo->isPrimitiveType(), false);

  auto foo2 = OpaqueType::create<Foo>();
  ASSERT_NE(*foo, *bar);
  ASSERT_EQ(*foo, *foo2);

  OpaqueType::registerSerialization<Foo>("id_of_foo");
  ASSERT_EQ(foo->serialize()["opaque"], "id_of_foo");
  VELOX_ASSERT_THROW(
      foo->getSerializeFunc(),
      "No serialization function registered for OPAQUE<Foo>");
  VELOX_ASSERT_THROW(
      foo->getDeserializeFunc(),
      "No deserialization function registered for OPAQUE<Foo>");
  VELOX_ASSERT_THROW(
      bar->serialize(),
      "No serialization persistent name registered for OPAQUE<Bar>");
  VELOX_ASSERT_THROW(
      bar->getSerializeFunc(),
      "No serialization function registered for OPAQUE<Bar>");
  VELOX_ASSERT_THROW(
      bar->getDeserializeFunc(),
      "No deserialization function registered for OPAQUE<Bar>");

  auto foo3 = Type::create(foo->serialize());
  ASSERT_EQ(*foo, *foo3);

  OpaqueType::registerSerialization<Bar>(
      "id_of_bar",
      [](const std::shared_ptr<Bar>&) -> std::string { return ""; },
      [](const std::string&) -> std::shared_ptr<Bar> { return nullptr; });
  bar->getSerializeFunc();
  bar->getDeserializeFunc();
}

// Example of an opaque type that keeps some additional type-level metadata.
// It's not a common case, but may be useful for some applications
class OpaqueWithMetadata {};
class OpaqueWithMetadataType : public OpaqueType {
 public:
  explicit OpaqueWithMetadataType(int metadata)
      : OpaqueType(std::type_index(typeid(OpaqueWithMetadata))),
        metadata(metadata) {}

  bool operator==(const Type& other) const override {
    return OpaqueType::operator==(other) &&
        reinterpret_cast<const OpaqueWithMetadataType*>(&other)->metadata ==
        metadata;
  }

  folly::dynamic serialize() const override {
    auto r = OpaqueType::serialize();
    r["my_extra"] = metadata;
    return r;
  }

  std::shared_ptr<const OpaqueType> deserializeExtra(
      const folly::dynamic& json) const override {
    return std::make_shared<OpaqueWithMetadataType>(json["my_extra"].asInt());
  }

  const int metadata;
};

namespace facebook::velox {
template <>
std::shared_ptr<const OpaqueType> OpaqueType::create<OpaqueWithMetadata>() {
  return std::make_shared<OpaqueWithMetadataType>(-1);
}
} // namespace facebook::velox

TEST(TypeTest, opaqueWithMetadata) {
  auto def = OpaqueType::create<OpaqueWithMetadata>();
  auto type = std::make_shared<OpaqueWithMetadataType>(123);
  auto type2 = std::make_shared<OpaqueWithMetadataType>(123);
  auto other = std::make_shared<OpaqueWithMetadataType>(234);
  EXPECT_TRUE(def->operator!=(*type));
  EXPECT_EQ(*type, *type2);
  EXPECT_NE(*type, *other);

  OpaqueType::registerSerialization<OpaqueWithMetadata>("my_fancy_type");

  EXPECT_EQ(*Type::create(type->serialize()), *type);
  EXPECT_EQ(
      std::dynamic_pointer_cast<const OpaqueWithMetadataType>(
          Type::create(type->serialize()))
          ->metadata,
      123);
  EXPECT_EQ(
      std::dynamic_pointer_cast<const OpaqueWithMetadataType>(
          Type::create(other->serialize()))
          ->metadata,
      234);
}

TEST(TypeTest, fluentCast) {
  std::shared_ptr<const Type> t = INTEGER();
  EXPECT_THROW(t->asBigint(), std::bad_cast);
  EXPECT_EQ(t->asInteger().toString(), "INTEGER");
}

const std::string* firstFieldNameOrNull(const Type& type) {
  // shows different ways of casting & pattern matching
  switch (type.kind()) {
    case TypeKind::ROW:
      EXPECT_TRUE(type.isRow());
      return &type.asRow().nameOf(0);
    default:
      return nullptr;
  }
}

TEST(TypeTest, patternMatching) {
  auto a = ROW({{"a", INTEGER()}});
  auto b = BIGINT();
  EXPECT_EQ(*firstFieldNameOrNull(*a), "a");
  EXPECT_EQ(firstFieldNameOrNull(*b), nullptr);
}

TEST(TypeTest, equality) {
  // scalar
  EXPECT_TRUE(*INTEGER() == *INTEGER());
  EXPECT_FALSE(*INTEGER() != *INTEGER());
  EXPECT_FALSE(INTEGER()->operator==(*REAL()));

  // map
  EXPECT_TRUE(*MAP(INTEGER(), REAL()) == *MAP(INTEGER(), REAL()));
  EXPECT_FALSE(*MAP(REAL(), INTEGER()) == *MAP(INTEGER(), REAL()));
  EXPECT_FALSE(*MAP(REAL(), INTEGER()) == *MAP(REAL(), BIGINT()));
  EXPECT_FALSE(*MAP(REAL(), INTEGER()) == *MAP(BIGINT(), INTEGER()));

  // arr
  EXPECT_TRUE(*ARRAY(INTEGER()) == *ARRAY(INTEGER()));
  EXPECT_FALSE(*ARRAY(INTEGER()) == *ARRAY(REAL()));
  EXPECT_FALSE(*ARRAY(INTEGER()) == *ARRAY(ARRAY(INTEGER())));

  // struct
  EXPECT_TRUE(
      *ROW({{"a", INTEGER()}, {"b", REAL()}}) ==
      *ROW({{"a", INTEGER()}, {"b", REAL()}}));
  EXPECT_TRUE(
      *ROW({{"a", INTEGER()}, {"b", MAP(INTEGER(), INTEGER())}}) ==
      *ROW({{"a", INTEGER()}, {"b", MAP(INTEGER(), INTEGER())}}));
  EXPECT_FALSE(
      *ROW({{"a", INTEGER()}, {"b", REAL()}}) ==
      *ROW({{"a", INTEGER()}, {"b", BIGINT()}}));
  EXPECT_FALSE(
      *ROW({{"a", INTEGER()}, {"b", REAL()}}) == *ROW({{"a", INTEGER()}}));
  EXPECT_FALSE(
      *ROW({{"a", INTEGER()}}) == *ROW({{"a", INTEGER()}, {"b", REAL()}}));
  EXPECT_FALSE(
      *ROW({{"a", INTEGER()}, {"b", REAL()}}) ==
      *ROW({{"a", INTEGER()}, {"d", REAL()}}));
  EXPECT_FALSE(
      *ROW({{"a", ROW({{"x", INTEGER()}, {"y", INTEGER()}})}}) ==
      *ROW({{"a", ROW({{"x", INTEGER()}, {"z", INTEGER()}})}}));
  EXPECT_FALSE(
      *ROW({{"a", ROW({{"x", INTEGER()}, {"y", INTEGER()}})}}) ==
      *ARRAY(INTEGER()));

  // mix
  EXPECT_FALSE(MAP(REAL(), INTEGER())
                   ->operator==(*ROW({{"a", REAL()}, {"b", INTEGER()}})));
  EXPECT_FALSE(ARRAY(REAL())->operator==(*ROW({{"a", REAL()}})));
}

TEST(TypeTest, cpp2Type) {
  EXPECT_EQ(*CppToType<int64_t>::create(), *BIGINT());
  EXPECT_EQ(*CppToType<int32_t>::create(), *INTEGER());
  EXPECT_EQ(*CppToType<int16_t>::create(), *SMALLINT());
  EXPECT_EQ(*CppToType<int8_t>::create(), *TINYINT());
  EXPECT_EQ(*CppToType<velox::StringView>::create(), *VARCHAR());
  EXPECT_EQ(*CppToType<std::string>::create(), *VARCHAR());
  EXPECT_EQ(*CppToType<folly::ByteRange>::create(), *VARBINARY());
  EXPECT_EQ(*CppToType<float>::create(), *REAL());
  EXPECT_EQ(*CppToType<double>::create(), *DOUBLE());
  EXPECT_EQ(*CppToType<bool>::create(), *BOOLEAN());
  EXPECT_EQ(*CppToType<Timestamp>::create(), *TIMESTAMP());
  EXPECT_EQ(*CppToType<Date>::create(), *INTEGER());
  EXPECT_EQ(*CppToType<Array<int32_t>>::create(), *ARRAY(INTEGER()));
  auto type = CppToType<Map<int32_t, Map<int64_t, float>>>::create();
  EXPECT_EQ(*type, *MAP(INTEGER(), MAP(BIGINT(), REAL())));
}

TEST(TypeTest, equivalent) {
  EXPECT_TRUE(ROW({{"a", BIGINT()}})->equivalent(*ROW({{"b", BIGINT()}})));
  EXPECT_FALSE(ROW({{"a", BIGINT()}})->equivalent(*ROW({{"a", INTEGER()}})));
  EXPECT_TRUE(ROW({{"a", BIGINT()}})->equivalent(*ROW({{"b", BIGINT()}})));
  EXPECT_TRUE(MAP(BIGINT(), BIGINT())->equivalent(*MAP(BIGINT(), BIGINT())));
  EXPECT_FALSE(
      MAP(BIGINT(), BIGINT())->equivalent(*MAP(BIGINT(), ARRAY(BIGINT()))));
  EXPECT_TRUE(ARRAY(BIGINT())->equivalent(*ARRAY(BIGINT())));
  EXPECT_FALSE(ARRAY(BIGINT())->equivalent(*ARRAY(INTEGER())));
  EXPECT_FALSE(ARRAY(BIGINT())->equivalent(*ROW({{"a", BIGINT()}})));
  EXPECT_TRUE(DECIMAL(10, 5)->equivalent(*DECIMAL(10, 5)));
  EXPECT_FALSE(DECIMAL(10, 6)->equivalent(*DECIMAL(10, 5)));
  EXPECT_FALSE(DECIMAL(11, 5)->equivalent(*DECIMAL(10, 5)));
  EXPECT_TRUE(DECIMAL(30, 5)->equivalent(*DECIMAL(30, 5)));
  EXPECT_FALSE(DECIMAL(30, 6)->equivalent(*DECIMAL(30, 5)));
  EXPECT_FALSE(DECIMAL(31, 5)->equivalent(*DECIMAL(30, 5)));
  auto complexTypeA = ROW(
      {{"a0", ARRAY(ROW({{"a1", DECIMAL(20, 8)}}))},
       {"a2", MAP(VARCHAR(), ROW({{"a3", DECIMAL(10, 5)}}))}});
  auto complexTypeB = ROW(
      {{"b0", ARRAY(ROW({{"b1", DECIMAL(20, 8)}}))},
       {"b2", MAP(VARCHAR(), ROW({{"b3", DECIMAL(10, 5)}}))}});
  EXPECT_TRUE(complexTypeA->equivalent(*complexTypeB));
  // Change Array element type.
  complexTypeB = ROW(
      {{"b0", ARRAY(ROW({{"b1", DECIMAL(20, 7)}}))},
       {"b2", MAP(VARCHAR(), ROW({{"b3", DECIMAL(10, 5)}}))}});
  EXPECT_FALSE(complexTypeA->equivalent(*complexTypeB));
  // Change Map value type.
  complexTypeB = ROW(
      {{"b0", ARRAY(ROW({{"b1", DECIMAL(20, 8)}}))},
       {"b2", MAP(VARCHAR(), ROW({{"b3", DECIMAL(20, 5)}}))}});
  EXPECT_FALSE(complexTypeA->equivalent(*complexTypeB));
}

TEST(TypeTest, kindEquals) {
  EXPECT_TRUE(ROW({{"a", BIGINT()}})->kindEquals(ROW({{"b", BIGINT()}})));
  EXPECT_FALSE(ROW({{"a", BIGINT()}})->kindEquals(ROW({{"a", INTEGER()}})));
  EXPECT_TRUE(MAP(BIGINT(), BIGINT())->kindEquals(MAP(BIGINT(), BIGINT())));
  EXPECT_FALSE(
      MAP(BIGINT(), BIGINT())->kindEquals(MAP(BIGINT(), ARRAY(BIGINT()))));
  EXPECT_TRUE(ARRAY(BIGINT())->kindEquals(ARRAY(BIGINT())));
  EXPECT_FALSE(ARRAY(BIGINT())->kindEquals(ARRAY(INTEGER())));
  EXPECT_FALSE(ARRAY(BIGINT())->kindEquals(ROW({{"a", BIGINT()}})));
  EXPECT_TRUE(DECIMAL(10, 5)->kindEquals(DECIMAL(10, 5)));
  EXPECT_TRUE(DECIMAL(10, 6)->kindEquals(DECIMAL(10, 5)));
  EXPECT_TRUE(DECIMAL(11, 5)->kindEquals(DECIMAL(10, 5)));
  EXPECT_TRUE(DECIMAL(30, 5)->kindEquals(DECIMAL(30, 5)));
  EXPECT_TRUE(DECIMAL(30, 6)->kindEquals(DECIMAL(30, 5)));
  EXPECT_TRUE(DECIMAL(31, 5)->kindEquals(DECIMAL(30, 5)));
}

TEST(TypeTest, kindHash) {
  EXPECT_EQ(BIGINT()->hashKind(), BIGINT()->hashKind());
  EXPECT_EQ(TIMESTAMP()->hashKind(), TIMESTAMP()->hashKind());
  EXPECT_EQ(DATE()->hashKind(), DATE()->hashKind());
  EXPECT_NE(BIGINT()->hashKind(), INTEGER()->hashKind());
  EXPECT_EQ(
      ROW({{"a", BIGINT()}})->hashKind(), ROW({{"b", BIGINT()}})->hashKind());
  EXPECT_EQ(
      MAP(BIGINT(), BIGINT())->hashKind(), MAP(BIGINT(), BIGINT())->hashKind());
  EXPECT_NE(
      MAP(BIGINT(), BIGINT())->hashKind(),
      MAP(BIGINT(), ARRAY(BIGINT()))->hashKind());
  EXPECT_EQ(ARRAY(BIGINT())->hashKind(), ARRAY(BIGINT())->hashKind());
  EXPECT_NE(ARRAY(BIGINT())->hashKind(), ARRAY(INTEGER())->hashKind());
  EXPECT_NE(ARRAY(BIGINT())->hashKind(), ROW({{"a", BIGINT()}})->hashKind());
}

template <TypeKind KIND>
int32_t returnKindIntPlus(int32_t val) {
  return (int32_t)KIND + val;
}

TEST(TypeTest, dynamicTypeDispatch) {
  auto val1 =
      VELOX_DYNAMIC_TYPE_DISPATCH(returnKindIntPlus, TypeKind::INTEGER, 1);
  EXPECT_EQ(val1, (int32_t)TypeKind::INTEGER + 1);

  auto val2 = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      returnKindIntPlus, TypeKind::BIGINT, 2);
  EXPECT_EQ(val2, (int32_t)TypeKind::BIGINT + 2);
}

TEST(TypeTest, kindStreamOp) {
  std::stringbuf buf;
  std::ostream os(&buf);
  os << TypeKind::BIGINT;
  EXPECT_EQ(buf.str(), "BIGINT");
}

TEST(TypeTest, function) {
  auto type = std::make_shared<FunctionType>(
      std::vector<TypePtr>{BIGINT(), VARCHAR()}, BOOLEAN());
  EXPECT_EQ(3, type->size());
  EXPECT_EQ(BIGINT(), type->childAt(0));
  EXPECT_EQ(VARCHAR(), type->childAt(1));
  EXPECT_EQ(BOOLEAN(), type->childAt(2));

  EXPECT_STREQ(type->name(), "FUNCTION");
  EXPECT_EQ(type->parameters().size(), 3);
  for (auto i = 0; i < 3; ++i) {
    EXPECT_TRUE(type->parameters()[i].kind == TypeParameterKind::kType);
    EXPECT_EQ(*type->parameters()[i].type, *type->childAt(i));
  }

  EXPECT_EQ(
      *type,
      *getType(
          "FUNCTION",
          {
              TypeParameter(BIGINT()),
              TypeParameter(VARCHAR()),
              TypeParameter(BOOLEAN()),
          }));

  testTypeSerde(type);
}

TEST(TypeTest, follySformat) {
  EXPECT_EQ("BOOLEAN", folly::sformat("{}", BOOLEAN()));
  EXPECT_EQ("TINYINT", folly::sformat("{}", TINYINT()));
  EXPECT_EQ("SMALLINT", folly::sformat("{}", SMALLINT()));
  EXPECT_EQ("INTEGER", folly::sformat("{}", INTEGER()));
  EXPECT_EQ("BIGINT", folly::sformat("{}", BIGINT()));
  EXPECT_EQ("REAL", folly::sformat("{}", REAL()));
  EXPECT_EQ("DOUBLE", folly::sformat("{}", DOUBLE()));
  EXPECT_EQ("VARCHAR", folly::sformat("{}", VARCHAR()));
  EXPECT_EQ("VARBINARY", folly::sformat("{}", VARBINARY()));
  EXPECT_EQ("TIMESTAMP", folly::sformat("{}", TIMESTAMP()));
  EXPECT_EQ("DATE", folly::sformat("{}", DATE()));

  EXPECT_EQ("ARRAY<VARCHAR>", folly::sformat("{}", ARRAY(VARCHAR())));
  EXPECT_EQ(
      "MAP<VARCHAR,BIGINT>", folly::sformat("{}", MAP(VARCHAR(), BIGINT())));
  EXPECT_EQ(
      "ROW<\"\":BOOLEAN,\"\":VARCHAR,\"\":BIGINT>",
      folly::sformat("{}", ROW({BOOLEAN(), VARCHAR(), BIGINT()})));
  EXPECT_EQ(
      "ROW<a:BOOLEAN,b:VARCHAR,c:BIGINT>",
      folly::sformat(
          "{}", ROW({{"a", BOOLEAN()}, {"b", VARCHAR()}, {"c", BIGINT()}})));
}

TEST(TypeTest, unknownArray) {
  const auto unknownArray = ARRAY(UNKNOWN());
  ASSERT_EQ(approximateTypeEncodingwidth(unknownArray), 2);
  ASSERT_TRUE(unknownArray->containsUnknown());

  testTypeSerde(unknownArray);

  ASSERT_EQ(0, unknownArray->elementType()->cppSizeInBytes());
}

TEST(TypeTest, isVariadicType) {
  EXPECT_TRUE(isVariadicType<Variadic<int64_t>>::value);
  EXPECT_TRUE(isVariadicType<Variadic<Array<float>>>::value);
  EXPECT_FALSE(isVariadicType<velox::StringView>::value);
  EXPECT_FALSE(isVariadicType<bool>::value);
  EXPECT_FALSE((isVariadicType<Map<int8_t, Date>>::value));
}

TEST(TypeTest, rowEquvialentCheckWithChildRowsWithDifferentNames) {
  std::vector<TypePtr> types;
  std::vector<TypePtr> typesWithDifferentNames;
  RowTypePtr childRowType1 =
      ROW({"a", "b", "c"}, {TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr childRowType2 =
      ROW({"d", "e", "f"}, {TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr childRowType3 =
      ROW({"x", "y", "z"}, {TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr childRowType1WithDifferentNames =
      ROW({"A", "B", "C"}, {TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr childRowType2WithDifferentNames =
      ROW({"D", "E", "F"}, {TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr childRowType3WithDifferentNames =
      ROW({"X", "Y", "Z"}, {TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr rowType =
      ROW({"A", "B", "C"}, {childRowType1, childRowType2, childRowType3});
  RowTypePtr rowTypeWithDifferentName =
      ROW({"a", "b", "c"},
          {childRowType1WithDifferentNames,
           childRowType2WithDifferentNames,
           childRowType3WithDifferentNames});
  ASSERT_TRUE(rowTypeWithDifferentName->equivalent(*rowType));
}

TEST(TypeTest, unionWith) {
  std::vector<TypePtr> types;
  std::vector<TypePtr> typesWithDifferentNames;
  RowTypePtr emptyRowType = ROW({}, {});
  RowTypePtr childRowType1 =
      ROW({"a", "b", "c"}, {TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr childRowType2 =
      ROW({"d", "e", "f"}, {TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr resultRowType =
      ROW({"a", "b", "c", "d", "e", "f"},
          {TINYINT(), VARBINARY(), BIGINT(), TINYINT(), VARBINARY(), BIGINT()});
  RowTypePtr resultRowType2 =
      ROW({"a", "b", "c", "a", "b", "c"},
          {TINYINT(), VARBINARY(), BIGINT(), TINYINT(), VARBINARY(), BIGINT()});

  ASSERT_TRUE(
      emptyRowType->unionWith(childRowType1)->equivalent(*childRowType1));
  ASSERT_TRUE(
      childRowType1->unionWith(childRowType2)->equivalent(*resultRowType));
  ASSERT_TRUE(
      childRowType1->unionWith(childRowType1)->equivalent(*resultRowType2));
}

TEST(TypeTest, orderableComparable) {
  // Scalar type.
  EXPECT_TRUE(INTEGER()->isOrderable());
  EXPECT_TRUE(INTEGER()->isComparable());
  EXPECT_TRUE(REAL()->isOrderable());
  EXPECT_TRUE(REAL()->isComparable());
  EXPECT_TRUE(VARCHAR()->isOrderable());
  EXPECT_TRUE(VARCHAR()->isComparable());
  EXPECT_TRUE(BIGINT()->isOrderable());
  EXPECT_TRUE(BIGINT()->isComparable());
  EXPECT_TRUE(DOUBLE()->isOrderable());
  EXPECT_TRUE(DOUBLE()->isComparable());

  // Map type.
  auto mapType = MAP(INTEGER(), REAL());
  EXPECT_FALSE(mapType->isOrderable());
  EXPECT_TRUE(mapType->isComparable());

  // Array type.
  auto arrayType = ARRAY(INTEGER());
  EXPECT_TRUE(arrayType->isOrderable());
  EXPECT_TRUE(arrayType->isComparable());

  arrayType = ARRAY(mapType);
  EXPECT_FALSE(arrayType->isOrderable());
  EXPECT_TRUE(arrayType->isComparable());

  // Row type.
  auto rowType = ROW({INTEGER(), REAL()});
  EXPECT_TRUE(rowType->isOrderable());
  EXPECT_TRUE(rowType->isComparable());

  rowType = ROW({INTEGER(), mapType});
  EXPECT_FALSE(rowType->isOrderable());
  EXPECT_TRUE(rowType->isComparable());

  // Decimal types.
  auto shortDecimal = DECIMAL(10, 5);
  EXPECT_TRUE(shortDecimal->isOrderable());
  EXPECT_TRUE(shortDecimal->isComparable());

  auto longDecimal = DECIMAL(30, 5);
  EXPECT_TRUE(longDecimal->isOrderable());
  EXPECT_TRUE(longDecimal->isComparable());

  // Function type.
  auto functionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>{BIGINT(), VARCHAR()}, BOOLEAN());
  EXPECT_FALSE(functionType->isOrderable());
  EXPECT_FALSE(functionType->isComparable());

  // Mixed.
  mapType = MAP(INTEGER(), functionType);
  EXPECT_FALSE(mapType->isOrderable());
  EXPECT_FALSE(mapType->isComparable());

  arrayType = ARRAY(mapType);
  EXPECT_FALSE(arrayType->isOrderable());
  EXPECT_FALSE(arrayType->isComparable());

  rowType = ROW({INTEGER(), mapType});
  EXPECT_FALSE(rowType->isOrderable());
  EXPECT_FALSE(rowType->isComparable());
}

TEST(TypeTest, functionTypeEquivalent) {
  auto functionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>{BIGINT(), VARCHAR()}, BOOLEAN());
  auto otherFunctionType =
      std::make_shared<FunctionType>(std::vector<TypePtr>{BIGINT()}, BOOLEAN());
  EXPECT_FALSE(functionType->equivalent(*otherFunctionType));

  otherFunctionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>{BIGINT(), VARCHAR()}, BOOLEAN());
  EXPECT_TRUE(functionType->equivalent(*otherFunctionType));

  functionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>{ARRAY(BIGINT())}, BOOLEAN());
  otherFunctionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>{ARRAY(BIGINT())}, BOOLEAN());
  EXPECT_TRUE(functionType->equivalent(*otherFunctionType));

  functionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>{MAP(BIGINT(), VARCHAR())}, BOOLEAN());
  EXPECT_FALSE(functionType->equivalent(*otherFunctionType));

  otherFunctionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>{MAP(BIGINT(), VARCHAR())}, BOOLEAN());

  EXPECT_TRUE(functionType->equivalent(*otherFunctionType));
}

TEST(TypeTest, providesCustomComparison) {
  // None of the builtin types provide custom comparison.
  EXPECT_FALSE(BOOLEAN()->providesCustomComparison());
  EXPECT_FALSE(TINYINT()->providesCustomComparison());
  EXPECT_FALSE(SMALLINT()->providesCustomComparison());
  EXPECT_FALSE(INTEGER()->providesCustomComparison());
  EXPECT_FALSE(BIGINT()->providesCustomComparison());
  EXPECT_FALSE(HUGEINT()->providesCustomComparison());
  EXPECT_FALSE(REAL()->providesCustomComparison());
  EXPECT_FALSE(DOUBLE()->providesCustomComparison());
  EXPECT_FALSE(VARCHAR()->providesCustomComparison());
  EXPECT_FALSE(VARBINARY()->providesCustomComparison());
  EXPECT_FALSE(TIMESTAMP()->providesCustomComparison());
  EXPECT_FALSE(DATE()->providesCustomComparison());
  EXPECT_FALSE(ARRAY(INTEGER())->providesCustomComparison());
  EXPECT_FALSE(MAP(INTEGER(), INTEGER())->providesCustomComparison());
  EXPECT_FALSE(ROW({INTEGER()})->providesCustomComparison());
  EXPECT_FALSE(DECIMAL(10, 5)->providesCustomComparison());
  EXPECT_FALSE(UNKNOWN()->providesCustomComparison());
  EXPECT_FALSE(INTERVAL_YEAR_MONTH()->providesCustomComparison());
  EXPECT_FALSE(INTERVAL_DAY_TIME()->providesCustomComparison());
  EXPECT_FALSE(FUNCTION({INTEGER()}, INTEGER())->providesCustomComparison());

  // This custom type does provide custom comparison.
  EXPECT_TRUE(
      test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON()->providesCustomComparison());
  EXPECT_EQ(0, test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON()->compare(0, 0));
  EXPECT_EQ(
      8633297058295171728, test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON()->hash(0));

  // BIGINT does not provide custom comparison so calling compare or hash on
  // it should fail.
  EXPECT_THROW(BIGINT()->compare(0, 0), VeloxRuntimeError);
  EXPECT_THROW(BIGINT()->hash(0), VeloxRuntimeError);

  // This type claims it providesCustomComparison but does not implement the
  // compare and hash functions so invoking them should still fail.
  EXPECT_TRUE(test::BIGINT_TYPE_WITH_INVALID_CUSTOM_COMPARISON()
                  ->providesCustomComparison());
  EXPECT_THROW(
      test::BIGINT_TYPE_WITH_INVALID_CUSTOM_COMPARISON()->compare(0, 0),
      VeloxRuntimeError);
  EXPECT_THROW(
      test::BIGINT_TYPE_WITH_INVALID_CUSTOM_COMPARISON()->hash(0),
      VeloxRuntimeError);

  // We do not support variable width custom comparison for variable width
  // types, so attempting to instantiate one should fail.
  EXPECT_THROW(test::VARCHAR_TYPE_WITH_CUSTOM_COMPARISON(), VeloxRuntimeError);
}

TEST(TypeTest, toSummaryString) {
  EXPECT_EQ("BOOLEAN", BOOLEAN()->toSummaryString());
  EXPECT_EQ("BIGINT", BIGINT()->toSummaryString());
  EXPECT_EQ("VARCHAR", VARCHAR()->toSummaryString());
  EXPECT_EQ("OPAQUE", OPAQUE<int>()->toSummaryString());

  const auto arrayType = ARRAY(INTEGER());
  EXPECT_EQ("ARRAY", arrayType->toSummaryString());
  EXPECT_EQ("ARRAY(INTEGER)", arrayType->toSummaryString({.maxChildren = 2}));
  EXPECT_EQ(
      "ARRAY(ARRAY)", ARRAY(arrayType)->toSummaryString({.maxChildren = 2}));

  const auto mapType = MAP(INTEGER(), VARCHAR());
  EXPECT_EQ("MAP", mapType->toSummaryString());
  EXPECT_EQ(
      "MAP(INTEGER, VARCHAR)", mapType->toSummaryString({.maxChildren = 2}));
  EXPECT_EQ(
      "MAP(INTEGER, MAP)",
      MAP(INTEGER(), mapType)->toSummaryString({.maxChildren = 2}));

  const auto rowType = ROW({
      BOOLEAN(),
      INTEGER(),
      VARCHAR(),
      arrayType,
      mapType,
      ROW({INTEGER(), VARCHAR()}),
  });
  EXPECT_EQ("ROW(6)", rowType->toSummaryString());
  EXPECT_EQ(
      "ROW(BOOLEAN, INTEGER, ...4 more)",
      rowType->toSummaryString({.maxChildren = 2}));
  EXPECT_EQ(
      "ROW(BOOLEAN, INTEGER, VARCHAR, ARRAY, MAP, ROW)",
      rowType->toSummaryString({.maxChildren = 10}));
}
