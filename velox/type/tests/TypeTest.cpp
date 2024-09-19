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
  auto arrayType = ARRAY(ARRAY(ARRAY(INTEGER())));
  EXPECT_EQ("ARRAY<ARRAY<ARRAY<INTEGER>>>", arrayType->toString());
  EXPECT_EQ(arrayType->size(), 1);
  EXPECT_STREQ(arrayType->kindName(), "ARRAY");
  EXPECT_EQ(arrayType->isPrimitiveType(), false);
  EXPECT_STREQ(arrayType->elementType()->kindName(), "ARRAY");
  EXPECT_EQ(arrayType->childAt(0)->toString(), "ARRAY<ARRAY<INTEGER>>");
  EXPECT_THROW(arrayType->childAt(1), VeloxUserError);

  EXPECT_STREQ(arrayType->name(), "ARRAY");
  EXPECT_EQ(arrayType->parameters().size(), 1);
  EXPECT_TRUE(arrayType->parameters()[0].kind == TypeParameterKind::kType);
  EXPECT_EQ(*arrayType->parameters()[0].type, *arrayType->childAt(0));

  EXPECT_EQ(
      *arrayType, *getType("ARRAY", {TypeParameter(ARRAY(ARRAY(INTEGER())))}));

  testTypeSerde(arrayType);
}

TEST(TypeTest, integer) {
  auto int0 = INTEGER();
  EXPECT_EQ(int0->toString(), "INTEGER");
  EXPECT_EQ(int0->size(), 0);
  EXPECT_THROW(int0->childAt(0), std::invalid_argument);
  EXPECT_EQ(int0->kind(), TypeKind::INTEGER);
  EXPECT_STREQ(int0->kindName(), "INTEGER");
  EXPECT_EQ(int0->begin(), int0->end());

  testTypeSerde(int0);
}

TEST(TypeTest, hugeint) {
  EXPECT_EQ(getType("HUGEINT", {}), HUGEINT());
}

TEST(TypeTest, timestamp) {
  auto t0 = TIMESTAMP();
  EXPECT_EQ(t0->toString(), "TIMESTAMP");
  EXPECT_EQ(t0->size(), 0);
  EXPECT_THROW(t0->childAt(0), std::invalid_argument);
  EXPECT_EQ(t0->kind(), TypeKind::TIMESTAMP);
  EXPECT_STREQ(t0->kindName(), "TIMESTAMP");
  EXPECT_EQ(t0->begin(), t0->end());

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
  auto date = DATE();
  EXPECT_EQ(date->toString(), "DATE");
  EXPECT_EQ(date->size(), 0);
  EXPECT_THROW(date->childAt(0), std::invalid_argument);
  EXPECT_EQ(date->kind(), TypeKind::INTEGER);
  EXPECT_STREQ(date->kindName(), "INTEGER");
  EXPECT_EQ(date->begin(), date->end());

  EXPECT_TRUE(date->kindEquals(INTEGER()));
  EXPECT_NE(*date, *INTEGER());
  EXPECT_FALSE(date->equivalent(*INTEGER()));
  EXPECT_FALSE(INTEGER()->equivalent(*date));

  testTypeSerde(date);
}

TEST(TypeTest, intervalDayTime) {
  auto interval = INTERVAL_DAY_TIME();
  EXPECT_EQ(interval->toString(), "INTERVAL DAY TO SECOND");
  EXPECT_EQ(interval->size(), 0);
  EXPECT_THROW(interval->childAt(0), std::invalid_argument);
  EXPECT_EQ(interval->kind(), TypeKind::BIGINT);
  EXPECT_STREQ(interval->kindName(), "BIGINT");
  EXPECT_EQ(interval->begin(), interval->end());

  EXPECT_TRUE(interval->kindEquals(BIGINT()));
  EXPECT_NE(*interval, *BIGINT());
  EXPECT_FALSE(interval->equivalent(*BIGINT()));
  EXPECT_FALSE(BIGINT()->equivalent(*interval));

  int64_t millis = kMillisInDay * 5 + kMillisInHour * 4 + kMillisInMinute * 6 +
      kMillisInSecond * 7 + 98;
  EXPECT_EQ("5 04:06:07.098", INTERVAL_DAY_TIME()->valueToString(millis));

  testTypeSerde(interval);
}

TEST(TypeTest, intervalYearMonth) {
  auto interval = INTERVAL_YEAR_MONTH();
  EXPECT_EQ(interval->toString(), "INTERVAL YEAR TO MONTH");
  EXPECT_EQ(interval->size(), 0);
  EXPECT_THROW(interval->childAt(0), std::invalid_argument);
  EXPECT_EQ(interval->kind(), TypeKind::INTEGER);
  EXPECT_STREQ(interval->kindName(), "INTEGER");
  EXPECT_EQ(interval->begin(), interval->end());

  EXPECT_TRUE(interval->kindEquals(INTEGER()));
  EXPECT_NE(*interval, *INTEGER());
  EXPECT_FALSE(interval->equivalent(*INTEGER()));
  EXPECT_FALSE(INTEGER()->equivalent(*interval));

  int32_t month = kMonthInYear * 2 + 1;
  EXPECT_EQ("2-1", INTERVAL_YEAR_MONTH()->valueToString(month));

  month = kMonthInYear * -2 + -1;
  EXPECT_EQ("-2-1", INTERVAL_YEAR_MONTH()->valueToString(month));

  EXPECT_EQ(
      "-178956970-8",
      INTERVAL_YEAR_MONTH()->valueToString(
          std::numeric_limits<int32_t>::min()));

  testTypeSerde(interval);
}

TEST(TypeTest, unknown) {
  auto type = UNKNOWN();
  EXPECT_EQ(type->toString(), "UNKNOWN");
  EXPECT_EQ(type->size(), 0);
  EXPECT_THROW(type->childAt(0), std::invalid_argument);
  EXPECT_EQ(type->kind(), TypeKind::UNKNOWN);
  EXPECT_STREQ(type->kindName(), "UNKNOWN");
  EXPECT_EQ(type->begin(), type->end());
  EXPECT_TRUE(type->isComparable());
  EXPECT_TRUE(type->isOrderable());

  testTypeSerde(type);
}

TEST(TypeTest, shortDecimal) {
  auto shortDecimal = DECIMAL(10, 5);
  EXPECT_EQ(shortDecimal->toString(), "DECIMAL(10, 5)");
  EXPECT_EQ(shortDecimal->size(), 0);
  EXPECT_THROW(shortDecimal->childAt(0), std::invalid_argument);
  EXPECT_EQ(shortDecimal->kind(), TypeKind::BIGINT);
  EXPECT_EQ(shortDecimal->begin(), shortDecimal->end());

  EXPECT_EQ(*DECIMAL(10, 5), *shortDecimal);
  EXPECT_NE(*DECIMAL(9, 5), *shortDecimal);
  EXPECT_NE(*DECIMAL(10, 4), *shortDecimal);

  VELOX_ASSERT_THROW(
      DECIMAL(0, 0), "Precision of decimal type must be at least 1");

  EXPECT_STREQ(shortDecimal->name(), "DECIMAL");
  EXPECT_EQ(shortDecimal->parameters().size(), 2);
  EXPECT_TRUE(
      shortDecimal->parameters()[0].kind == TypeParameterKind::kLongLiteral);
  EXPECT_EQ(shortDecimal->parameters()[0].longLiteral.value(), 10);
  EXPECT_TRUE(
      shortDecimal->parameters()[1].kind == TypeParameterKind::kLongLiteral);
  EXPECT_EQ(shortDecimal->parameters()[1].longLiteral.value(), 5);

  EXPECT_EQ(
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
  auto longDecimal = DECIMAL(30, 5);
  EXPECT_EQ(longDecimal->toString(), "DECIMAL(30, 5)");
  EXPECT_EQ(longDecimal->size(), 0);
  EXPECT_THROW(longDecimal->childAt(0), std::invalid_argument);
  EXPECT_EQ(longDecimal->kind(), TypeKind::HUGEINT);
  EXPECT_EQ(longDecimal->begin(), longDecimal->end());
  EXPECT_EQ(*DECIMAL(30, 5), *longDecimal);
  EXPECT_NE(*DECIMAL(9, 5), *longDecimal);
  EXPECT_NE(*DECIMAL(30, 3), *longDecimal);
  VELOX_ASSERT_THROW(
      DECIMAL(39, 5), "Precision of decimal type must not exceed 38");
  VELOX_ASSERT_THROW(
      DECIMAL(25, 26), "Scale of decimal type must not exceed its precision");

  EXPECT_STREQ(longDecimal->name(), "DECIMAL");
  EXPECT_EQ(longDecimal->parameters().size(), 2);
  EXPECT_TRUE(
      longDecimal->parameters()[0].kind == TypeParameterKind::kLongLiteral);
  EXPECT_EQ(longDecimal->parameters()[0].longLiteral.value(), 30);
  EXPECT_TRUE(
      longDecimal->parameters()[1].kind == TypeParameterKind::kLongLiteral);
  EXPECT_EQ(longDecimal->parameters()[1].longLiteral.value(), 5);

  EXPECT_EQ(
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
  auto mapType = MAP(INTEGER(), ARRAY(BIGINT()));
  EXPECT_EQ(mapType->toString(), "MAP<INTEGER,ARRAY<BIGINT>>");
  EXPECT_EQ(mapType->size(), 2);
  EXPECT_EQ(mapType->childAt(0)->toString(), "INTEGER");
  EXPECT_EQ(mapType->childAt(1)->toString(), "ARRAY<BIGINT>");
  EXPECT_THROW(mapType->childAt(2), VeloxUserError);
  EXPECT_EQ(mapType->kind(), TypeKind::MAP);
  EXPECT_STREQ(mapType->kindName(), "MAP");
  int32_t num = 0;
  for (auto& i : *mapType) {
    if (num == 0) {
      EXPECT_EQ(i->toString(), "INTEGER");
    } else if (num == 1) {
      EXPECT_EQ(i->toString(), "ARRAY<BIGINT>");
    } else {
      FAIL();
    }
    ++num;
  }
  CHECK_EQ(num, 2);

  EXPECT_STREQ(mapType->name(), "MAP");
  EXPECT_EQ(mapType->parameters().size(), 2);
  for (auto i = 0; i < 2; ++i) {
    EXPECT_TRUE(mapType->parameters()[i].kind == TypeParameterKind::kType);
    EXPECT_EQ(*mapType->parameters()[i].type, *mapType->childAt(i));
  }

  EXPECT_EQ(
      *mapType,
      *getType(
          "MAP",
          {
              TypeParameter(INTEGER()),
              TypeParameter(ARRAY(BIGINT())),
          }));

  testTypeSerde(mapType);
}

TEST(TypeTest, row) {
  VELOX_ASSERT_THROW(ROW({{"a", nullptr}}), "Child types cannot be null");
  auto row0 = ROW({{"a", INTEGER()}, {"b", ROW({{"a", BIGINT()}})}});
  auto rowInner = row0->childAt(1);
  EXPECT_EQ(row0->toString(), "ROW<a:INTEGER,b:ROW<a:BIGINT>>");
  EXPECT_EQ(row0->size(), 2);
  EXPECT_EQ(rowInner->size(), 1);
  EXPECT_STREQ(row0->childAt(0)->kindName(), "INTEGER");
  EXPECT_STREQ(row0->findChild("a")->kindName(), "INTEGER");
  EXPECT_EQ(row0->nameOf(0), "a");
  EXPECT_EQ(row0->nameOf(1), "b");
  EXPECT_THROW(row0->nameOf(4), std::out_of_range);
  EXPECT_THROW(row0->findChild("not_exist"), VeloxUserError);
  // todo: expected case behavior?:
  VELOX_ASSERT_THROW(
      row0->findChild("A"), "Field not found: A. Available fields are: a, b.");
  EXPECT_EQ(row0->childAt(1)->toString(), "ROW<a:BIGINT>");
  EXPECT_EQ(row0->findChild("b")->toString(), "ROW<a:BIGINT>");
  EXPECT_EQ(row0->findChild("b")->asRow().findChild("a")->toString(), "BIGINT");
  EXPECT_TRUE(row0->containsChild("a"));
  EXPECT_TRUE(row0->containsChild("b"));
  EXPECT_FALSE(row0->containsChild("c"));
  int32_t seen = 0;
  for (auto& i : *row0) {
    if (seen == 0) {
      EXPECT_STREQ("INTEGER", i->kindName());
    } else if (seen == 1) {
      EXPECT_EQ("ROW<a:BIGINT>", i->toString());
      int32_t seen2 = 0;
      for (auto& j : *i) {
        EXPECT_EQ(j->toString(), "BIGINT");
        seen2++;
      }
      EXPECT_EQ(seen2, 1);
    }
    seen++;
  }
  CHECK_EQ(seen, 2);

  EXPECT_STREQ(row0->name(), "ROW");
  EXPECT_EQ(row0->parameters().size(), 2);
  for (auto i = 0; i < 2; ++i) {
    EXPECT_TRUE(row0->parameters()[i].kind == TypeParameterKind::kType);
    EXPECT_EQ(*row0->parameters()[i].type, *row0->childAt(i));
  }

  auto row1 =
      ROW({{"a,b", INTEGER()}, {R"(my "column")", ROW({{"#1", BIGINT()}})}});
  EXPECT_EQ(
      row1->toString(),
      R"(ROW<"a,b":INTEGER,"my ""column""":ROW<"#1":BIGINT>>)");
  EXPECT_EQ(row1->nameOf(0), "a,b");
  EXPECT_EQ(row1->nameOf(1), R"(my "column")");
  EXPECT_EQ(row1->childAt(1)->toString(), R"(ROW<"#1":BIGINT>)");

  auto row2 = ROW({{"", INTEGER()}});
  EXPECT_EQ(row2->toString(), R"(ROW<"":INTEGER>)");
  EXPECT_EQ(row2->nameOf(0), "");

  VELOX_ASSERT_THROW(createScalarType(TypeKind::ROW), "not a scalar type");
  VELOX_ASSERT_THROW(
      createType(TypeKind::ROW, {}), "Not supported for kind: ROW");

  testTypeSerde(row0);
  testTypeSerde(row1);
  testTypeSerde(row2);
  testTypeSerde(rowInner);
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

class Foo {};
class Bar {};

TEST(TypeTest, opaque) {
  auto foo = OpaqueType::create<Foo>();
  auto bar = OpaqueType::create<Bar>();
  // Names currently use typeid which is not stable across platforms. We'd need
  // to change it later if we start serializing opaque types, e.g. we can ask
  // user to "register" the name for the type explicitly.
  EXPECT_NE(std::string::npos, foo->toString().find("OPAQUE<"));
  EXPECT_NE(std::string::npos, foo->toString().find("Foo"));
  EXPECT_EQ(foo->size(), 0);
  EXPECT_THROW(foo->childAt(0), std::invalid_argument);
  EXPECT_STREQ(foo->kindName(), "OPAQUE");
  EXPECT_EQ(foo->isPrimitiveType(), false);

  auto foo2 = OpaqueType::create<Foo>();
  EXPECT_NE(*foo, *bar);
  EXPECT_EQ(*foo, *foo2);

  OpaqueType::registerSerialization<Foo>("id_of_foo");
  EXPECT_EQ(foo->serialize()["opaque"], "id_of_foo");
  EXPECT_THROW(foo->getSerializeFunc(), VeloxException);
  EXPECT_THROW(foo->getDeserializeFunc(), VeloxException);
  EXPECT_THROW(bar->serialize(), VeloxException);
  EXPECT_THROW(bar->getSerializeFunc(), VeloxException);
  EXPECT_THROW(bar->getDeserializeFunc(), VeloxException);

  auto foo3 = Type::create(foo->serialize());
  EXPECT_EQ(*foo, *foo3);

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
  auto unknownArray = ARRAY(UNKNOWN());
  EXPECT_TRUE(unknownArray->containsUnknown());

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

  // BIGINT does not provide custom comparison so calling compare or hash on it
  // should fail.
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
