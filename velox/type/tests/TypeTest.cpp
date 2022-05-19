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

using namespace facebook;
using namespace facebook::velox;

TEST(TypeTest, array) {
  auto arr0 = ARRAY(ARRAY(ARRAY(INTEGER())));
  EXPECT_EQ("ARRAY<ARRAY<ARRAY<INTEGER>>>", arr0->toString());
  EXPECT_EQ(arr0->size(), 1);
  EXPECT_STREQ(arr0->kindName(), "ARRAY");
  EXPECT_EQ(arr0->isPrimitiveType(), false);
  EXPECT_STREQ(arr0->elementType()->kindName(), "ARRAY");
  EXPECT_EQ(arr0->childAt(0)->toString(), "ARRAY<ARRAY<INTEGER>>");
  EXPECT_THROW(arr0->childAt(1), VeloxUserError);
}

TEST(TypeTest, fixedLenArray) {
  auto arr0 = FIXED_SIZE_ARRAY(3, INTEGER());
  EXPECT_EQ("FIXED_SIZE_ARRAY(3)<INTEGER>", arr0->toString());
  EXPECT_EQ(arr0->size(), 1);
  EXPECT_STREQ(arr0->kindName(), "FIXED_SIZE_ARRAY");
  EXPECT_EQ(arr0->isPrimitiveType(), false);
  EXPECT_STREQ(arr0->elementType()->kindName(), "INTEGER");
  EXPECT_EQ(arr0->childAt(0)->toString(), "INTEGER");
  EXPECT_THROW(arr0->childAt(1), VeloxUserError);
}

TEST(TypeTest, integer) {
  auto int0 = INTEGER();
  EXPECT_EQ(int0->toString(), "INTEGER");
  EXPECT_EQ(int0->size(), 0);
  EXPECT_THROW(int0->childAt(0), std::invalid_argument);
  EXPECT_EQ(int0->kind(), TypeKind::INTEGER);
  EXPECT_STREQ(int0->kindName(), "INTEGER");
  EXPECT_EQ(int0->begin(), int0->end());
}

TEST(TypeTest, timestamp) {
  auto t0 = TIMESTAMP();
  EXPECT_EQ(t0->toString(), "TIMESTAMP");
  EXPECT_EQ(t0->size(), 0);
  EXPECT_THROW(t0->childAt(0), std::invalid_argument);
  EXPECT_EQ(t0->kind(), TypeKind::TIMESTAMP);
  EXPECT_STREQ(t0->kindName(), "TIMESTAMP");
  EXPECT_EQ(t0->begin(), t0->end());
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
  EXPECT_EQ(date->kind(), TypeKind::DATE);
  EXPECT_STREQ(date->kindName(), "DATE");
  EXPECT_EQ(date->begin(), date->end());
}

TEST(TypeTest, shortDecimal) {
  auto shortDecimal = SHORT_DECIMAL(10, 5);
  EXPECT_EQ(shortDecimal->toString(), "SHORT_DECIMAL(10,5)");
  EXPECT_EQ(shortDecimal->size(), 0);
  EXPECT_THROW(shortDecimal->childAt(0), std::invalid_argument);
  EXPECT_EQ(shortDecimal->kind(), TypeKind::SHORT_DECIMAL);
  EXPECT_STREQ(shortDecimal->kindName(), "SHORT_DECIMAL");
  EXPECT_EQ(shortDecimal->begin(), shortDecimal->end());
  EXPECT_EQ(*SHORT_DECIMAL(10, 5), *shortDecimal);
  EXPECT_NE(*SHORT_DECIMAL(9, 5), *shortDecimal);
  EXPECT_NE(*SHORT_DECIMAL(10, 4), *shortDecimal);
  try {
    shortDecimal = SHORT_DECIMAL(19, 5);
    FAIL() << "Function should throw.";
  } catch (const VeloxRuntimeError& e) {
    EXPECT_EQ("(19 vs. 18)", e.message());
  }
}

TEST(TypeTest, longDecimal) {
  auto longDecimal = LONG_DECIMAL(30, 5);
  EXPECT_EQ(longDecimal->toString(), "LONG_DECIMAL(30,5)");
  EXPECT_EQ(longDecimal->size(), 0);
  EXPECT_THROW(longDecimal->childAt(0), std::invalid_argument);
  EXPECT_EQ(longDecimal->kind(), TypeKind::LONG_DECIMAL);
  EXPECT_STREQ(longDecimal->kindName(), "LONG_DECIMAL");
  EXPECT_EQ(longDecimal->begin(), longDecimal->end());
  EXPECT_EQ(*LONG_DECIMAL(30, 5), *longDecimal);
  EXPECT_NE(*LONG_DECIMAL(9, 5), *longDecimal);
  EXPECT_NE(*LONG_DECIMAL(30, 3), *longDecimal);
  try {
    longDecimal = LONG_DECIMAL(39, 5);
    FAIL() << "Function should throw.";
  } catch (const VeloxRuntimeError& e) {
    EXPECT_EQ("(39 vs. 38)", e.message());
  }
}

TEST(TypeTest, dateToString) {
  Date epoch(0);
  EXPECT_EQ(epoch.toString(), "1970-01-01");

  // 50 years after epoch
  Date jan2020(18262);
  EXPECT_EQ(jan2020.toString(), "2020-01-01");

  Date beforeEpoch(-5);
  EXPECT_EQ(beforeEpoch.toString(), "1969-12-27");

  // 50 years before epoch
  Date wayBeforeEpoch(-18262);
  EXPECT_EQ(wayBeforeEpoch.toString(), "1920-01-02");

  // Trying a very large -integer for boundary checks. Such values are tested in
  // ExpressionFuzzer.
  // Since we use int64 for the intermediate conversion of days to ms,
  // the large -ve value remains valid. However, gmtime uses int32
  // for the number of years, so the eventual results might look like garbage.
  // However, they are consistent with presto java so keeping the same
  // implementation.
  Date dateOverflow(-1855961014);
  EXPECT_EQ(dateOverflow.toString(), "-5079479-05-03");
}

TEST(TypeTest, dateComparison) {
  Date epoch(0);
  Date beforeEpoch(-5);
  Date jan2020(18262);
  Date jan2020Copy(18262);
  Date dec2019(18261);

  EXPECT_EQ(jan2020, jan2020Copy);
  EXPECT_EQ(jan2020Copy, jan2020);

  EXPECT_NE(jan2020, dec2019);
  EXPECT_NE(dec2019, jan2020);
  EXPECT_NE(epoch, beforeEpoch);

  EXPECT_LT(dec2019, jan2020);
  EXPECT_LT(beforeEpoch, epoch);

  EXPECT_LE(jan2020, jan2020Copy);
  EXPECT_LE(dec2019, jan2020);
  EXPECT_LE(beforeEpoch, epoch);

  EXPECT_GT(jan2020, dec2019);
  EXPECT_GT(epoch, beforeEpoch);

  EXPECT_GE(jan2020, jan2020Copy);
  EXPECT_GE(jan2020, dec2019);
  EXPECT_GE(epoch, beforeEpoch);
}

TEST(TypeTest, parseStringToDate) {
  auto parseDate = [](const std::string& dateStr) {
    Date returnDate;
    parseTo(dateStr, returnDate);
    return returnDate;
  };

  // Epoch.
  EXPECT_EQ(parseDate("1970-01-01").days(), 0);

  // 50 years after epoch.
  EXPECT_EQ(parseDate("2020-01-01").days(), 18262);

  // Before epoch.
  EXPECT_EQ(parseDate("1969-12-27").days(), -5);

  // 50 years before epoch.
  EXPECT_EQ(parseDate("1920-01-02").days(), -18262);

  // Century before epoch.
  EXPECT_EQ(parseDate("1812-04-15").days(), -57604);

  // Century after epoch.
  EXPECT_EQ(parseDate("2135-11-09").days(), 60577);
}

TEST(TypeTest, map) {
  auto map0 = MAP(INTEGER(), ARRAY(BIGINT()));
  EXPECT_EQ(map0->toString(), "MAP<INTEGER,ARRAY<BIGINT>>");
  EXPECT_EQ(map0->size(), 2);
  EXPECT_EQ(map0->childAt(0)->toString(), "INTEGER");
  EXPECT_EQ(map0->childAt(1)->toString(), "ARRAY<BIGINT>");
  EXPECT_THROW(map0->childAt(2), VeloxUserError);
  EXPECT_EQ(map0->kind(), TypeKind::MAP);
  EXPECT_STREQ(map0->kindName(), "MAP");
  int32_t num = 0;
  for (auto& i : *map0) {
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
}

TEST(TypeTest, row) {
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

  auto row1 =
      ROW({{"a,b", INTEGER()}, {"my \"column\"", ROW({{"#1", BIGINT()}})}});
  EXPECT_EQ(
      row1->toString(),
      "ROW<\"a,b\":INTEGER,\"my \"\"column\"\"\":ROW<\"#1\":BIGINT>>");
  EXPECT_EQ(row1->nameOf(0), "a,b");
  EXPECT_EQ(row1->nameOf(1), "my \"column\"");
  EXPECT_EQ(row1->childAt(1)->toString(), "ROW<\"#1\":BIGINT>");

  auto row2 = ROW({{"", INTEGER()}});
  EXPECT_EQ(row2->toString(), "ROW<\"\":INTEGER>");
  EXPECT_EQ(row2->nameOf(0), "");
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

  // mix
  EXPECT_FALSE(MAP(REAL(), INTEGER())
                   ->
                   operator==(*ROW({{"a", REAL()}, {"b", INTEGER()}})));
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
  EXPECT_EQ(*CppToType<Date>::create(), *DATE());
  EXPECT_EQ(*CppToType<Array<int32_t>>::create(), *ARRAY(INTEGER()));
  auto type = CppToType<Map<int32_t, Map<int64_t, float>>>::create();
  EXPECT_EQ(*type, *MAP(INTEGER(), MAP(BIGINT(), REAL())));
}

TEST(TypeTest, kindHash) {
  EXPECT_EQ(BIGINT()->hashKind(), BIGINT()->hashKind());
  EXPECT_EQ(TIMESTAMP()->hashKind(), TIMESTAMP()->hashKind());
  EXPECT_EQ(DATE()->hashKind(), DATE()->hashKind());
  EXPECT_NE(BIGINT()->hashKind(), INTEGER()->hashKind());
  EXPECT_EQ(
      ROW({{"a", BIGINT()}})->hashKind(), ROW({{"b", BIGINT()}})->hashKind());
  EXPECT_TRUE(ROW({{"a", BIGINT()}})->kindEquals(ROW({{"b", BIGINT()}})));
  EXPECT_FALSE(ROW({{"a", BIGINT()}})->kindEquals(ROW({{"a", INTEGER()}})));
  EXPECT_TRUE(MAP(BIGINT(), BIGINT())->kindEquals(MAP(BIGINT(), BIGINT())));
  EXPECT_FALSE(
      MAP(BIGINT(), BIGINT())->kindEquals(MAP(BIGINT(), ARRAY(BIGINT()))));
  EXPECT_TRUE(ARRAY(BIGINT())->kindEquals(ARRAY(BIGINT())));
  EXPECT_FALSE(ARRAY(BIGINT())->kindEquals(ARRAY(INTEGER())));
  EXPECT_FALSE(ARRAY(BIGINT())->kindEquals(ROW({{"a", BIGINT()}})));

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
  ASSERT_EQ(3, type->size());
  ASSERT_EQ(BIGINT(), type->childAt(0));
  ASSERT_EQ(VARCHAR(), type->childAt(1));
  ASSERT_EQ(BOOLEAN(), type->childAt(2));
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

TEST(TypeTest, unknown) {
  auto unknownArray = ARRAY(UNKNOWN());
  EXPECT_TRUE(unknownArray->containsUnknown());
}

TEST(TypeTest, isVariadicType) {
  EXPECT_TRUE(isVariadicType<Variadic<int64_t>>::value);
  EXPECT_TRUE(isVariadicType<Variadic<Array<float>>>::value);
  EXPECT_FALSE(isVariadicType<velox::StringView>::value);
  EXPECT_FALSE(isVariadicType<bool>::value);
  EXPECT_FALSE((isVariadicType<Map<int8_t, Date>>::value));
}
