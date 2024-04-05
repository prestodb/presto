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
#include <gtest/gtest.h>
#include "velox/common/base/VeloxException.h"
#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/type/fbhive/HiveTypeSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::typeutils;
using facebook::velox::type::fbhive::HiveTypeParser;
using facebook::velox::type::fbhive::HiveTypeSerializer;

void assertEqualTypeWithId(
    std::shared_ptr<const TypeWithId>& actual,
    std::shared_ptr<const TypeWithId>& expected) {
  EXPECT_EQ(actual->size(), expected->size());
  for (auto idx = 0; idx < actual->size(); idx++) {
    auto actualTypeChild = actual->childAt(idx);
    auto expectedTypeChild = expected->childAt(idx);
    EXPECT_TRUE(actualTypeChild->type()->kindEquals(expectedTypeChild->type()));
    EXPECT_EQ(actualTypeChild->id(), expectedTypeChild->id());
    EXPECT_EQ(actualTypeChild->column(), expectedTypeChild->column());
    assertEqualTypeWithId(actualTypeChild, expectedTypeChild);
  }
}

void assertValidTypeWithId(
    const std::shared_ptr<const TypeWithId>& typeWithId) {
  for (auto idx = 0; idx < typeWithId->size(); idx++) {
    EXPECT_EQ(typeWithId->childAt(idx)->parent(), typeWithId.get());
    assertValidTypeWithId(typeWithId->childAt(idx));
  }
}

TEST(TestType, selectedType) {
  auto type = HiveTypeParser().parse(
      "struct<col0:tinyint,col1:smallint,col2:array<string>,"
      "col3:map<float,double>,col4:float,"
      "col5:int,col6:bigint,col7:string>");
  EXPECT_STREQ(
      "struct<col0:tinyint,col1:smallint,col2:array<string>,"
      "col3:map<float,double>,col4:float,"
      "col5:int,col6:bigint,col7:string>",
      HiveTypeSerializer::serialize(type).c_str());
  std::shared_ptr<const TypeWithId> typeWithId = TypeWithId::create(type);
  EXPECT_EQ(0, typeWithId->id());
  EXPECT_EQ(11, typeWithId->maxId());

  auto copySelector = [](size_t index) { return true; };

  // The following two lines verify that the original type tree's children are
  // not re-parented by the buildSelectedType method when copying. If it is
  // re-parented, then this test would crash with SIGSEGV. The return type is
  // deliberately ignored so the copied type will be deallocated upon return.
  buildSelectedType(typeWithId, copySelector);
  EXPECT_EQ(typeWithId->childAt(1)->parent()->type()->kind(), TypeKind::ROW);

  auto cutType = buildSelectedType(typeWithId, copySelector);
  assertEqualTypeWithId(cutType, typeWithId);
  assertValidTypeWithId(typeWithId);
  assertValidTypeWithId(cutType);

  std::vector<bool> selected(12);
  selected[0] = true;
  selected[2] = true;
  auto selector = [&selected](size_t index) { return selected[index]; };
  cutType = buildSelectedType(typeWithId, selector);
  assertValidTypeWithId(typeWithId);
  assertValidTypeWithId(cutType);
  EXPECT_STREQ(
      "struct<col1:smallint>",
      HiveTypeSerializer::serialize(cutType->type()).c_str());
  EXPECT_EQ(0, cutType->id());
  EXPECT_EQ(11, cutType->maxId());
  EXPECT_EQ(2, cutType->childAt(0)->maxId());

  selected.assign(12, true);
  cutType = buildSelectedType(typeWithId, selector);
  assertValidTypeWithId(typeWithId);
  assertValidTypeWithId(cutType);
  EXPECT_STREQ(
      "struct<col0:tinyint,col1:smallint,col2:array<string>,"
      "col3:map<float,double>,col4:float,"
      "col5:int,col6:bigint,col7:string>",
      HiveTypeSerializer::serialize(cutType->type()).c_str());
  EXPECT_EQ(0, cutType->id());
  EXPECT_EQ(11, cutType->maxId());

  selected.assign(12, false);
  selected[0] = true;
  selected[8] = true;
  cutType = buildSelectedType(typeWithId, selector);
  assertValidTypeWithId(typeWithId);
  assertValidTypeWithId(cutType);
  EXPECT_STREQ(
      "struct<col4:float>",
      HiveTypeSerializer::serialize(cutType->type()).c_str());
  EXPECT_EQ(0, cutType->id());
  EXPECT_EQ(11, cutType->maxId());
  EXPECT_EQ(8, cutType->childAt(0)->id());
  EXPECT_EQ(8, cutType->childAt(0)->maxId());

  selected.assign(12, false);
  selected[0] = true;
  selected[3] = true;
  EXPECT_THROW(buildSelectedType(typeWithId, selector), VeloxUserError);

  selected.assign(12, false);
  selected[0] = true;
  EXPECT_THROW(buildSelectedType(typeWithId, selector), VeloxUserError);

  selected.assign(12, false);
  selected[0] = true;
  selected[3] = true;
  selected[4] = true;
  cutType = buildSelectedType(typeWithId, selector);
  assertValidTypeWithId(typeWithId);
  assertValidTypeWithId(cutType);
  EXPECT_STREQ(
      "struct<col2:array<string>>",
      HiveTypeSerializer::serialize(cutType->type()).c_str());

  selected.assign(12, false);
  selected[0] = true;
  selected[3] = true;
  EXPECT_THROW(buildSelectedType(typeWithId, selector), VeloxUserError);

  selected.assign(12, false);
  selected[0] = true;
  selected[5] = true;
  selected[6] = true;
  selected[7] = true;
  cutType = buildSelectedType(typeWithId, selector);
  assertValidTypeWithId(typeWithId);
  assertValidTypeWithId(cutType);
  EXPECT_STREQ(
      "struct<col3:map<float,double>>",
      HiveTypeSerializer::serialize(cutType->type()).c_str());
  EXPECT_EQ(5, cutType->childAt(0)->id());
  EXPECT_EQ(7, cutType->childAt(0)->maxId());

  // TODO : Consider supporting partial selection of some compound types
  selected.assign(12, false);
  selected[0] = true;
  selected[5] = true;
  EXPECT_THROW(buildSelectedType(typeWithId, selector), VeloxUserError);

  selected.assign(12, false);
  selected[0] = true;
  selected[5] = true;
  selected[6] = true;
  EXPECT_THROW(buildSelectedType(typeWithId, selector), VeloxUserError);

  selected.assign(12, false);
  selected[0] = true;
  selected[5] = true;
  selected[7] = true;
  EXPECT_THROW(buildSelectedType(typeWithId, selector), VeloxUserError);

  selected.assign(12, false);
  selected[0] = true;
  selected[1] = true;
  selected[11] = true;
  cutType = buildSelectedType(typeWithId, selector);
  assertValidTypeWithId(typeWithId);
  assertValidTypeWithId(cutType);

  EXPECT_STREQ(
      "struct<col0:tinyint,col7:string>",
      HiveTypeSerializer::serialize(cutType->type()).c_str());
  EXPECT_EQ(1, cutType->childAt(0)->id());
  EXPECT_EQ(1, cutType->childAt(0)->maxId());
  EXPECT_EQ(11, cutType->childAt(1)->id());
  EXPECT_EQ(11, cutType->childAt(1)->maxId());
}

TEST(TestType, buildTypeFromString) {
  std::string typeStr = "struct<a:int,b:string,c:float,d:string>";
  auto type = HiveTypeParser().parse(typeStr);
  EXPECT_STREQ(typeStr.c_str(), HiveTypeSerializer::serialize(type).c_str());

  typeStr = "map<boolean,float>";
  type = HiveTypeParser().parse(typeStr);
  EXPECT_STREQ(typeStr.c_str(), HiveTypeSerializer::serialize(type).c_str());

  typeStr = "struct<a:bigint,b:struct<a:binary,b:timestamp>>";
  type = HiveTypeParser().parse(typeStr);
  EXPECT_STREQ(typeStr.c_str(), HiveTypeSerializer::serialize(type).c_str());

  typeStr =
      "struct<a:bigint,b:struct<a:binary,b:timestamp>,c:map<double,tinyint>>";
  type = HiveTypeParser().parse(typeStr);
  EXPECT_STREQ(typeStr.c_str(), HiveTypeSerializer::serialize(type).c_str());
}

TEST(TestType, typeCompatibility) {
  // have one more file column and one more partition key
  auto from = ROW({INTEGER()});
  auto to = ROW({INTEGER(), REAL(), VARCHAR()});
  checkTypeCompatibility(*from, *to, true);

  // have incompatible type
  to = ROW({REAL()});
  EXPECT_THROW(checkTypeCompatibility(*from, *to, true), VeloxUserError);

  // last column as partition key which is incompatible with last column of
  // the file
  from = ROW({INTEGER(), REAL()});
  to = ROW({INTEGER()});
  checkTypeCompatibility(*from, *to, true);

  // incompatible type but not selected is ok
  from = ROW({INTEGER(), REAL()});
  to = ROW({INTEGER(), VARCHAR()});
  ColumnSelector cs{to, std::vector<uint64_t>{0}};
  checkTypeCompatibility(*from, cs);

  ColumnSelector cs2{to, std::vector<uint64_t>{1}};
  EXPECT_THROW(checkTypeCompatibility(*from, cs2), VeloxUserError);

  from = ROW({ARRAY(INTEGER())});
  to = ROW({ARRAY(DOUBLE())});
  EXPECT_THROW(checkTypeCompatibility(*from, *to, true), VeloxUserError);

  from = ROW({MAP(VARCHAR(), INTEGER())});
  to = ROW({MAP(VARCHAR(), DOUBLE())});
  EXPECT_THROW(checkTypeCompatibility(*from, *to, true), VeloxUserError);
}

TEST(TestType, typeCompatibilityWithErrorMessage) {
  // have one more file column and one more partition key
  auto from = ROW({INTEGER()});
  auto to = ROW({REAL()});
  ColumnSelector cs{to, std::vector<uint64_t>{0}};
  std::string exceptionContext{"test error message"};
  std::function<std::string()> errorMessageCreator = [&]() {
    return exceptionContext;
  };

  std::string expectedMsg = fmt::format(
      "{}, From Kind: {}, To Kind: {}", exceptionContext, "INTEGER", "REAL");
  EXPECT_THROW(
      {
        try {
          checkTypeCompatibility(*from, cs, errorMessageCreator);
        } catch (const facebook::velox::VeloxException& ex) {
          EXPECT_NE(ex.message().find(expectedMsg), std::string::npos);
          EXPECT_EQ(ex.errorCode(), "SCHEMA_MISMATCH");
          EXPECT_TRUE(ex.isUserError());
          throw;
        }
      },
      VeloxUserError);
}

TEST(TestType, typeColumns) {
  auto type = HiveTypeParser().parse(
      "struct<col0:tinyint,col1:smallint,col2:array<string>,"
      "col3:map<float,double>,col4:float,"
      "col5:int,col6:bigint,col7:struct<c1:tinyint,c2:map<int,float>>,col8:string>");
  auto typeWithId = TypeWithId::create(type);
  EXPECT_EQ(0, typeWithId->id());
  EXPECT_EQ(0, typeWithId->column());
  EXPECT_EQ(9, typeWithId->size());

  EXPECT_EQ(0, typeWithId->childAt(0)->column());
  EXPECT_EQ(0, typeWithId->childAt(0)->size());

  EXPECT_EQ(1, typeWithId->childAt(1)->column());
  EXPECT_EQ(0, typeWithId->childAt(1)->size());

  EXPECT_EQ(2, typeWithId->childAt(2)->column());
  EXPECT_EQ(1, typeWithId->childAt(2)->size());
  EXPECT_EQ(2, typeWithId->childAt(2)->childAt(0)->column());

  EXPECT_EQ(3, typeWithId->childAt(3)->column());
  EXPECT_EQ(2, typeWithId->childAt(3)->size());
  EXPECT_EQ(3, typeWithId->childAt(3)->childAt(0)->column());
  EXPECT_EQ(3, typeWithId->childAt(3)->childAt(1)->column());
  EXPECT_EQ(0, typeWithId->childAt(3)->childAt(0)->size());
  EXPECT_EQ(0, typeWithId->childAt(3)->childAt(1)->size());

  EXPECT_EQ(4, typeWithId->childAt(4)->column());
  EXPECT_EQ(0, typeWithId->childAt(4)->size());

  EXPECT_EQ(5, typeWithId->childAt(5)->column());
  EXPECT_EQ(0, typeWithId->childAt(5)->size());

  EXPECT_EQ(6, typeWithId->childAt(6)->column());
  EXPECT_EQ(0, typeWithId->childAt(6)->size());

  // Only root struct types should allocate column ids.
  // Sub struct types should inherit column id and not
  // allocate new ids
  EXPECT_EQ(7, typeWithId->childAt(7)->column());
  EXPECT_EQ(2, typeWithId->childAt(7)->size());
  EXPECT_EQ(7, typeWithId->childAt(7)->childAt(0)->column());
  EXPECT_EQ(7, typeWithId->childAt(7)->childAt(1)->column());
  EXPECT_EQ(0, typeWithId->childAt(7)->childAt(0)->size());
  EXPECT_EQ(2, typeWithId->childAt(7)->childAt(1)->size());
  EXPECT_EQ(7, typeWithId->childAt(7)->childAt(1)->childAt(0)->column());
  EXPECT_EQ(7, typeWithId->childAt(7)->childAt(1)->childAt(1)->column());

  EXPECT_EQ(8, typeWithId->childAt(8)->column());
  EXPECT_EQ(0, typeWithId->childAt(8)->size());

  // Root, non-struct compound objects should not allocate column ids
  // to child types
  type = HiveTypeParser().parse("map<float,double>");
  typeWithId = TypeWithId::create(type);
  EXPECT_EQ(0, typeWithId->id());
  EXPECT_EQ(0, typeWithId->column());
  EXPECT_EQ(2, typeWithId->size());
  EXPECT_EQ(0, typeWithId->childAt(0)->column());
  EXPECT_EQ(0, typeWithId->childAt(1)->column());
  EXPECT_EQ(0, typeWithId->childAt(0)->size());
  EXPECT_EQ(0, typeWithId->childAt(1)->size());
}

TEST(TestType, typeParents) {
  auto type = HiveTypeParser().parse(
      "struct<col0:tinyint,col1:smallint,col2:array<string>,"
      "col3:map<float,double>,col4:float,"
      "col5:int,col6:bigint,col7:struct<c1:tinyint,c2:map<int,float>>,col8:string>");
  auto typeWithId = TypeWithId::create(type);
  EXPECT_EQ(0, typeWithId->id());
  EXPECT_EQ(nullptr, typeWithId->parent());
  EXPECT_EQ(9, typeWithId->size());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(0)->parent());
  EXPECT_EQ(0, typeWithId->childAt(0)->size());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(1)->parent());
  EXPECT_EQ(0, typeWithId->childAt(1)->size());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(2)->parent());
  EXPECT_EQ(1, typeWithId->childAt(2)->size());
  EXPECT_EQ(
      typeWithId->childAt(2).get(),
      typeWithId->childAt(2)->childAt(0)->parent());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(3)->parent());
  EXPECT_EQ(2, typeWithId->childAt(3)->size());
  EXPECT_EQ(
      typeWithId->childAt(3).get(),
      typeWithId->childAt(3)->childAt(0)->parent());
  EXPECT_EQ(
      typeWithId->childAt(3).get(),
      typeWithId->childAt(3)->childAt(1)->parent());
  EXPECT_EQ(0, typeWithId->childAt(3)->childAt(0)->size());
  EXPECT_EQ(0, typeWithId->childAt(3)->childAt(1)->size());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(4)->parent());
  EXPECT_EQ(0, typeWithId->childAt(4)->size());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(5)->parent());
  EXPECT_EQ(0, typeWithId->childAt(5)->size());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(6)->parent());
  EXPECT_EQ(0, typeWithId->childAt(6)->size());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(7)->parent());
  EXPECT_EQ(2, typeWithId->childAt(7)->size());
  EXPECT_EQ(
      typeWithId->childAt(7).get(),
      typeWithId->childAt(7)->childAt(0)->parent());
  EXPECT_EQ(
      typeWithId->childAt(7).get(),
      typeWithId->childAt(7)->childAt(1)->parent());
  EXPECT_EQ(0, typeWithId->childAt(7)->childAt(0)->size());
  EXPECT_EQ(2, typeWithId->childAt(7)->childAt(1)->size());
  EXPECT_EQ(
      typeWithId->childAt(7)->childAt(1).get(),
      typeWithId->childAt(7)->childAt(1)->childAt(0)->parent());
  EXPECT_EQ(
      typeWithId->childAt(7)->childAt(1).get(),
      typeWithId->childAt(7)->childAt(1)->childAt(1)->parent());

  EXPECT_EQ(typeWithId.get(), typeWithId->childAt(8)->parent());
  EXPECT_EQ(0, typeWithId->childAt(8)->size());
}
