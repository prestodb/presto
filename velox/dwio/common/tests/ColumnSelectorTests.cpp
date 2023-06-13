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

#include <gtest/gtest.h>
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/type/Type.h"

using namespace facebook::velox::dwio::common;
using facebook::velox::RowType;
using facebook::velox::dwio::type::fbhive::HiveTypeParser;

TEST(ColumnSelectorTests, testBasicFilterTree) {
  const auto schema =
      "struct<"
      "id:bigint"
      "values:array<float>"
      "tags:map<int, string>"
      "notes:struct<f1:int, f2:double, f3:string>"
      "memo:string>";
  auto type =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(schema));

  // test basic filter tree constructor without any filter
  const int32_t expectedNodes = 12;
  std::array<uint64_t, 12> columns{MAX_UINT64, 0, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4};
  std::array<std::string, 12> names{
      "_ROOT_",
      "id",
      "values",
      "values.[ITEM]",
      "tags",
      "tags.[KEY]",
      "tags.[VALUE]",
      "notes",
      "notes.f1",
      "notes.f2",
      "notes.f3",
      "memo"};
  {
    ColumnSelector s1(type);
    EXPECT_TRUE(s1.shouldReadAll());

    for (int32_t i = 0; i < expectedNodes; ++i) {
      auto node = s1.getNode(i)->getNode();
      EXPECT_TRUE(node.valid());
      EXPECT_EQ(node.node, i);
      EXPECT_EQ(node.column, columns[i]);
      EXPECT_EQ(node.name, names[i]);
      EXPECT_TRUE(s1.shouldReadNode(i));
    }

    // all columns should be read
    for (int32_t col = 0; col < 5; ++col) {
      EXPECT_TRUE(s1.shouldReadColumn(col));
    }
  }

  // test filter tree by name
  {
    std::array<bool, 12> nodesReadState{
        true,
        true,
        false,
        false,
        true,
        true,
        true,
        false,
        false,
        false,
        false,
        true};
    std::array<bool, 5> columnsReadState{true, false, true, false, true};

    ColumnSelector s2(type, std::vector<std::string>{"id", "tags", "memo"});
    EXPECT_FALSE(s2.shouldReadAll());

    for (int32_t i = 0; i < nodesReadState.size(); ++i) {
      EXPECT_EQ(s2.shouldReadNode(i), nodesReadState[i]);
    }

    for (int32_t i = 0; i < columnsReadState.size(); ++i) {
      EXPECT_EQ(s2.shouldReadColumn(i), columnsReadState[i]);
    }
  }

  // filter by column idx
  {
    ColumnSelector s3(type, std::vector<uint64_t>{3});
    std::array<bool, 12> nodesReadState{
        true,
        false,
        false,
        false,
        false,
        false,
        false,
        true,
        true,
        true,
        true,
        false};
    std::array<bool, 5> columnsReadState{false, false, false, true, false};
    EXPECT_FALSE(s3.shouldReadAll());

    for (int32_t i = 0; i < nodesReadState.size(); ++i) {
      EXPECT_EQ(s3.shouldReadNode(i), nodesReadState[i]);
    }

    for (int32_t i = 0; i < columnsReadState.size(); ++i) {
      EXPECT_EQ(s3.shouldReadColumn(i), columnsReadState[i]);
    }
  }

  // filter by node id list
  {
    ColumnSelector s4(type, std::vector<uint64_t>{5, 7}, true);
    std::array<bool, 12> nodesReadState{
        true,
        false,
        false,
        false,
        false,
        true,
        false,
        true,
        false,
        false,
        false,
        false};

    std::array<bool, 5> columnsReadState{false, false, false, true, false};
    EXPECT_FALSE(s4.shouldReadAll());

    for (int32_t i = 0; i < nodesReadState.size(); ++i) {
      EXPECT_EQ(s4.shouldReadNode(i), nodesReadState[i]);
    }

    for (int32_t i = 0; i < columnsReadState.size(); ++i) {
      EXPECT_EQ(s4.shouldReadColumn(i), columnsReadState[i]);
    }
  }

  // filter through row read options - reversed
  {
    std::vector<uint64_t> opts1{1, 2, 3};
    std::vector<std::string> opts2{"values", "tags", "notes"};

    std::array<bool, 12> nodesReadState{
        true,
        false,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        false};

    std::array<bool, 5> columnsReadState{false, true, true, true, false};

    ColumnSelector s5(type, opts1);
    ColumnSelector s6(type, opts2);

    EXPECT_FALSE(s5.shouldReadAll());
    EXPECT_FALSE(s6.shouldReadAll());

    for (int32_t i = 0; i < nodesReadState.size(); ++i) {
      EXPECT_EQ(s5.shouldReadNode(i), nodesReadState[i]);
      EXPECT_EQ(s6.shouldReadNode(i), nodesReadState[i]);
    }

    for (int32_t i = 0; i < columnsReadState.size(); ++i) {
      EXPECT_EQ(s5.shouldReadColumn(i), columnsReadState[i]);
      EXPECT_EQ(s6.shouldReadColumn(i), columnsReadState[i]);
    }
  }
}

TEST(ColumnSelectorTests, testGetColumnFilter) {
  const auto schema =
      "struct<"
      "id:bigint"
      "values:array<float>"
      "tags:map<int, string>"
      "notes:struct<f1:int, f2:double, f3:string>"
      "memo:string>";
  auto type =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(schema));
  ColumnSelector cs(type, std::vector<std::string>{"id", "tags", "memo"});
  EXPECT_FALSE(cs.shouldReadAll());

  auto filter = cs.getProjection();
  EXPECT_EQ(filter.size(), 3);

  EXPECT_EQ(filter[0].name, "id");
  EXPECT_EQ(filter[0].column, 0);
  EXPECT_EQ(filter[0].node, 1);

  EXPECT_EQ(filter[1].name, "tags");
  EXPECT_EQ(filter[1].column, 2);
  EXPECT_EQ(filter[1].node, 4);

  EXPECT_EQ(filter[2].name, "memo");
  EXPECT_EQ(filter[2].column, 4);
  EXPECT_EQ(filter[2].node, 11);

  ColumnSelector noFilter(type);
  EXPECT_EQ(noFilter.getProjection().size(), 5);

  ColumnSelector emptyFilter(type, std::vector<std::string>());
  EXPECT_EQ(emptyFilter.getProjection().size(), 5);
}

TEST(ColumnSelectorTests, testSearchInColumnFilter) {
  const auto schema =
      "struct<"
      "id:bigint"
      "values:array<float>"
      "tags:map<int, string>"
      "notes:struct<f1:int, f2:double, f3:string>"
      "memo:string>";
  auto type =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(schema));
  ColumnSelector cs(type, std::vector<std::string>({"values"}));
  EXPECT_FALSE(cs.shouldReadAll());

  auto filter = cs.getProjection();
  EXPECT_EQ(filter.size(), 1);
  EXPECT_EQ(filter[0].name, "values");
  EXPECT_EQ(filter[0].column, 1);
  EXPECT_EQ(filter[0].node, 2);

  EXPECT_TRUE(FilterNode(0).in(filter) == filter.cend());
  // only values column is fitlered so only it matches the filter
  EXPECT_TRUE(FilterNode(1).in(filter) != filter.cend());
  EXPECT_TRUE(FilterNode(2).in(filter) == filter.cend());
  EXPECT_TRUE(FilterNode(3).in(filter) == filter.cend());
  EXPECT_TRUE(FilterNode(4).in(filter) == filter.cend());
}

TEST(ColumnSelectorTests, testSchemaMismatchHandling) {
  const auto strSchema =
      "struct<"
      "id:bigint"
      "values:array<float>"
      "tags:map<int, string>"
      "notes:struct<f1:int, f2:double, f3:string>"
      "memo:string>";
  auto schema = std::dynamic_pointer_cast<const RowType>(
      HiveTypeParser().parse(strSchema));
  {
    // missing last field
    const auto strContentSchema1 =
        "struct<"
        "id:bigint"
        "values:array<float>"
        "tags:map<int, string>"
        "notes:struct<f1:int, f2:double, f3:string>"
        ">";
    auto contentSchema1 = std::dynamic_pointer_cast<const RowType>(
        HiveTypeParser().parse(strContentSchema1));

    ColumnSelector s1(schema, contentSchema1);
    for (int32_t i = 0; i < 11; ++i) {
      auto filterNode = s1.getNode(i);
      EXPECT_TRUE(filterNode->isInContent());

      auto node = filterNode->getNode();
      EXPECT_TRUE(node.valid());
      EXPECT_EQ(node.node, i);
      EXPECT_TRUE(s1.shouldReadNode(i));
    }

    // last column is not in content
    EXPECT_FALSE(s1.getNode(11)->isInContent());
  }
  {
    // has more field at the end
    const auto strContentSchema2 =
        "struct<"
        "id:bigint"
        "values:array<float>"
        "tags:map<int, string>"
        "notes:struct<f1:int, f2:double, f3:string>"
        "memo:string"
        "extra:string>";
    auto contentSchema2 = std::dynamic_pointer_cast<const RowType>(
        HiveTypeParser().parse(strContentSchema2));
    ColumnSelector s2(schema, contentSchema2);
    for (int32_t i = 0; i < 12; ++i) {
      auto filterNode = s2.getNode(i);
      EXPECT_TRUE(filterNode->isInContent());

      auto node = filterNode->getNode();
      EXPECT_TRUE(node.valid());
      EXPECT_EQ(node.node, i);
      EXPECT_TRUE(s2.shouldReadNode(i));
    }

    // last node in content but not in reading schema so not found
    EXPECT_ANY_THROW(s2.getNode(12));
  }
}

TEST(ColumnSelectorTests, testMapKeyFilterSyntax) {
  const auto schema =
      "struct<"
      "id:bigint"
      "values:array<float>"
      "tags:map<int, string>"
      "notes:struct<f1:int, f2:double, f3:string>"
      "memo:string>";

  auto type =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(schema));

  // check a specialized filter is supported
  // real data reading support test please refer flat map column reader testing

  ColumnSelector specialSelector(
      type, std::vector<std::string>{"tags#[1,2,3]", "notes.f2"});

  auto filter = specialSelector.getProjection();
  EXPECT_EQ(filter.size(), 2);

  EXPECT_EQ(filter[0].name, "tags");
  EXPECT_EQ(filter[0].column, 2);
  EXPECT_EQ(filter[0].node, 4);
  // first one is map tags,
  // right now we expect expression to be a simple JSON string
  // in the future - this will become real executable expression
  EXPECT_EQ(filter[0].expression, "[1,2,3]");

  EXPECT_EQ(filter[1].name, "notes");
  EXPECT_EQ(filter[1].column, 3);
  EXPECT_EQ(filter[1].node, 7);

  // support find column searching with annotation
  // the annotation value is not used for column matching
  auto tags = specialSelector.findColumn("tags#[1,2,3]");
  EXPECT_TRUE(tags->valid());
  auto& tagsNode = tags->getNode();
  EXPECT_EQ(tagsNode.name, "tags");
  EXPECT_EQ(tagsNode.column, 2);
  EXPECT_EQ(tagsNode.node, 4);

  auto tags2 = specialSelector.findColumn("tags#[1,2]");
  EXPECT_TRUE(tags2->valid());
}

TEST(ColumnSelectorTests, testPartitionKeysMark) {
  const auto schema = std::dynamic_pointer_cast<const RowType>(
      HiveTypeParser().parse("struct<"
                             "id:bigint"
                             "memo:string"
                             "ds:string"
                             "key:string>"));

  const auto physicalSchema = std::dynamic_pointer_cast<const RowType>(
      HiveTypeParser().parse("struct<"
                             "id:bigint"
                             "memo:string>"));

  // use schema and physical schema to initialize a column selector
  // without filtering
  {
    ColumnSelector cs(schema, physicalSchema);
    auto root = cs.getNode(0);
    EXPECT_EQ(root->size(), 4);

    EXPECT_TRUE(root->childAt(0)->shouldRead());
    EXPECT_TRUE(root->childAt(0)->isInContent());
    auto id = root->childAt(0)->getNode();
    EXPECT_EQ(id.node, 1);
    EXPECT_EQ(id.column, 0);
    EXPECT_EQ(id.name, "id");
    EXPECT_EQ(id.partitionKey, false);

    EXPECT_TRUE(root->childAt(1)->shouldRead());
    EXPECT_TRUE(root->childAt(1)->isInContent());
    auto memo = root->childAt(1)->getNode();
    EXPECT_EQ(memo.node, 2);
    EXPECT_EQ(memo.column, 1);
    EXPECT_EQ(memo.name, "memo");
    EXPECT_EQ(memo.partitionKey, false);

    EXPECT_TRUE(root->childAt(2)->shouldRead());
    EXPECT_FALSE(root->childAt(2)->isInContent());
    auto& ds = root->childAt(2)->getNode();
    EXPECT_EQ(ds.node, 3);
    EXPECT_EQ(ds.column, 2);
    EXPECT_EQ(ds.name, "ds");
    EXPECT_EQ(ds.partitionKey, true);

    EXPECT_TRUE(root->childAt(3)->shouldRead());
    EXPECT_FALSE(root->childAt(3)->isInContent());
    auto& key = root->childAt(3)->getNode();
    EXPECT_EQ(key.node, 4);
    EXPECT_EQ(key.column, 3);
    EXPECT_EQ(key.name, "key");
    EXPECT_EQ(key.partitionKey, true);
  }
  {
    auto cs = std::make_shared<ColumnSelector>(schema, physicalSchema);
    // test set constant expression on constant nodes (partition columns)
    auto root = cs->getNode(0);
    cs->setConstants(
        std::vector<std::string>{"ds", "key"},
        std::vector<std::string>{"2018-09-01", "gold"});
    EXPECT_EQ(root->childAt(2)->getNode().expression, "2018-09-01");
    EXPECT_EQ(root->childAt(3)->getNode().expression, "gold");

    // test apply to real data file disk schema
    const auto schemaMore = std::dynamic_pointer_cast<const RowType>(
        HiveTypeParser().parse("struct<"
                               "id:bigint"
                               "memo:string"
                               "extra:array<float>>"));
    const auto schemaLess = std::dynamic_pointer_cast<const RowType>(
        HiveTypeParser().parse("struct<"
                               "id:bigint>"));

    auto csMore = ColumnSelector::apply(cs, schemaMore);
    LOG(INFO) << "CS filter size: " << cs->getProjection().size();
    LOG(INFO) << "CS More filter size: " << csMore.getProjection().size();
    root = csMore.getNode(0);

    // columns in data file but not in logic schema, ignore them
    EXPECT_EQ(root->size(), 4);
    EXPECT_EQ(root->childAt(0)->getNode().name, "id");
    EXPECT_TRUE(root->childAt(0)->shouldRead());
    EXPECT_TRUE(root->childAt(0)->isInContent());
    EXPECT_EQ(root->childAt(1)->getNode().name, "memo");
    EXPECT_TRUE(root->childAt(1)->shouldRead());
    EXPECT_TRUE(root->childAt(1)->isInContent());

    LOG(INFO) << "DS node: " << root->childAt(2)->getNode().toString();
    EXPECT_EQ(root->childAt(2)->getNode().name, "ds");
    EXPECT_TRUE(root->childAt(2)->shouldRead());
    EXPECT_FALSE(root->childAt(2)->isInContent());

    LOG(INFO) << "Key node: " << root->childAt(3)->getNode().toString();
    EXPECT_EQ(root->childAt(3)->getNode().name, "key");
    EXPECT_TRUE(root->childAt(3)->shouldRead());
    EXPECT_FALSE(root->childAt(3)->isInContent());

    // columns not in data file, the will be filled with null
    auto csLess = ColumnSelector::apply(cs, schemaLess);
    LOG(INFO) << "CS Less filter size: " << csLess.getProjection().size();
    root = csLess.getNode(0);
    EXPECT_EQ(root->size(), 4);
    EXPECT_EQ(root->childAt(0)->getNode().name, "id");
    EXPECT_TRUE(root->childAt(0)->shouldRead());
    EXPECT_TRUE(root->childAt(0)->isInContent());
    EXPECT_EQ(root->childAt(1)->getNode().name, "memo");
    EXPECT_TRUE(root->childAt(1)->shouldRead());
    EXPECT_FALSE(root->childAt(1)->isInContent());
    EXPECT_EQ(root->childAt(2)->getNode().name, "ds");
    EXPECT_TRUE(root->childAt(2)->shouldRead());
    EXPECT_FALSE(root->childAt(2)->isInContent());
    EXPECT_EQ(root->childAt(3)->getNode().name, "key");
    EXPECT_TRUE(root->childAt(3)->shouldRead());
    EXPECT_FALSE(root->childAt(3)->isInContent());
  }
}

TEST(ColumnSelectorTests, testProjectionUnchangedWhenReadSetChanged) {
  const auto schema = std::dynamic_pointer_cast<const RowType>(
      HiveTypeParser().parse("struct<"
                             "id:bigint"
                             "values:array<float>"
                             "tags:map<int, string>"
                             "notes:struct<f1:int, f2:double, f3:string>"
                             "memo:string"
                             "extra:string>"));
  ColumnSelector cs(schema, std::vector<std::string>{"id", "values"});
  cs.setRead(cs.findColumn("notes"));

  auto columnFilter = cs.getProjection();
  EXPECT_EQ(columnFilter.size(), 2);
  EXPECT_EQ(columnFilter.at(0).column, 0);
  EXPECT_EQ(columnFilter.at(0).name, "id");
  EXPECT_EQ(columnFilter.at(1).column, 1);
  EXPECT_EQ(columnFilter.at(1).name, "values");

  std::vector<bool> readStates = {
      true,
      true,
      true,
      true,
      false,
      false,
      false,
      true,
      true,
      true,
      true,
      false,
      false};

  // column read filter
  auto filter = cs.getFilter();
  EXPECT_TRUE(filter(0));
  EXPECT_TRUE(filter(1));
  EXPECT_FALSE(filter(2));
  EXPECT_TRUE(filter(3));
  EXPECT_FALSE(filter(4));
  EXPECT_FALSE(filter(5));

  // node read filter
  for (int32_t i = 0; i < readStates.size(); ++i) {
    EXPECT_EQ(readStates.at(i), cs.shouldReadNode(i));
  }
}

TEST(ColumnSelectorTests, testProjectOrder) {
  const auto schema = std::dynamic_pointer_cast<const RowType>(
      HiveTypeParser().parse("struct<"
                             "id:bigint"
                             "values:array<float>"
                             "tags:map<int, string>"
                             "notes:struct<f1:int, f2:double, f3:string>"
                             "memo:string>"));

  // test filter with names with order of tags, memo and id
  {
    std::vector<std::string> nameFilter{"tags", "memo", "id"};
    ColumnSelector cs(schema, nameFilter);
    auto cf = cs.getProjection();
    EXPECT_EQ(cf.size(), 3);
    EXPECT_EQ(cf.at(0).column, 2);
    EXPECT_EQ(cf.at(0).name, "tags");

    EXPECT_EQ(cf.at(1).column, 4);
    EXPECT_EQ(cf.at(1).name, "memo");

    EXPECT_EQ(cf.at(2).column, 0);
    EXPECT_EQ(cf.at(2).name, "id");
  }

  // test filter with sub field list
  {
    std::vector<std::string> nameFilter{"notes.f2", "id"};
    ColumnSelector cs(schema, nameFilter);

    // column filter only serves column level info
    // we need different API to serve node level data
    auto cf = cs.getProjection();
    EXPECT_EQ(cf.size(), 2);
    EXPECT_EQ(cf.at(0).column, 3);
    EXPECT_EQ(cf.at(0).name, "notes");

    EXPECT_EQ(cf.at(1).column, 0);
    EXPECT_EQ(cf.at(1).name, "id");
  }

  // test filter with something not found - should throw?
  {
    bool failed = false;
    try {
      std::vector<std::string> nameFilter{"memo", "dead", "id"};
      ColumnSelector cs(schema, nameFilter);
    } catch (const std::runtime_error& e) {
      auto msg = folly::to<std::string>(e.what());
      EXPECT_EQ(msg, "Columns not found in hive table: dead");
      failed = true;
    }

    EXPECT_TRUE(failed);
  }

  // test filter with column index
  {
    std::vector<std::uint64_t> indexFilter{3, 1};
    ColumnSelector cs(schema, indexFilter);
    auto cf = cs.getProjection();
    EXPECT_EQ(cf.size(), 2);
    EXPECT_EQ(cf.at(0).column, 3);
    EXPECT_EQ(cf.at(0).name, "notes");

    EXPECT_EQ(cf.at(1).column, 1);
    EXPECT_EQ(cf.at(1).name, "values");
  }

  // test filter with node list
  {
    std::vector<std::uint64_t> indexFilter{4, 1};
    ColumnSelector cs(schema, indexFilter, true);
    auto cf = cs.getProjection();
    EXPECT_EQ(cf.size(), 2);
    EXPECT_EQ(cf.at(0).column, 2);
    EXPECT_EQ(cf.at(0).name, "tags");

    EXPECT_EQ(cf.at(1).column, 0);
    EXPECT_EQ(cf.at(1).name, "id");
  }
}

TEST(ColumnSelectorTests, testNonexistingColFilters) {
  const auto schema = std::dynamic_pointer_cast<const RowType>(
      HiveTypeParser().parse("struct<"
                             "id:bigint"
                             "values:array<float>"
                             "tags:map<int, string>"
                             "notes:struct<f1:int, f2:double, f3:string>"
                             "memo:string"
                             "extra:string>"));

  EXPECT_THROW(
      ColumnSelector cs(
          schema,
          std::vector<std::string>{"id", "values", "notexists#[10,20,30,40]"}),
      std::runtime_error);
}
