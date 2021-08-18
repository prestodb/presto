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
#include <algorithm>
#include <memory>
#include "folly/Conv.h"
#include "velox/experimental/codegen/transform/utils/adapters.h"
#include "velox/experimental/codegen/transform/utils/utils.h"

using facebook::velox::transform::utils::topologicalTraversal;

// Test Tree with labeled node.
template <typename LabelType>
struct TestTree {
  explicit TestTree(const LabelType& label) : label(label) {}
  TestTree(
      const LabelType& label,
      const std::vector<std::shared_ptr<TestTree<LabelType>>>& children)
      : label(label), childrenPtr(children) {}
  template <typename Iterator>
  TestTree(const LabelType& label, Iterator begin, Iterator end)
      : label(label), childrenPtr(begin, end) {}
  LabelType label;
  std::vector<std::shared_ptr<TestTree<LabelType>>> childrenPtr;
  auto children() const {
    return views::transform(
        childrenPtr, [](const auto& node) -> auto& { return *node; });
  }

  static std::pair<
      std::shared_ptr<TestTree<LabelType>>,
      std::vector<std::shared_ptr<TestTree<LabelType>>>>
  randomTree(std::vector<LabelType> labels) {
    assert(!labels.empty());
    std::srand(
        std::time(nullptr)); // use current time as seed for random generator
    std::vector<std::shared_ptr<TestTree<LabelType>>> nodes;
    auto rootNode = std::make_shared<TestTree<LabelType>>(labels.back());
    labels.pop_back();
    nodes.push_back(rootNode);
    while (!labels.empty()) {
      // Randomly select where to insert the new node in the tree
      auto selectedNode = nodes[std::rand() % nodes.size()];
      auto newNode = std::make_shared<TestTree<LabelType>>(labels.back());
      selectedNode->childrenPtr.push_back(newNode);
      nodes.push_back(newNode);
      labels.pop_back();
    };
    return {rootNode, nodes};
  }
};
template <typename LabelType>
auto children(const TestTree<LabelType>& tree) {
  return tree.children();
};
static_assert(
    ranges::viewable_range<topologicalTraversal<TestTree<std::string>>>);

struct TestStruct {
  TestStruct(double a, int b) : i(a), j(b) {}
  double i;
  int j;
};

struct TestStructValueReader {
  auto operator()(const TestStruct& a) {
    return std::forward_as_tuple(a.i, a.j);
  }
};
using copyTestStruct = facebook::velox::transform::utils::adapter::
    NodeCopy<TestStruct, TestStructValueReader>;

TEST(TransformUtilsTestSuite, copyWith) {
  TestStruct testStruct{1, 3};
  auto newTestStruct =
      copyTestStruct::copyWith(testStruct, 5.0, std::placeholders::_1);
  ASSERT_EQ(newTestStruct->i, 5.0);
  ASSERT_EQ(newTestStruct->j, 3);
}

TEST(TransformUtilsTestSuite, topologicalOrder) {
  // build random node
  auto [tree, nodes_] =
      TestTree<std::string>::randomTree({"a", "b", "c", "f", "e"});
  topologicalTraversal orderedNode(*tree);

  // validation
  std::map<const TestTree<std::string>*, bool> visited;
  std::set<const TestTree<std::string>*> nodes;
  std::transform(
      nodes_.begin(),
      nodes_.end(),
      std::inserter(nodes, nodes.begin()),
      [](auto& node) { return &*node; });

  for (const auto& node : orderedNode) {
    // test that the node is actually from the tree, this guard against shadow
    // copies
    ASSERT_TRUE(nodes.find(&node.get()) != nodes.end());
    visited[&node.get()] = true;
    // test that we already visited all the children
    auto children = node.get().children();
    bool childrenVisited = std::all_of(
        children.begin(),
        children.end(),
        [&visited](const TestTree<std::string>& node) {
          return visited[&node];
        });
    ASSERT_EQ(true, childrenVisited);
  }
}

TEST(TransformUtilsTestSuite, topologicalOrder2) {
  TestTree<std::string> bottomLeft{"bottom left", {}};
  TestTree<std::string> bottomRight{"bottom right", {}};
  TestTree<std::string> top{
      "top",
      {std::make_shared<TestTree<std::string>>(bottomLeft),
       std::make_shared<TestTree<std::string>>(bottomRight)}};
  topologicalTraversal orderedNode(top);
  auto dataView = ranges::transform_view(
      orderedNode, [](auto& node) { return node.get().label; });

  std::vector<std::string> results(dataView.begin(), dataView.end());
  std::vector<std::string> expected{"bottom right", "bottom left", "top"};
  ASSERT_EQ(expected, results);
};

TEST(TransformUtilsTestSuite, treeTranform) {
  using facebook::velox::transform::utils::isomorphicTreeTransform;
  TestTree<int> bottomLeft{1, {}};
  TestTree<int> bottomRight{2, {}};
  TestTree<int> tree{
      3,
      {std::make_shared<TestTree<int>>(bottomLeft),
       std::make_shared<TestTree<int>>(bottomRight)}};

  auto doubleTransform = [](const TestTree<int>& node, auto& children_) {
    return std::make_shared<TestTree<int>>(
        2 * node.label, children_.begin(), children_.end());
  };
  auto [newTree, newNodeMap] = isomorphicTreeTransform(tree, doubleTransform);

  topologicalTraversal orderedNodes(tree);
  for (auto& node : orderedNodes) {
    // check that each node is properly transformed
    ASSERT_TRUE(newNodeMap.count(&node.get()) == 1);
    ASSERT_EQ(2 * node.get().label, newNodeMap[&node.get()]->label);

    // check that the tree structure is preserved by the transformation
    auto childrenView = views::transform(
        node.get().children(), [](const auto& node) { return &node; });
    auto newChildrenView = views::transform(
        newNodeMap[&node.get()]->children(),
        [](const auto& node) { return &node; });

    std::vector<const TestTree<int>*> childrenVector(
        childrenView.begin(), childrenView.end());
    std::vector<const TestTree<int>*> newChildrenVector(
        newChildrenView.begin(), newChildrenView.end());
    ASSERT_EQ(childrenVector.size(), newChildrenVector.size());
    for (auto i = 0; i < newChildrenVector.size(); i++) {
      ASSERT_TRUE(newNodeMap.count(childrenVector[i]) == 1);
      ASSERT_EQ(newChildrenVector[i], newNodeMap[childrenVector[i]].get());
    };
  };
};
