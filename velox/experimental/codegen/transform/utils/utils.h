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

/*
 * This header define utility functions,classes and concept useful for the
 * transformtion framework. In general, it focuses on tree manipulation and
 * traversal.
 */
#pragma once

#if ENABLE_CONCEPTS
#include <concepts> // @manual
#endif
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>
#include "velox/core/PlanNode.h"
#include "velox/experimental/codegen/transform/utils/ranges_utils.h"

namespace facebook {
namespace velox {
namespace transform {

namespace utils {

#if ENABLE_CONCEPTS

/**
 * Basic tree concept
 * The only requirement here is to have an iterable range representing the
 * chidlren nodes
 * TODO: Introduce more refined of the tree concept : mutable tree
 * @tparam T
 */
template <typename T>
concept Tree = requires(const T& tree) {
  // typename T::nodeType;
  { children(tree) } -> std::ranges::range; // might need to be const here
  std::is_same_v<
      std::ranges::range_value_t<std::remove_cvref_t<decltype(children(tree))>>,
      T>;
};

template <typename NodeType, typename NodeTransformer>
concept isTansformer = requires(NodeType& node, NodeTransformer& tr) {
  requires Tree<NodeType>;
  // TODO: relax this constraint to allow more pointer type
  {
    tr(node, std::declval<std::vector<std::shared_ptr<NodeType>>>())
    } -> same_as<std::shared_ptr<NodeType>>;
};
#endif

/**
 * Range (begin/end) representing a topological traversal of a given tree.
 * This implementation is not lazy
 * TODO: Make sure that this class respects the range concept
 * TODO: Implement a lazy version
 * Implementation characteristics:
 * O(n) space
 * Constructor time O(n)
 * @tparam TreeType
 */
template <typename TreeType>
struct topologicalTraversal : public ranges::view_base {
  std::vector<std::reference_wrapper<const TreeType>> nodeList;
  const TreeType& tree;

  explicit topologicalTraversal(const TreeType& tree_) : tree(tree_) {
    // Compute a topological order of the tree nodes.
    nodeList.push_back(tree);
    for (size_t i = 0; i < nodeList.size(); ++i) {
      for (auto const& child : children(nodeList[i].get())) {
        nodeList.push_back(child);
      }
    };
  }

  auto begin() const {
    return nodeList.crbegin();
  }
  auto end() const {
    return nodeList.crend();
  }

  auto rbegin() const {
    return nodeList.cbegin();
  }
  auto rend() const {
    return nodeList.cend();
  }
};

template <typename TreeType, typename Func>
void dfsApply(const TreeType& tree, const Func& lambda) {
  std::vector<std::reference_wrapper<const TreeType>> nodeList;
  nodeList.push_back(tree);
  while (!nodeList.empty()) {
    auto ref = nodeList.back();
    lambda(ref.get());
    nodeList.pop_back();
    for (auto const& child : children(ref.get())) {
      nodeList.push_back(child);
    };
  };
}

/**
 * Perform an isomorphic transformation of a given tree, using a node to node
 * function.
 *
 * @tparam TreeType see Tree concept
 * @tparam NodeTransformer  (TODO: see NodeTransformer concept)
 * @param tree
 * @param transformer function
 * @return A new tree, and mapping between nodes in the old and new tree
 */
template <typename TreeType, typename NodeTransformer>
#if ENABLE_CONCEPTS
requires isTansformer<TreeType, NodeTransformer>
#endif
auto isomorphicTreeTransform(
    const TreeType& tree,
    const NodeTransformer& transformer) {
  // Note: Ideally we would rather use here std::reference_wrapper instead of
  // const TreeType *; However the compare function on ref_wrapper are not
  // appropriate
  std::map<const TreeType*, std::shared_ptr<TreeType>> newNodes;

  auto topoOrder = topologicalTraversal(tree);
  for (const auto& currentNode : topoOrder) {
    // First find the new nodes corresponding the child of the current node
    auto newChildren = children(currentNode.get()) |
        views::transform([&newNodes](const auto& child) {
                         return newNodes[&child];
                       });

    auto newNode = transformer(currentNode.get(), newChildren);
    newNodes[&currentNode.get()] = newNode;
  }
  return std::make_pair(newNodes[&topoOrder.rbegin()->get()], newNodes);
}
} // namespace utils

} // namespace transform
} // namespace velox
}; // namespace facebook

namespace ranges {
template <typename Range>
RANGES_INLINE_VAR constexpr bool enable_borrowed_range<
    facebook::velox::transform::utils::topologicalTraversal<Range>> = true;

};
