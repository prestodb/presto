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

#include "velox/dwio/common/ColumnSelector.h"

#include <iterator>

#include "folly/Conv.h"
#include "velox/dwio/common/TypeUtils.h"

namespace facebook::velox::dwio::common {

using common::typeutils::CompatChecker;
using velox::RowType;
using velox::Type;
using velox::TypeKind;

namespace {
std::string childName(
    const std::shared_ptr<const velox::Type>& type,
    size_t childIdx,
    const std::string& inherit) {
  switch (type->kind()) {
    case velox::TypeKind::ARRAY: {
      DWIO_ENSURE_EQ(childIdx, 0);
      return folly::to<std::string>(inherit, ".[ITEM]");
    }
    case velox::TypeKind::MAP: {
      DWIO_ENSURE_LT(childIdx, 2);
      return folly::to<std::string>(
          inherit, childIdx == 0 ? ".[KEY]" : ".[VALUE]");
    }
    case velox::TypeKind::ROW: {
      auto name = std::dynamic_pointer_cast<const velox::RowType>(type)->nameOf(
          childIdx);
      return folly::to<std::string>(inherit, ".", name);
    }
    default:
      // for others that has no sub field filter - just use its parent's name
      return inherit;
  }
}
} // namespace

void ColumnSelector::buildNodes(
    const std::shared_ptr<const RowType>& schema,
    const std::shared_ptr<const RowType>& contentSchema) {
  buildNode(
      FilterNode(0, MAX_UINT64, "_ROOT_", "", false),
      nullptr,
      std::dynamic_pointer_cast<const Type>(schema),
      std::dynamic_pointer_cast<const Type>(contentSchema),
      true);
}

FilterTypePtr ColumnSelector::buildNode(
    const FilterNode& node,
    const std::shared_ptr<FilterType>& parent,
    const std::shared_ptr<const Type>& type,
    const std::shared_ptr<const Type>& contentType,
    const bool inContent) {
  DWIO_ENSURE(node.node == nodes_.size(), "current node align with vector");

  // make sure content type is compatible to reading request type
  // when content type is present (it may be absent due to schema mismatch)
  if (contentType != nullptr) {
    CompatChecker::check(*contentType, *type);
  }

  // the process will cover supported schema evolution:
  // appending columns at the end or deleting columns at the end
  auto current =
      std::make_shared<FilterType>(node, parent, inContent, type, contentType);
  nodes_.push_back(current);

  // generate nodes for all its children
  // check if this requested type is in the content to be read or not
  // also, if the asked type shorter than content, it won't be show up in
  // column selector filter tree
  nodes_.reserve(nodes_.size() + type->size());
  if (node.node == 0) {
    auto rowType = type->asRow();
    for (size_t i = 0, size = type->size(); i < size; ++i) {
      bool inData = contentType && i < contentType->size();
      current->addChild(buildNode(
          FilterNode(nodes_.size(), i, rowType.nameOf(i), "", !inData),
          current,
          type->childAt(i),
          inData ? contentType->childAt(i) : nullptr,
          inData));
    }
  } else {
    for (size_t i = 0, size = type->size(); i < size; ++i) {
      bool inData = contentType && i < contentType->size();
      current->addChild(buildNode(
          FilterNode(
              nodes_.size(),
              node.column,
              childName(type, i, node.name),
              "",
              !inData),
          current,
          type->childAt(i),
          inData ? contentType->childAt(i) : nullptr,
          inData));
    }
  }

  return current;
}

// this copy method only update inContent and data type
// based on disk data type
void ColumnSelector::copy(
    common::FilterTypePtr& node,
    const std::shared_ptr<const Type>& diskType,
    const common::FilterTypePtr& origin) {
  auto originIsNull = (origin == nullptr);
  if (!originIsNull) {
    node->setInContent(origin->isInContent());
    node->setSequenceFilter(origin->getSequenceFilter());
  }

  // we're not interested in non-read nodes and nodes that not in content
  if (node->shouldRead() && node->isInContent()) {
    // not found in disk type
    if (diskType == nullptr) {
      node->setInContent(false);
      return;
    }

    // ensure disk type can be converted to request type
    CompatChecker::check(*diskType, *node->getRequestType());

    // update data type during the visit as well as other data fields
    node->setDataType(diskType);
    if (!originIsNull) {
      const common::FilterNode& f = origin->getNode();
      auto& fn = const_cast<common::FilterNode&>(node->getNode());
      fn.expression = f.expression;
      fn.partitionKey = f.partitionKey;
    }

    // visit all children
    for (size_t i = 0; i < node->size(); ++i) {
      copy(
          const_cast<common::FilterTypePtr&>(node->childAt(i)),
          i < diskType->size() ? diskType->childAt(i) : nullptr,
          originIsNull ? nullptr : origin->childAt(i));
    }
  }
}

/**
 * Copy the selector tree and apply file schema to handle schema mismatch
 */
ColumnSelector ColumnSelector::apply(
    const std::shared_ptr<ColumnSelector>& origin,
    const std::shared_ptr<const RowType>& fileSchema) {
  // current instance maybe null
  if (origin == nullptr) {
    return ColumnSelector(fileSchema);
  }

  // if selector has no schema, we just build a new tree with file schema
  // selector.getProjection will carry all logic information including nodes
  auto onlyFilter = !origin->hasSchema();
  ColumnSelector cs(
      onlyFilter ? fileSchema : origin->getSchema(),
      origin->getNodeFilter(),
      true);

  // visit file schema and decide in content call
  copy(cs.nodes_[0], fileSchema, onlyFilter ? nullptr : origin->nodes_[0]);
  return cs;
}

/**
 * Mark the node and all its children recursively to be read.
 *
 * @param node the starting id
 */
void ColumnSelector::setRead(const common::FilterTypePtr& node, bool only) {
  if (!node->valid()) {
    return;
  }

  // and all its child
  node->setRead();
  if (only) {
    return;
  }

  // set all its children to be in read state
  for (size_t i = 0, size = node->size(); i < size; ++i) {
    setRead(node->childAt(i));
  }

  // and all its ancestor should be in read state
  // so a reader is expected to check if reading a top node
  // if no, none of its children should be read
  // if yes, all or part of its children may be read - need further check on
  // node
  for (auto&& p = node->getParent().lock(); p; p = p->getParent().lock()) {
    p->setRead();
  }
}

// TODO (cao) - SelectedTypeBuilder is uncessary to exist, can be killed
std::shared_ptr<const TypeWithId> ColumnSelector::buildSelected() const {
  auto selector = [this](size_t index) { return shouldReadNode(index); };
  return typeutils::SelectedTypeBuilder::build(
      TypeWithId::create(schema_), selector);
}

std::shared_ptr<const RowType> ColumnSelector::buildSelectedReordered() const {
  std::vector<std::string> names;
  std::vector<std::shared_ptr<const Type>> types;
  for (auto& node : filter_) {
    names.push_back(schema_->nameOf(node.column));
    types.push_back(schema_->childAt(node.column));
  }
  return std::make_shared<RowType>(std::move(names), std::move(types));
}

void ColumnSelector::setReadAll() {
  DWIO_ENSURE(shouldReadAll(), "should not called on non-read all case");
  auto root = getNode(0);
  setRead(root);

  // generate all columns in the filter list
  for (size_t i = 0, size = root->size(); i < size; ++i) {
    filter_.push_back(root->childAt(i)->getNode());
  }
}

const ColumnFilter& ColumnSelector::getProjection() const {
  return filter_;
}

void ColumnSelector::setConstants(
    const std::vector<std::string>& keys,
    const std::vector<std::string>& values) {
  DWIO_ENSURE_EQ(values.size(), keys.size(), "keys/values size mismatch");

  // for every node value pair - set the value into the node's expression
  for (size_t i = 0, size = keys.size(); i < size; ++i) {
    auto& key = keys[i];
    auto& value = values[i];

    auto node = findColumn(key);
    DWIO_ENSURE(node->valid(), "constant node not found: ", key);

    // currently we only support limited set of types for constant expression
    auto kind = node->getKind();
    DWIO_ENSURE(
        kind == TypeKind::BOOLEAN || kind == TypeKind::TINYINT ||
            kind == TypeKind::SMALLINT || kind == TypeKind::INTEGER ||
            kind == TypeKind::BIGINT || kind == TypeKind::REAL ||
            kind == TypeKind::DOUBLE || kind == TypeKind::VARCHAR,
        "unsupported constant type: ",
        kind);

    // assign the value to the constant's expression
    const_cast<FilterNode&>(node->getNode()).expression = value;
  }
}

// note that - this list includes root NODE - should we?
std::vector<uint64_t> ColumnSelector::getNodeFilter() const {
  std::vector<uint64_t> nodeIds;
  nodeIds.reserve(nodes_.size());
  for (const auto& node : nodes_) {
    if (node->shouldRead()) {
      nodeIds.emplace_back(node->getId());
    }
  }
  return nodeIds;
}

const FilterTypePtr& ColumnSelector::process(const std::string& column, bool) {
  // map column support column name followed by expression
  // after "#" character which is not allowed as legal column name
  auto pair = extractColumnName(column);
  auto expr = pair.second;
  if (!expr.empty()) {
    // when seeing this type of column, we require it has to exist
    const auto& node = findNode(pair.first);
    if (node->valid()) {
      DWIO_ENSURE_EQ(
          node->getKind(),
          TypeKind::MAP,
          "only support expression for map type currently: ",
          column);

      // set expression for this node
      auto& nodeValue = node->getNode();
      const_cast<common::FilterNode&>(nodeValue).expression = expr;
    }
    return node;
  }

  // pass over special rules handling - find the node and return it
  return findNode(column);
}

std::pair<std::string_view, std::string_view> extractColumnName(
    const std::string_view& name) {
  // right now this is the only supported expression for MAP key filter
  auto pos = name.find('#');
  if (pos != std::string::npos) {
    // special map column handling
    auto colName = name.substr(0, pos);
    auto expr = name.substr(pos + 1);
    return std::make_pair(colName, expr);
  }

  return std::make_pair(name, "");
}

void ColumnSelector::logFilter() const {
  auto numColumns = 0;
  auto numNodes = 0;
  // log current filter_ members
  if (hasSchema()) {
    numColumns = getNode(0)->size();
    numNodes = nodes_.size();
  }

  // log it out
  getLog()->logColumnFilter(filter_, numColumns, numNodes, hasSchema());
}

} // namespace facebook::velox::dwio::common
