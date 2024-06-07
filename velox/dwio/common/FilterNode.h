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

#pragma once

#include <vector>

#include <fmt/format.h>

#include "velox/dwio/common/exception/Exception.h"
#include "velox/type/Type.h"

namespace facebook::velox::dwio::common {

constexpr uint64_t MAX_UINT64 = std::numeric_limits<uint64_t>::max();
using SeqFilter = std::shared_ptr<const std::unordered_set<size_t>>;

/**
 * Defines a filter node in a type tree.
 * The filter node can be used individually such as vector
 * Or it can be used in a node tree which is good for traverse
 */
struct FilterNode {
  static const FilterNode& getInvalid() {
    static const FilterNode kInvalid(MAX_UINT64);
    return kInvalid;
  }

  FilterNode(
      const uint64_t n,
      const uint64_t c,
      std::string nm,
      std::string expr,
      const bool pk)
      : node{n},
        column{c},
        name{std::move(nm)},
        expression{std::move(expr)},
        partitionKey{pk} {}

  // right now - we only filter at top-level column
  /* implicit */ FilterNode(const uint64_t column)
      : FilterNode(MAX_UINT64, column, "", "", false) {}

  // unique node ID in the physical schema tree
  // we want to prefer this because we may want to support sub-field filtering
  // eg. struct: "column_struct.field2", this can be modeled as expression too
  const uint64_t node;

  // column ordinal index in physical schema
  const uint64_t column;

  // column name (or field name)
  const std::string name;

  // align with BBIO to carry extra information.
  // maybe changed to transformation expression that is executable
  // right now - it's a customized literals
  std::string expression;

  // a value indicating current node is partition column
  // it won't have children and at this time expression captures its value
  bool partitionKey;

  bool operator==(const FilterNode& other) const {
    return node == other.node && column == other.column;
  }

  // match by available information - may not equals
  bool match(const FilterNode& other) const {
    // no match if any is invalid
    if (!valid()) {
      // even current is invlaid
      return false;
    }

    // search with available information
    // 1. match by node id
    if (other.node != MAX_UINT64) {
      return node == other.node;
    }

    // 2. match by column id
    if (other.column != MAX_UINT64) {
      return column == other.column;
    }

    // 3. match by column name
    return name == other.name;
  }

  bool match(const std::string_view& name_to_match_2) const {
    // no match if any is invalid
    if (!valid()) {
      // even current is invlaid
      return false;
    }

    return name == name_to_match_2;
  }

  // expect the incoming list has all valid nodes
  // so that current node can have partial info to match any of list item
  std::vector<FilterNode>::const_iterator in(
      const std::vector<FilterNode>& list) const {
    return std::find_if(
        list.cbegin(), list.cend(), [this](const FilterNode& filter_node_2) {
          return filter_node_2.match(*this);
        });
  }

  std::size_t hash() const {
    return std::hash<uint64_t>()(node) ^ std::hash<uint64_t>()(column);
  }

  bool valid() const {
    return node != MAX_UINT64;
  }

  std::string toString() const {
    return fmt::format(
        "[node={},column={},name={},expression={},pkey={}]",
        node,
        column,
        name,
        expression,
        partitionKey);
  }
};

// Define two aliases to capture column filter types
using ColumnFilter = std::vector<velox::dwio::common::FilterNode>;

/**
 * This class is exact physical schema of data (file)
 * with filter node information associated with it.
 *
 * TODO (cao) Ideally this should merge with TypeWithId, but due to
 * it's too large footprint of change, we keep this separate and seek merging
 * in the future.
 */
class FilterType;
using FilterTypePtr = std::shared_ptr<FilterType>;
class FilterType {
 private:
  const FilterNode node_;
  const std::weak_ptr<FilterType> parent_;
  std::vector<FilterTypePtr> children_;
  // a flat to decide if current node is needed
  bool read_;
  // a flag to indicate if current node is in content
  bool inContent_;
  // request type in the filter tree node
  std::shared_ptr<const velox::Type> requestType_;
  // data type in the filter tree node
  std::shared_ptr<const velox::Type> fileType_;
  // sequence filter for given node - empty if no filter
  SeqFilter seqFilter_;

 public:
  // a single value indicating not found (invalid node)
  static const FilterTypePtr& getInvalid() {
    static const FilterTypePtr kInvalid = std::make_shared<FilterType>(
        FilterNode::getInvalid(), nullptr, true, nullptr, nullptr);
    return kInvalid;
  }

  FilterType(
      const FilterNode& node,
      const std::shared_ptr<FilterType>& parent,
      std::vector<FilterTypePtr> children,
      const bool inContent,
      velox::TypePtr type,
      velox::TypePtr contentType)
      : node_{node},
        parent_{parent},
        children_{std::move(children)},
        read_{node.node == 0},
        inContent_{inContent},
        requestType_{std::move(type)},
        fileType_{std::move(contentType)},
        seqFilter_{std::make_shared<std::unordered_set<size_t>>()} {}

  FilterType(
      const FilterNode& node,
      const std::shared_ptr<FilterType>& parent,
      const bool inContent,
      const std::shared_ptr<const velox::Type>& type,
      const std::shared_ptr<const velox::Type>& contentType)
      : FilterType(node, parent, {}, inContent, type, contentType) {}

  size_t size() const {
    return children_.size();
  }

  const FilterTypePtr& childAt(size_t idx) const {
    return children_[idx];
  }

  const FilterNode& getNode() const {
    return node_;
  }

 public:
  inline void setRead() {
    read_ = true;
  }

  inline bool shouldRead() const {
    return read_;
  }

  inline bool isInContent() const {
    return inContent_;
  }

  inline void setInContent(bool inContent) {
    inContent_ = inContent;
  }

  inline const std::shared_ptr<const velox::Type>& getRequestType() const {
    return requestType_;
  }

  inline const std::shared_ptr<const velox::Type>& getDataType() const {
    return fileType_;
  }

  inline void setDataType(const std::shared_ptr<const velox::Type>& fileType) {
    fileType_ = fileType;
  }

  inline bool valid() const {
    return node_.valid();
  }

  inline const std::weak_ptr<FilterType>& getParent() const {
    return parent_;
  }

  inline velox::TypeKind getKind() const {
    // always use request type when asking fro type kind
    return requestType_->kind();
  }

  // return node ID in the type tree
  inline uint64_t getId() const {
    // Cannot get ID for invalid node
    DWIO_ENSURE_EQ(valid(), true);
    return node_.node;
  }

  inline bool isRoot() const {
    return node_.node == 0;
  }

  inline void addChild(FilterTypePtr child) {
    children_.push_back(std::move(child));
  }

  inline void setSequenceFilter(const SeqFilter& seqFilter) {
    // sequence filter is shared by whole sub-tree
    seqFilter_ = seqFilter;
    for (auto& node : children_) {
      node->setSequenceFilter(seqFilter);
    }
  }

  inline const SeqFilter& getSequenceFilter() {
    return seqFilter_;
  }

  inline bool hasSequence(size_t sequence) const {
    return seqFilter_->empty() ||
        seqFilter_->find(sequence) != seqFilter_->end();
  }
};

} // namespace facebook::velox::dwio::common
