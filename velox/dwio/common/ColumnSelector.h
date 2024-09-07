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

#include "velox/dwio/common/FilterNode.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/TypeWithId.h"

namespace facebook::velox::dwio::common {

/**
 * A quick answer to tell special cases - READ ALL
 */
enum class ReadState { kPartial, kAll };

// A utility function to extract column name and expression
// from a augmented column names in current spec.
std::pair<std::string_view, std::string_view> extractColumnName(
    const std::string_view& name);

class ColumnSelector {
 public:
  /**
   * This is a special case we need to support:
   * Use case that doesn't have schema before reading such as file reader tool
   * For this case, it may have filter only and we need to rebuild selector
   * in the run time when a file content schema is determined
   */
  explicit ColumnSelector(
      const ColumnFilter& filter,
      MetricsLogPtr log = nullptr)
      : log_{std::move(log)},
        schema_{nullptr},
        state_{ReadState::kPartial},
        nodes_{},
        filter_{filter.begin(), filter.end()} {
    logFilter();
  }

  /**
   * Constructor with a pre-built schema.
   * @param contents schema of the file
   */
  explicit ColumnSelector(
      const std::shared_ptr<const velox::RowType>& schema,
      const MetricsLogPtr& log = nullptr,
      bool fileColumnNamesReadAsLowerCase = false)
      : ColumnSelector(schema, schema, log, fileColumnNamesReadAsLowerCase) {}

  explicit ColumnSelector(
      const std::shared_ptr<const velox::RowType>& schema,
      const std::shared_ptr<const velox::RowType>& contentSchema,
      MetricsLogPtr log = nullptr,
      bool fileColumnNamesReadAsLowerCase = false)
      : log_{std::move(log)}, schema_{schema}, state_{ReadState::kAll} {
    buildNodes(schema, contentSchema);

    // no filter, read everything
    setReadAll();
    checkSelectColNonDuplicate(fileColumnNamesReadAsLowerCase);
  }

  /**
   * Create column selector out of column name list
   */
  explicit ColumnSelector(
      const std::shared_ptr<const velox::RowType>& schema,
      const std::vector<std::string>& names,
      const MetricsLogPtr& log = nullptr,
      bool fileColumnNamesReadAsLowerCase = false)
      : ColumnSelector(
            schema,
            schema,
            names,
            log,
            fileColumnNamesReadAsLowerCase) {}

  explicit ColumnSelector(
      const std::shared_ptr<const velox::RowType>& schema,
      const std::shared_ptr<const velox::RowType>& contentSchema,
      const std::vector<std::string>& names,
      MetricsLogPtr log = nullptr,
      bool fileColumnNamesReadAsLowerCase = false)
      : log_{std::move(log)},
        schema_{schema},
        state_{names.empty() ? ReadState::kAll : ReadState::kPartial} {
    acceptFilter(schema, contentSchema, names, false);
    checkSelectColNonDuplicate(fileColumnNamesReadAsLowerCase);
  }

  /**
   * Create column selector out of id list as filter
   */
  explicit ColumnSelector(
      const std::shared_ptr<const velox::RowType>& schema,
      const std::vector<uint64_t>& ids,
      const bool filterByNodes = false,
      const MetricsLogPtr& log = nullptr,
      bool fileColumnNamesReadAsLowerCase = false)
      : ColumnSelector(
            schema,
            schema,
            ids,
            filterByNodes,
            log,
            fileColumnNamesReadAsLowerCase) {}

  explicit ColumnSelector(
      const std::shared_ptr<const velox::RowType>& schema,
      const std::shared_ptr<const velox::RowType>& contentSchema,
      const std::vector<uint64_t>& ids,
      const bool filterByNodes = false,
      MetricsLogPtr log = nullptr,
      bool fileColumnNamesReadAsLowerCase = false)
      : log_{std::move(log)},
        schema_{schema},
        state_{ids.empty() ? ReadState::kAll : ReadState::kPartial} {
    acceptFilter(schema, contentSchema, ids, filterByNodes);
    checkSelectColNonDuplicate(fileColumnNamesReadAsLowerCase);
  }

  /// Sets a specific node to read state
  /// only means we only enable exact the node only.
  void setRead(const FilterTypePtr& node, bool only = false);

  /**
   * Get id with pre-order traversal index
   *
   * @param node ID
   * @return the id in the tree
   */
  const FilterTypePtr& getNode(size_t id) const {
    VELOX_CHECK(
        inRange(id), "node: {} is out of range of {}", id, nodes_.size());
    return nodes_[id];
  }

  /**
   * Get request type and data type into
   * TODO (cao) - eventually these two mappings can be replaced by expression
   * for tranformation/projection
   */
  const std::shared_ptr<const velox::Type>& getRequestType(
      const uint64_t node) const {
    return getNode(node)->getRequestType();
  }

  const std::shared_ptr<const velox::Type>& getDataType(
      const uint64_t node) const {
    return getNode(node)->getDataType();
  }

  bool shouldReadStream(size_t node, uint32_t sequence = 0) const {
    if (UNLIKELY(!inRange(node))) {
      return false;
    }

    const auto& filterNode = getNode(node);
    return filterNode->shouldRead() && filterNode->hasSequence(sequence);
  }

  /**
   * Check if we should read node with node ID == index
   */
  bool shouldReadNode(size_t index) const {
    return inRange(index) && getNode(index)->shouldRead();
  }

  /**
   * Return if a top level id should be read or not.
   * A column should be read if any id in under this column needs read
   *
   * @param column index of top level column starting at 0
   * @return true for read and false for skip
   */
  bool shouldReadColumn(size_t column) const {
    return findColumn(column)->shouldRead();
  }

  /**
   * Check if read every node
   */
  bool shouldReadAll() const {
    return state_ == ReadState::kAll;
  }

  /**
   * Find a column id by column index
   *
   * @param columnIndex column index
   * @return the filter node pointer, if not found return invalid instance
   */
  const FilterTypePtr& findColumn(const size_t columnIndex) const {
    FilterTypePtr root = nodes_[0];
    if (columnIndex < root->size()) {
      return root->childAt(columnIndex);
    }

    // NOT FOUND
    return FilterType::getInvalid();
  }

  /**
   * Find a column id by column name
   *
   * @param columnExpression column name - can be augmented with filter
   * @return the filter node pointer, if not found return invalid instance
   */
  const FilterTypePtr& findColumn(const std::string& columnExpression) const {
    // extract method will give us striped version of column name
    auto columnName = extractColumnName(columnExpression).first;
    const auto& root = nodes_[0];

    // only search top level columns for now
    // this can be extended to support any node search
    for (size_t i = 0, size = root->size(); i < size; ++i) {
      // matching by name
      auto& child = root->childAt(i);
      if (child->getNode().name == columnName) {
        return child;
      }
    }

    // not found
    return FilterType::getInvalid();
  }

  const FilterTypePtr& findNode(const std::string_view& name) {
    for (auto& item : nodes_) {
      // operator== of filter node shall equal strict
      // instead search by available information
      if (item->getNode().match(name)) {
        return item;
      }
    }

    // not found
    return FilterType::getInvalid();
  }

  /// Builds selected schema based on current filter.
  std::shared_ptr<const TypeWithId> buildSelected() const;

  /// Builds selected schema based on current filter and reorder columns
  /// according to what filter specifies.
  std::shared_ptr<const velox::RowType> buildSelectedReordered() const;

  /// Build a column filter out of filter tree.
  /// This only returns top columns today and can be extended to node level
  const ColumnFilter& getProjection() const;

  /// A filter lambda function accept column index for query.
  std::function<bool(uint64_t)> getFilter() const {
    return [this](uint64_t column) { return shouldReadColumn(column); };
  }

  /// This is essentially the effective schema when column selector was built.
  bool hasSchema() const {
    return schema_ != nullptr;
  }

  const std::shared_ptr<const velox::RowType>& getSchema() const {
    return schema_;
  }

  const std::shared_ptr<const TypeWithId>& getSchemaWithId() const {
    if (!schemaWithId_) {
      schemaWithId_ = TypeWithId::create(schema_);
    }
    return schemaWithId_;
  }

  void setConstants(
      const std::vector<std::string>& keys,
      const std::vector<std::string>& values);

  /// Creates a file selector based on a logic selector and disk schema.
  static ColumnSelector apply(
      const std::shared_ptr<ColumnSelector>& origin,
      const std::shared_ptr<const velox::RowType>& fileSchema);

  static std::shared_ptr<ColumnSelector> fromScanSpec(
      const velox::common::ScanSpec& spec,
      const RowTypePtr& rowType);

 private:
  // visit the tree with disk type
  static void copy(
      FilterTypePtr&,
      const std::shared_ptr<const velox::Type>& diskType,
      const FilterTypePtr& origin);

  // build filter tree through schema
  void buildNodes(
      const std::shared_ptr<const velox::RowType>& schema,
      const std::shared_ptr<const velox::RowType>& contentSchema);

  FilterTypePtr buildNode(
      const FilterNode& node,
      const std::shared_ptr<FilterType>& parent,
      const std::shared_ptr<const velox::Type>& type,
      const std::shared_ptr<const velox::Type>& contentType,
      bool inContent);

  bool inRange(size_t index) const {
    return index < nodes_.size();
  }

  // generate column filter
  void setReadAll();

  // get node ID list to be read
  std::vector<uint64_t> getNodeFilter() const;

  void checkSelectColNonDuplicate(bool fileColumnNamesReadAsLowerCase) {
    if (!fileColumnNamesReadAsLowerCase) {
      return;
    }
    std::unordered_map<std::string, int> names;
    for (auto node : nodes_) {
      auto& name = node->getNode().name;
      names[name]++;
    }
    for (auto filter : filter_) {
      if (names[filter.name] > 1) {
        VELOX_USER_FAIL(
            "Found duplicate field(s) {} in read lowercase mode", filter.name);
      }
    }
  }

  // accept filter
  template <typename T>
  void acceptFilter(
      const std::shared_ptr<const velox::RowType>& schema,
      const std::shared_ptr<const velox::RowType>& contentSchema,
      const std::vector<T>& filter,
      bool byNode = false) {
    buildNodes(schema, contentSchema);

    // no filter
    if (filter.empty()) {
      setReadAll();
      return;
    }

    DWIO_ENSURE(!shouldReadAll(), "should not read all with filters");
    std::vector<T> notFound;
    for (const auto& col : filter) {
      auto node = process(col, byNode);
      if (!node->valid()) {
        notFound.push_back(col);
        continue;
      }

      // set read on this node
      setRead(node, byNode);

      // TODO (cao) - should we have project order on node level
      // since current column filter is only top column level.
      if (!node->isRoot()) {
        FilterTypePtr& cursor = node;
        for (auto&& p = cursor->getParent().lock(); p && !p->isRoot();
             p = cursor->getParent().lock()) {
          cursor = p;
        }

        filter_.push_back(cursor->getNode());
      }
    }

    // We're changing this to rumtime_error due to client (caffe2)
    // expect a runtime_error rather than fault.
    // Do-Not change the message as expected by client in failure case
    if (!notFound.empty()) {
      throw std::runtime_error(folly::to<std::string>(
          "Columns not found in hive table: ", folly::join(", ", notFound)));
    }
  }

  // process raw key and get node
  const FilterTypePtr& process(uint64_t key, bool byNode) const {
    return byNode ? getNode(key) : findColumn(key);
  }

  // process special filtering expression and get node
  const FilterTypePtr& process(const std::string& column, bool);

  void logFilter() const;

  MetricsLogPtr getLog() const {
    return log_ == nullptr ? MetricsLog::voidLog() : log_;
  }

  const MetricsLogPtr log_;
  // a copy of the schema how column selector is built
  const std::shared_ptr<const velox::RowType> schema_;

  // lazily populated
  mutable std::shared_ptr<const TypeWithId> schemaWithId_;

  const ReadState state_;
  // store flattened nodes of data tree; each item index == its node ID
  std::vector<FilterTypePtr> nodes_;

  // note that - this filter list is not node level
  // it captures top level column projection for now
  ColumnFilter filter_;
};

} // namespace facebook::velox::dwio::common
