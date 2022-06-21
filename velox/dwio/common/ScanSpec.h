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

#include "velox/common/base/SelectivityInfo.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/LazyVector.h"

#include <vector>

namespace facebook {
namespace velox {
namespace dwio::common {
class ColumnStatistics;
}
namespace common {

// Describes the filtering and value extraction for a
// SelectiveColumnReader. This is owned by the TableScan Operator and
// is passed to SelectiveColumnReaders at construction.  This is
// mutable by readers to reflect filter order and other adaptation.
class ScanSpec {
 public:
  static constexpr column_index_t kNoChannel = ~0;

  explicit ScanSpec(const Subfield::PathElement& element) {
    if (element.kind() == kNestedField) {
      auto field = reinterpret_cast<const Subfield::NestedField*>(&element);
      fieldName_ = field->name();

    } else {
      VELOX_CHECK(false, "Only nested fields are supported");
    }
  }

  explicit ScanSpec(const std::string& name) : fieldName_(name) {}

  // Filter to apply. If 'this' corresponds to a struct/list/map, this
  // can only be isNull or isNotNull, other filtering is given by
  // 'children'.
  common::Filter* filter() const {
    return filter_.get();
  }

  // Sets 'filter_'. May be used at initialization or when adding a
  // pushed down filter, e.g. top k cutoff.
  void setFilter(std::unique_ptr<Filter> filter) {
    filter_ = std::move(filter);
  }

  // Returns a constant vector if 'this' corresponds to a partitioning
  // column or to a missing column. These change from split to split.
  VectorPtr constantValue() const {
    return constantValue_;
  }

  void setConstantValue(VectorPtr value) {
    constantValue_ = value;
  }

  bool isConstant() const {
    return constantValue_ != nullptr;
  }

  // Name of the value in its container, i.e. field name in struct or
  // string key in map. Not all fields of 'this' apply in list/map
  // value cases but the overhead is manageable, the space taken is
  // less than the Subfield path that will in any case exist for each
  // separately named list/map element.
  const std::string& fieldName() const {
    return fieldName_;
  }

  // Subscript if this refers to a member of a list or an
  // integer-keyed map value. If this is a member in a row, this is
  // the ordinal position in the row type.  Subscript is mutable, for
  // example the position of the reader in a struct's readers may vary
  // between splits. Set to correspond to the position of 'fieldName'
  // when first reading a struct. Not mutable if this refers to a
  // list/map subscript.
  int64_t subscript() const {
    return subscript_;
  }

  void setSubscript(int64_t subscript) {
    subscript_ = subscript;
  }

  // True if the value is returned from scan. Fields can have
  // 'extractValues_' set and not be projected out if these are only
  // used in filter functions. A runtime pushdown of a filter function
  // may cause this to become false at run time.
  bool projectOut() const {
    return projectOut_;
  }

  void setProjectOut(bool projectOut) {
    projectOut_ = projectOut;
  }

  // Whether the value is extracted, to be collected with
  // getValues(). If this corresponds to a container, e.g. struct,
  // list, map of which at least one value is extracted, this is true.
  // A runtime pushdown may make this false, e.g. if a hash probe
  // changes into an IN predicate. This is true while 'projectOut_' is
  // false for columns that are used in filter functions.
  bool extractValues() const {
    return extractValues_;
  }

  void setExtractValues(bool extractValues) {
    extractValues_ = extractValues;
  }

  bool keepValues() const {
    return extractValues_ || projectOut_;
  }

  // Position in the RowVector returned by the top level scan. Applies
  // only to children of the root struct where projectOut_ is true.
  column_index_t channel() const {
    VELOX_CHECK(channel_ != kNoChannel);
    return channel_;
  }

  void setChannel(column_index_t channel) {
    channel_ = channel;
  }

  const std::vector<std::unique_ptr<ScanSpec>>& children() const {
    return children_;
  }

  // Returns a read sequence number. This can b used for tagging
  // lazy vectors with a generation number so that we can check that
  // the reader that made them has not advanced between the making and
  // the loading of the lazy vector. This must be called if 'this'
  // corresponds to a struct or flat map reader with pushdown. This
  // may periodically do adaptation such as filter reordering. This
  // will initialize the read order on first call and calling this at
  // each level of struct is mandatory.
  uint64_t newRead();

  // Returns the ScanSpec corresponding to 'subfield'. Creates it if
  // needed, including any intermediate levels. This is used at
  // TableScan initialization to create the ScanSpec tree that
  // corresponds to the ColumnReader tree.
  ScanSpec* getOrCreateChild(const Subfield& subfield);

  bool matches(const Subfield::PathElement& element) const {
    auto kind = element.kind();
    switch (kind) {
      case kNestedField:
        return fieldName_ ==
            reinterpret_cast<const Subfield::NestedField*>(&element)->name();
      case kLongSubscript:
        return subscript_ ==
            reinterpret_cast<const Subfield::LongSubscript*>(&element)->index();
      case kStringSubscript:
        return fieldName_ ==
            reinterpret_cast<const Subfield::StringSubscript*>(&element)
                ->index();
      default:
        VELOX_CHECK(
            false, "Only subfields that specify a single field are  supported");
    }
    return false;
  }

  ScanSpec* childByName(const std::string& name) const {
    for (auto& spec : children_) {
      if (spec->fieldName_ == name) {
        return spec.get();
      }
    }
    return nullptr;
  }

  SelectivityInfo& selectivity() {
    return selectivity_;
  }

  ValueHook* valueHook() const {
    return valueHook_;
  }

  void setValueHook(ValueHook* valueHook) {
    valueHook_ = valueHook;
  }

  // Returns true if the corresponding reader only needs to reference
  // the nulls stream. True if filter is is-null with or without value
  // extraction or if filter is is-not-null and no value is extracted.
  bool readsNullsOnly() const {
    if (filter_) {
      if (filter_->kind() == FilterKind::kIsNull) {
        return true;
      }
      if (filter_->kind() == FilterKind::kIsNotNull && !projectOut_ &&
          !extractValues_) {
        return true;
      }
    }
    return false;
  }

  bool makeFlat() const {
    return makeFlat_;
  }

  void setMakeFlat(bool makeFlat) {
    makeFlat_ = makeFlat;
  }

  // True if this or a descendant has a filter. This may change as a
  // result of runtime adaptation.
  bool hasFilter() const;

  // Resets cached values after this or children were updated, e.g. a new filter
  // was added or existing filter was modified.
  void resetCachedValues() {
    hasFilter_.reset();
    for (auto& child : children_) {
      child->resetCachedValues();
    }

    reorder();
  }

  void setEnableFilterReorder(bool enableFilterReorder) {
    enableFilterReorder_ = enableFilterReorder;
  }

  // Returns the child which produces values for 'channel'. Throws if not found.
  ScanSpec& getChildByChannel(column_index_t channel);

  std::string toString() const;

 private:
  void reorder();

  // Number of times read is called on the corresponding reader. This
  // is used for setup on first use and to produce a read sequence
  // number for LazyVectors.
  uint64_t numReads_ = 0;

  // Ordinal position of 'this' in its containing spec. For a struct
  // member this is the position of the reader in the child
  // readers. If this describes an operation on an array element or a
  // map with numeric key, this is the subscript as defined for array
  // or map.
  int64_t subscript_ = -1;
  // Column name if this is a struct mamber. String key if this
  // describes an operation on a map value.
  std::string fieldName_;
  // Ordinal position of the extracted value in the containing
  // RowVector. Set only when this describes a struct member.
  column_index_t channel_ = kNoChannel;

  VectorPtr constantValue_;
  bool projectOut_ = false;
  bool extractValues_ = false;
  // True if a string dictionary or flat map in this field should be
  // returned as flat.
  bool makeFlat_ = false;
  std::unique_ptr<common::Filter> filter_;
  SelectivityInfo selectivity_;
  // Sort children by filtering efficiency.
  bool enableFilterReorder_ = true;

  // Specification of action on child fields. This is filled in as
  // follows: Top level ScanSpec: All top level fields mentioned are
  // specified.  Nested struct/map/list: If filter-only,
  // projectOut/extractvalues are false in both container and children
  // and filtered subfields are represented.  If all children are
  // extracted and some are filtered: The container has
  // projectOut/extractValues set and filtered children, if any, are
  // in 'children_'. The filtered children have extractValues
  // false. If only some children are materialized (subfield pruning),
  // then the materialized children and filtered children are
  // represented in 'children_' and the materialized ones have
  // extractValues true.  Having at least one child with extractValues
  // true differentiates pruning from the case of extracting all children.

  std::vector<std::unique_ptr<ScanSpec>> children_;
  mutable std::optional<bool> hasFilter_;
  ValueHook* valueHook_ = nullptr;
};

// Returns false if no value from a range defined by stats can pass the
// filter. True, otherwise.
bool testFilter(
    common::Filter* filter,
    dwio::common::ColumnStatistics* stats,
    uint64_t totalRows,
    const TypePtr& type);

} // namespace common
} // namespace velox
} // namespace facebook
