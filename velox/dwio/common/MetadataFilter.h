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

#include "velox/core/ExpressionEvaluator.h"
#include "velox/core/ITypedExpr.h"

namespace facebook::velox::common {

class ScanSpec;

/// Represent a logical combination of filters that can be used in row group
/// skipping.  Filters are put at leaf nodes and internal nodes represents
/// logical conjunctions between them.
class MetadataFilter {
 public:
  class LeafNode;

  /// Construct from a ScanSpec and an expression.  The leaf filter nodes
  /// generated will be added to the corresponding position in ScanSpec.
  MetadataFilter(
      ScanSpec&,
      const core::ITypedExpr&,
      core::ExpressionEvaluator*);

  /// Evaluate the filter results based on logical conjunctions tracked in this
  /// object.  `leafNodeResults` could be reused for intermediate results.  The
  /// existing bitmask in `finalResult` will be ANDed with the result we get
  /// from evaluation and stored back.
  void eval(
      std::vector<std::pair<const LeafNode*, std::vector<uint64_t>>>&
          leafNodeResults,
      std::vector<uint64_t>& finalResult);

  std::string toString() const;

 private:
  struct Node;
  struct AndNode;
  struct OrNode;

  std::shared_ptr<Node> root_;
};

} // namespace facebook::velox::common
