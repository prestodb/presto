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

#include "velox/dwio/common/MetadataFilter.h"

#include <folly/container/F14Map.h>
#include "velox/dwio/common/ScanSpec.h"
#include "velox/expression/ExprToSubfieldFilter.h"

namespace facebook::velox::common {

namespace {
using LeafResults =
    folly::F14FastMap<const MetadataFilter::LeafNode*, std::vector<uint64_t>*>;
}

struct MetadataFilter::Node {
  static std::unique_ptr<Node> fromExpression(
      ScanSpec&,
      const core::ITypedExpr&);
  virtual ~Node() = default;
  virtual uint64_t* eval(LeafResults&, int size) const = 0;
};

class MetadataFilter::LeafNode : public Node {
 public:
  LeafNode(ScanSpec& scanSpec, Subfield&& field, std::unique_ptr<Filter> filter)
      : field_(std::move(field)) {
    scanSpec.getOrCreateChild(field_)->addMetadataFilter(
        this, std::move(filter));
  }

  uint64_t* eval(LeafResults& leafResults, int) const override {
    if (auto it = leafResults.find(this); it != leafResults.end()) {
      return it->second->data();
    }
    return nullptr;
  }

  const Subfield& field() const {
    return field_;
  }

 private:
  Subfield field_;
};

struct MetadataFilter::AndNode : Node {
  AndNode(std::unique_ptr<Node> lhs, std::unique_ptr<Node> rhs)
      : lhs_(std::move(lhs)), rhs_(std::move(rhs)) {}

  uint64_t* eval(LeafResults& leafResults, int size) const override {
    auto* l = lhs_->eval(leafResults, size);
    auto* r = rhs_->eval(leafResults, size);
    if (!l) {
      return r;
    }
    if (!r) {
      return l;
    }
    bits::orBits(l, r, 0, size);
    return l;
  }

 private:
  std::unique_ptr<Node> lhs_;
  std::unique_ptr<Node> rhs_;
};

struct MetadataFilter::OrNode : Node {
  OrNode(std::unique_ptr<Node> lhs, std::unique_ptr<Node> rhs)
      : lhs_(std::move(lhs)), rhs_(std::move(rhs)) {}

  uint64_t* eval(LeafResults& leafResults, int size) const override {
    auto* l = lhs_->eval(leafResults, size);
    auto* r = rhs_->eval(leafResults, size);
    if (!l || !r) {
      return nullptr;
    }
    bits::andBits(l, r, 0, size);
    return l;
  }

 private:
  std::unique_ptr<Node> lhs_;
  std::unique_ptr<Node> rhs_;
};

struct MetadataFilter::NotNode : Node {
  explicit NotNode(std::unique_ptr<Node> negated)
      : negated_(std::move(negated)) {}

  uint64_t* eval(LeafResults& leafResults, int size) const override {
    auto* bits = negated_->eval(leafResults, size);
    if (!bits) {
      return nullptr;
    }
    bits::negate(reinterpret_cast<char*>(bits), size);
    return bits;
  }

 private:
  std::unique_ptr<Node> negated_;
};

namespace {

const core::FieldAccessTypedExpr* asField(
    const core::ITypedExpr* expr,
    int index) {
  return dynamic_cast<const core::FieldAccessTypedExpr*>(
      expr->inputs()[index].get());
}

const core::CallTypedExpr* asCall(const core::ITypedExpr* expr) {
  return dynamic_cast<const core::CallTypedExpr*>(expr);
}

} // namespace

std::unique_ptr<MetadataFilter::Node> MetadataFilter::Node::fromExpression(
    ScanSpec& scanSpec,
    const core::ITypedExpr& expr) {
  auto* call = asCall(&expr);
  if (!call) {
    return nullptr;
  }
  if (call->name() == "and") {
    auto lhs = fromExpression(scanSpec, *call->inputs()[0]);
    auto rhs = fromExpression(scanSpec, *call->inputs()[1]);
    if (!lhs) {
      return rhs;
    }
    if (!rhs) {
      return lhs;
    }
    return std::make_unique<AndNode>(std::move(lhs), std::move(rhs));
  }
  if (call->name() == "or") {
    auto lhs = fromExpression(scanSpec, *call->inputs()[0]);
    auto rhs = fromExpression(scanSpec, *call->inputs()[1]);
    if (!lhs || !rhs) {
      return nullptr;
    }
    return std::make_unique<OrNode>(std::move(lhs), std::move(rhs));
  }
  if (call->name() == "not") {
    auto negated = fromExpression(scanSpec, *call->inputs()[0]);
    if (!negated) {
      return nullptr;
    }
    return std::make_unique<NotNode>(std::move(negated));
  }
  try {
    auto res = exec::leafCallToSubfieldFilter(*call);
    return std::make_unique<LeafNode>(
        scanSpec, std::move(res.first), std::move(res.second));
  } catch (const VeloxException& e) {
    VLOG(1) << e.what();
    return nullptr;
  }
}

MetadataFilter::MetadataFilter(ScanSpec& scanSpec, const core::ITypedExpr& expr)
    : root_(Node::fromExpression(scanSpec, expr)) {}

void MetadataFilter::eval(
    std::vector<std::pair<LeafNode*, std::vector<uint64_t>>>& leafNodeResults,
    std::vector<uint64_t>& finalResult) {
  if (!root_) {
    return;
  }
  LeafResults leafResults;
  for (auto& [leaf, result] : leafNodeResults) {
    VELOX_CHECK_EQ(
        result.size(),
        finalResult.size(),
        "Result size mismatch: {}",
        leaf->field().toString());
    VELOX_CHECK(
        leafResults.emplace(leaf, &result).second,
        "Duplicate results: {}",
        leaf->field().toString());
  }
  auto bitCount = finalResult.size() * 64;
  if (auto* combined = root_->eval(leafResults, bitCount)) {
    bits::orBits(finalResult.data(), combined, 0, bitCount);
  }
}

} // namespace facebook::velox::common
