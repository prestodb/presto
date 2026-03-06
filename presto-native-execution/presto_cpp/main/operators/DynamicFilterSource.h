/*
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

#include <folly/Synchronized.h>
#include <functional>
#include <map>
#include <string>
#include <unordered_set>
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/type/Variant.h"

namespace facebook::presto::operators {

/// Metadata for a single dynamic filter channel within a join.
struct DynamicFilterChannel {
  std::string filterId;
  velox::column_index_t columnIndex;
  velox::TypePtr type;
};

/// PlanNode that wraps the build-side child of a hash join to collect
/// dynamic filter values as data flows through. This is a pass-through node:
/// outputType() == source->outputType(). The operator extracts distinct values
/// (or min/max ranges) from the specified columns and delivers them to the
/// coordinator via a callback.
class DynamicFilterSourceNode : public velox::core::PlanNode {
 public:
  DynamicFilterSourceNode(
      const velox::core::PlanNodeId& id,
      std::vector<DynamicFilterChannel> channels,
      velox::core::PlanNodePtr source)
      : velox::core::PlanNode(id),
        channels_(std::move(channels)),
        sources_{std::move(source)} {}

  const velox::RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const std::vector<DynamicFilterChannel>& channels() const {
    return channels_;
  }

  std::string_view name() const override {
    return "DynamicFilterSource";
  }

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<DynamicFilterChannel> channels_;
  const std::vector<velox::core::PlanNodePtr> sources_;
};

/// Global per-task callback registry for delivering dynamic filter data from
/// DynamicFilterSourceOperator to PrestoTask.
class DynamicFilterCallbackRegistry {
 public:
  /// Called when a DynamicFilterSource pipeline flushes its collected filters.
  using FlushCallback = std::function<void(
      const std::map<std::string, protocol::TupleDomain<std::string>>&,
      const std::unordered_set<std::string>&)>;

  /// Called at operator creation time to register filter IDs the task will
  /// produce, so the coordinator knows when all pipelines have flushed.
  using RegisterCallback =
      std::function<void(const std::unordered_set<std::string>&)>;

  static DynamicFilterCallbackRegistry& instance();

  void registerCallbacks(
      const std::string& taskId,
      FlushCallback flushCallback,
      RegisterCallback registerCallback);

  /// Registers filter IDs for a task (called at operator creation time).
  void registerFilterIds(
      const std::string& taskId,
      const std::unordered_set<std::string>& filterIds);

  /// Invokes the flush callback for the given task without removing it.
  /// Multiple pipelines (DynamicFilterSourceNodes) within the same task can
  /// each call fire() independently — the callback stays registered until
  /// removeCallback() is called at task deletion time.
  void fire(
      const std::string& taskId,
      std::map<std::string, protocol::TupleDomain<std::string>> filters,
      std::unordered_set<std::string> flushedFilterIds);

  void removeCallback(const std::string& taskId);

 private:
  struct Callbacks {
    FlushCallback flush;
    RegisterCallback registerIds;
  };
  folly::Synchronized<std::map<std::string, Callbacks>> callbacks_;
};

/// Translator that converts DynamicFilterSourceNode to the operator.
class DynamicFilterSourceTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) override;
};

} // namespace facebook::presto::operators
