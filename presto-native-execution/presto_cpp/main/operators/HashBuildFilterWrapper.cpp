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
#include "presto_cpp/main/operators/HashBuildFilterWrapper.h"
#include "presto_cpp/main/operators/HashBuildFilterExtractor.h"
#include "presto_cpp/main/types/TupleDomainBuilder.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/Task.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto::operators {

namespace {

/// Per-task registry of join node IDs that have dynamic filters.
/// Populated during plan conversion, read during operator creation.
folly::Synchronized<
    std::unordered_map<
        std::string, // taskId
        std::unordered_map<
            std::string, // joinNodeId (planNodeId)
            std::vector<DynamicFilterChannel>>>>
    perTaskDppJoinNodes_;

/// Maximum estimated JSON bytes before collapsing to range.
static constexpr uint64_t kMaxDiscreteFilterJsonBytes = 10 << 20; // 10 MB
static constexpr uint64_t kJsonBytesPerDiscreteValue = 120;

/// Per-driver callback invoked from HashBuild::noMoreInput. Extracts
/// a dynamic filter from THIS driver's VectorHasher state and delivers
/// it to the coordinator via the DynamicFilterCallbackRegistry.
void onHashBuildNoMoreInput(
    const HashBuild& hashBuild,
    const std::string& taskId,
    const std::vector<DynamicFilterChannel>& channels,
    memory::MemoryPool* pool) {
  LOG(INFO) << "onHashBuildNoMoreInput: taskId=" << taskId
            << " channels=" << channels.size();
  std::map<std::string, protocol::TupleDomain<std::string>> filters;
  std::unordered_set<std::string> filterIds;

  for (const auto& channel : channels) {
    filterIds.insert(channel.filterId);
  }

  for (const auto& channel : channels) {
    auto result = hashBuild.getFilterForChannel(channel.columnIndex);
    if (!result.has_value()) {
      continue;
    }

    std::vector<variant> discreteValues;
    std::optional<variant> minValue = result->rangeMin;
    std::optional<variant> maxValue = result->rangeMax;
    bool hasFilterableHasher = false;

    if (result->filter) {
      if (result->filter->kind() == common::FilterKind::kAlwaysFalse) {
        hasFilterableHasher = true;
      } else {
        hasFilterableHasher = true;
        convertFilter(
            *result->filter,
            channel.type,
            discreteValues,
            minValue,
            maxValue);

        // Check estimated response size.
        uint64_t estimatedBytes =
            discreteValues.size() * kJsonBytesPerDiscreteValue;
        if (estimatedBytes > kMaxDiscreteFilterJsonBytes) {
          discreteValues.clear();
          discreteValues.shrink_to_fit();
        }
      }
    } else if (result->distinctOverflow) {
      if (result->rangeMin.has_value() && result->rangeMax.has_value()) {
        hasFilterableHasher = true;
      }
    }

    if (discreteValues.empty() && !minValue.has_value() &&
        !maxValue.has_value()) {
      if (hasFilterableHasher) {
        filters[channel.filterId] = buildNoneTupleDomain();
      }
      continue;
    }

    filters[channel.filterId] = buildTupleDomain(
        channel.filterId,
        channel.type,
        discreteValues,
        minValue,
        maxValue,
        false, // nullAllowed
        pool);
  }

  DynamicFilterCallbackRegistry::instance().fire(
      taskId, std::move(filters), std::move(filterIds));
}

/// PlanNodeTranslator that intercepts HashJoinNode and returns an
/// OperatorSupplier which creates HashBuild with a per-driver
/// noMoreInput callback for DPP filter extraction.
class HashBuildFilterWrapperTranslator : public Operator::PlanNodeTranslator {
 public:
  OperatorSupplier toOperatorSupplier(
      const core::PlanNodePtr& node) override {
    auto joinNode =
        std::dynamic_pointer_cast<const core::HashJoinNode>(node);
    if (!joinNode) {
      return nullptr;
    }

    auto joinNodeId = joinNode->id();

    return [joinNode, joinNodeId](
               int32_t operatorId,
               DriverCtx* ctx) -> std::unique_ptr<Operator> {
      // Always create the real HashBuild (same as Velox's built-in).
      if (ctx->task->hasMixedExecutionGroupJoin(joinNode.get()) &&
          needRightSideJoin(joinNode->joinType())) {
        VELOX_UNSUPPORTED(
            "Hash join currently does not support mixed grouped execution "
            "for join type {}",
            core::JoinTypeName::toName(joinNode->joinType()));
      }
      auto hashBuild =
          std::make_unique<HashBuild>(operatorId, ctx, joinNode);

      // Check if this join has DPP filters for this task. If so,
      // set the per-driver callback.
      const auto taskId = ctx->task->taskId();
      std::vector<DynamicFilterChannel> channels;
      {
        auto locked = perTaskDppJoinNodes_.rlock();
        auto taskIt = locked->find(taskId);
        if (taskIt != locked->end()) {
          auto joinIt = taskIt->second.find(joinNodeId);
          if (joinIt != taskIt->second.end()) {
            channels = joinIt->second;
          }
        }
      }

      LOG(INFO) << "HashBuildFilterWrapper: taskId=" << taskId
                << " joinNodeId=" << joinNodeId
                << " channels=" << channels.size()
                << " driverId=" << ctx->driverId;
      if (!channels.empty()) {
        auto leafPool = ctx->task->pool()->addLeafChild(
            fmt::format("df_perdriver_{}_{}", joinNodeId, ctx->driverId));

        hashBuild->setNoMoreInputCallback(
            [taskId = std::string(taskId),
             channels = std::move(channels),
             leafPool = std::move(leafPool)](const HashBuild& hb) {
              onHashBuildNoMoreInput(hb, taskId, channels, leafPool.get());
            });
      }

      return hashBuild;
    };
  }
};

} // namespace

void registerHashBuildFilterWrapper() {
  Operator::registerOperator(
      std::make_unique<HashBuildFilterWrapperTranslator>());
}

void setDynamicFilterJoinNodeIds(
    const std::string& taskId,
    std::unordered_map<
        std::string,
        std::vector<DynamicFilterChannel>> joinNodeChannels) {
  perTaskDppJoinNodes_.wlock()->emplace(
      taskId, std::move(joinNodeChannels));
}

void clearDynamicFilterJoinNodeIds(const std::string& taskId) {
  perTaskDppJoinNodes_.wlock()->erase(taskId);
}

} // namespace facebook::presto::operators
