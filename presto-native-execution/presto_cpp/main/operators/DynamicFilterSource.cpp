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
#include "presto_cpp/main/operators/DynamicFilterSource.h"
#include <glog/logging.h>
#include "presto_cpp/main/SessionProperties.h"
#include "presto_cpp/main/types/TupleDomainBuilder.h"
#include "velox/exec/Task.h"
#include "velox/vector/SimpleVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto::operators {

namespace {

/// Default maximum size in bytes of discrete values per channel before
/// falling back to min/max range (1MB).
constexpr size_t kDefaultMaxSizeBytes = 1048576;

/// Returns true if the type uses int64-based storage for discrete values.
bool isIntLikeType(const TypePtr& type) {
  return type->isBigint() || type->isInteger() || type->isSmallint() ||
      type->isTinyint() || type->isDate();
}

/// Extracts an int64 value from a SimpleVector at the given index, casting
/// from the vector's native type.  The if-constexpr guard is required because
/// VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH instantiates this template for every
/// scalar TypeKind, including non-integral ones (VARCHAR, TIMESTAMP, …).
template <TypeKind KIND>
int64_t extractInt64(const BaseVector* vector, vector_size_t index) {
  using T = typename TypeTraits<KIND>::NativeType;
  if constexpr (std::is_integral_v<T>) {
    return static_cast<int64_t>(
        vector->asUnchecked<SimpleVector<T>>()->valueAt(index));
  } else {
    VELOX_UNREACHABLE(
        "extractInt64 called for non-integral type: {}",
        TypeTraits<KIND>::name);
  }
}

/// Per-channel state for collecting distinct values and min/max.
struct ChannelCollector {
  DynamicFilterChannel channel;
  folly::F14FastSet<int64_t> intValues;
  folly::F14FastSet<std::string> stringValues;
  std::optional<int64_t> minInt;
  std::optional<int64_t> maxInt;
  std::optional<std::string> minStr;
  std::optional<std::string> maxStr;
  bool hasNull{false};
  bool overflowed{false};
  size_t estimatedSizeBytes{0};
  size_t maxSizeBytes;

  explicit ChannelCollector(
      const DynamicFilterChannel& ch,
      size_t maxSizeBytes = kDefaultMaxSizeBytes)
      : channel(ch), maxSizeBytes(maxSizeBytes) {}

  void addInt(int64_t value) {
    if (!minInt.has_value() || value < *minInt) {
      minInt = value;
    }
    if (!maxInt.has_value() || value > *maxInt) {
      maxInt = value;
    }
    if (overflowed) {
      return;
    }
    auto [it, inserted] = intValues.insert(value);
    if (inserted) {
      // sizeof(int64_t) for value + ~16 bytes hash table overhead per entry.
      estimatedSizeBytes += sizeof(int64_t) + 16;
      if (estimatedSizeBytes > maxSizeBytes) {
        overflowed = true;
        intValues.clear();
        estimatedSizeBytes = 0;
      }
    }
  }

  void addString(std::string value) {
    if (!minStr.has_value() || value < *minStr) {
      minStr = value;
    }
    if (!maxStr.has_value() || value > *maxStr) {
      maxStr = value;
    }
    if (overflowed) {
      return;
    }
    auto valueSize = value.size();
    auto [it, inserted] = stringValues.insert(std::move(value));
    if (inserted) {
      estimatedSizeBytes += sizeof(std::string) + valueSize + 16;
      if (estimatedSizeBytes > maxSizeBytes) {
        overflowed = true;
        stringValues.clear();
        estimatedSizeBytes = 0;
      }
    }
  }

  /// Creates a variant matching the channel type from an int64_t value.
  /// All int-like values are collected as int64_t, but the variant must
  /// match the channel type (e.g., INTEGER for year transforms).
  variant toIntVariant(int64_t value) const {
    switch (channel.type->kind()) {
      case TypeKind::INTEGER:
        return variant(static_cast<int32_t>(value));
      case TypeKind::SMALLINT:
        return variant(static_cast<int16_t>(value));
      case TypeKind::TINYINT:
        return variant(static_cast<int8_t>(value));
      default:
        return variant(value); // BIGINT, DATE
    }
  }

  std::vector<variant> getDiscreteValues() const {
    std::vector<variant> result;
    if (overflowed) {
      return result;
    }
    if (!intValues.empty()) {
      result.reserve(intValues.size());
      for (auto v : intValues) {
        result.push_back(toIntVariant(v));
      }
    } else if (!stringValues.empty()) {
      result.reserve(stringValues.size());
      for (const auto& v : stringValues) {
        result.push_back(variant(v));
      }
    }
    return result;
  }

  std::optional<variant> getMinValue() const {
    if (minInt.has_value()) {
      return toIntVariant(*minInt);
    }
    if (minStr.has_value()) {
      return variant(*minStr);
    }
    return std::nullopt;
  }

  std::optional<variant> getMaxValue() const {
    if (maxInt.has_value()) {
      return toIntVariant(*maxInt);
    }
    if (maxStr.has_value()) {
      return variant(*maxStr);
    }
    return std::nullopt;
  }

  bool isEmpty() const {
    return !minInt.has_value() && !minStr.has_value() && intValues.empty() &&
        stringValues.empty() && !hasNull;
  }
};

/// Shared state across multiple DynamicFilterSource operators created by the
/// same factory (when task.concurrency > 1).
struct SharedFilterState {
  std::atomic<int32_t> createdCount{0};
  std::atomic<int32_t> finishedCount{0};
  std::atomic<bool> flushed{false};

  // Per-channel collectors from each finished operator.
  folly::Synchronized<std::vector<std::vector<ChannelCollector>>> partitions;

  std::vector<DynamicFilterChannel> channels;
  std::string taskId;
  size_t maxSizeBytes{kDefaultMaxSizeBytes};

  void tryFlush(memory::MemoryPool* pool) {
    VLOG(1) << "tryFlush: task=" << taskId << " flushed=" << flushed.load()
            << " finished=" << finishedCount.load()
            << " created=" << createdCount.load();
    if (flushed.load()) {
      return;
    }
    if (finishedCount.load() < createdCount.load() ||
        createdCount.load() == 0) {
      return;
    }
    if (flushed.exchange(true)) {
      return; // Already flushed by another thread.
    }

    // Merge all partitions into per-channel collectors.
    auto allPartitions = partitions.wlock();
    std::vector<ChannelCollector> merged;
    merged.reserve(channels.size());
    for (size_t i = 0; i < channels.size(); ++i) {
      merged.emplace_back(channels[i]);
    }

    for (auto& partition : *allPartitions) {
      for (size_t i = 0; i < channels.size(); ++i) {
        auto& src = partition[i];
        auto& dst = merged[i];
        dst.hasNull = dst.hasNull || src.hasNull;

        // Merge int min/max.
        if (src.minInt.has_value()) {
          if (!dst.minInt.has_value() || *src.minInt < *dst.minInt) {
            dst.minInt = src.minInt;
          }
        }
        if (src.maxInt.has_value()) {
          if (!dst.maxInt.has_value() || *src.maxInt > *dst.maxInt) {
            dst.maxInt = src.maxInt;
          }
        }
        // Merge string min/max.
        if (src.minStr.has_value()) {
          if (!dst.minStr.has_value() || *src.minStr < *dst.minStr) {
            dst.minStr = src.minStr;
          }
        }
        if (src.maxStr.has_value()) {
          if (!dst.maxStr.has_value() || *src.maxStr > *dst.maxStr) {
            dst.maxStr = src.maxStr;
          }
        }

        if (!src.overflowed && !dst.overflowed) {
          for (auto v : src.intValues) {
            auto [it, inserted] = dst.intValues.insert(v);
            if (inserted) {
              dst.estimatedSizeBytes += sizeof(int64_t) + 16;
            }
          }
          for (const auto& v : src.stringValues) {
            auto [it, inserted] = dst.stringValues.insert(v);
            if (inserted) {
              dst.estimatedSizeBytes += sizeof(std::string) + v.size() + 16;
            }
          }
          if (dst.estimatedSizeBytes > maxSizeBytes) {
            dst.overflowed = true;
            dst.intValues.clear();
            dst.stringValues.clear();
            dst.estimatedSizeBytes = 0;
          }
        } else {
          dst.overflowed = true;
          dst.intValues.clear();
          dst.stringValues.clear();
        }
      }
    }

    // Build TupleDomains and fire callback.
    std::map<std::string, protocol::TupleDomain<std::string>> filters;
    std::unordered_set<std::string> filterIds;

    bool allEmpty = true;
    for (auto& collector : merged) {
      if (!collector.isEmpty()) {
        allEmpty = false;
        break;
      }
    }

    for (auto& collector : merged) {
      filterIds.insert(collector.channel.filterId);
      if (allEmpty) {
        // Empty build side -> none() for all channels.
        filters[collector.channel.filterId] = buildNoneTupleDomain();
      } else {
        auto discrete = collector.getDiscreteValues();
        filters[collector.channel.filterId] = buildTupleDomain(
            collector.channel.filterId,
            collector.channel.type,
            discrete,
            collector.getMinValue(),
            collector.getMaxValue(),
            collector.hasNull,
            pool);
      }
    }

    VLOG(1) << "tryFlush: firing callback for task=" << taskId
            << " filters=" << filters.size()
            << " filterIds=" << filterIds.size() << " allEmpty=" << allEmpty;
    DynamicFilterCallbackRegistry::instance().fire(
        taskId, std::move(filters), std::move(filterIds));
  }
};

/// The DynamicFilterSource operator: a pass-through that collects distinct
/// values from specified columns as data flows through, then delivers the
/// collected TupleDomain to the coordinator via PrestoTask on noMoreInput().
class DynamicFilterSource : public Operator {
 public:
  DynamicFilterSource(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const DynamicFilterSourceNode>& planNode,
      std::shared_ptr<SharedFilterState> sharedState)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "DynamicFilterSource"),
        sharedState_(std::move(sharedState)) {
    const auto& channels = planNode->channels();
    collectors_.reserve(channels.size());
    for (const auto& channel : channels) {
      collectors_.emplace_back(channel, sharedState_->maxSizeBytes);
    }
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    for (auto& collector : collectors_) {
      auto column = input->childAt(collector.channel.columnIndex);
      auto size = input->size();

      if (isIntLikeType(collector.channel.type)) {
        for (vector_size_t i = 0; i < size; ++i) {
          if (column->isNullAt(i)) {
            collector.hasNull = true;
            continue;
          }
          int64_t value = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
              extractInt64, collector.channel.type->kind(), column.get(), i);
          collector.addInt(value);
        }
      } else if (
          collector.channel.type->isVarchar() ||
          collector.channel.type->isVarbinary()) {
        auto* simpleVector = column->asUnchecked<SimpleVector<StringView>>();
        for (vector_size_t i = 0; i < size; ++i) {
          if (column->isNullAt(i)) {
            collector.hasNull = true;
            continue;
          }
          auto sv = simpleVector->valueAt(i);
          collector.addString(std::string(sv.data(), sv.size()));
        }
      } else {
        // Unsupported type: collect min/max only via range fallback.
        collector.overflowed = true;
      }
    }
    // Pass through unchanged.
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    return std::move(input_);
  }

  void noMoreInput() override {
    Operator::noMoreInput();

    // Add our collectors to the shared state.
    sharedState_->partitions.wlock()->push_back(std::move(collectors_));
    sharedState_->finishedCount.fetch_add(1);
    sharedState_->tryFlush(operatorCtx_->pool());
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

 private:
  std::shared_ptr<SharedFilterState> sharedState_;
  std::vector<ChannelCollector> collectors_;
  RowVectorPtr input_;
};

/// Registry of shared filter states for multi-driver coordination.
/// Key: "taskId:planNodeId". All drivers created from the same
/// DynamicFilterSourceNode in the same task share one SharedFilterState.
folly::Synchronized<std::map<std::string, std::shared_ptr<SharedFilterState>>>&
getSharedFilterStates() {
  static folly::Synchronized<
      std::map<std::string, std::shared_ptr<SharedFilterState>>>
      instance;
  return instance;
}

void cleanupSharedStatesForTask(const std::string& taskId) {
  auto prefix = taskId + ":";
  auto locked = getSharedFilterStates().wlock();
  for (auto it = locked->begin(); it != locked->end();) {
    if (it->first.compare(0, prefix.size(), prefix) == 0) {
      it = locked->erase(it);
    } else {
      ++it;
    }
  }
}

} // namespace

// --- DynamicFilterSourceNode implementation ---

folly::dynamic DynamicFilterSourceNode::serialize() const {
  auto obj = PlanNode::serialize();
  folly::dynamic channelsArr = folly::dynamic::array();
  for (const auto& ch : channels_) {
    folly::dynamic chObj = folly::dynamic::object();
    chObj["filterId"] = ch.filterId;
    chObj["columnIndex"] = static_cast<int>(ch.columnIndex);
    chObj["type"] = ch.type->toString();
    channelsArr.push_back(std::move(chObj));
  }
  obj["channels"] = channelsArr;
  obj["sources"] = ISerializable::serialize(sources_);
  return obj;
}

velox::core::PlanNodePtr DynamicFilterSourceNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto sources = ISerializable::deserialize<std::vector<core::PlanNode>>(
      obj["sources"], context);
  std::vector<DynamicFilterChannel> channels;
  for (const auto& chObj : obj["channels"]) {
    DynamicFilterChannel ch;
    ch.filterId = chObj["filterId"].asString();
    ch.columnIndex = static_cast<column_index_t>(chObj["columnIndex"].asInt());
    // Derive type from the source output type.
    ch.type = sources[0]->outputType()->childAt(ch.columnIndex);
    channels.push_back(std::move(ch));
  }
  return std::make_shared<DynamicFilterSourceNode>(
      obj["id"].asString(), std::move(channels), sources[0]);
}

void DynamicFilterSourceNode::addDetails(std::stringstream& stream) const {
  stream << "channels=[";
  for (size_t i = 0; i < channels_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << channels_[i].filterId << ":" << channels_[i].columnIndex;
  }
  stream << "]";
}

// --- DynamicFilterCallbackRegistry implementation ---

DynamicFilterCallbackRegistry& DynamicFilterCallbackRegistry::instance() {
  static DynamicFilterCallbackRegistry registry;
  return registry;
}

void DynamicFilterCallbackRegistry::registerCallbacks(
    const std::string& taskId,
    FlushCallback flushCallback,
    RegisterCallback registerCallback) {
  callbacks_.wlock()->emplace(
      taskId, Callbacks{std::move(flushCallback), std::move(registerCallback)});
}

void DynamicFilterCallbackRegistry::registerFilterIds(
    const std::string& taskId,
    const std::unordered_set<std::string>& filterIds) {
  RegisterCallback callback;
  {
    auto locked = callbacks_.rlock();
    auto it = locked->find(taskId);
    if (it == locked->end()) {
      LOG(WARNING) << "registerFilterIds: no callback found for task="
                   << taskId;
      return;
    }
    callback = it->second.registerIds;
  }
  if (callback) {
    callback(filterIds);
  }
}

void DynamicFilterCallbackRegistry::fire(
    const std::string& taskId,
    std::map<std::string, protocol::TupleDomain<std::string>> filters,
    std::unordered_set<std::string> flushedFilterIds) {
  VLOG(1) << "fire: task=" << taskId << " filters=" << filters.size()
          << " flushedFilterIds=" << flushedFilterIds.size();
  FlushCallback callback;
  {
    auto locked = callbacks_.rlock();
    auto it = locked->find(taskId);
    if (it == locked->end()) {
      LOG(WARNING) << "fire: no callback found for task=" << taskId;
      return;
    }
    callback = it->second.flush;
  }
  if (callback) {
    callback(filters, flushedFilterIds);
  }
}

void DynamicFilterCallbackRegistry::removeCallback(const std::string& taskId) {
  callbacks_.wlock()->erase(taskId);
  cleanupSharedStatesForTask(taskId);
}

// --- DynamicFilterSourceTranslator ---

std::unique_ptr<Operator> DynamicFilterSourceTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto dfNode =
          std::dynamic_pointer_cast<const DynamicFilterSourceNode>(node)) {
    // Get or create shared state for multi-driver coordination.
    auto key = ctx->task->taskId() + ":" + std::string(dfNode->id());
    std::shared_ptr<SharedFilterState> sharedState;
    {
      auto locked = getSharedFilterStates().wlock();
      auto it = locked->find(key);
      if (it == locked->end()) {
        auto state = std::make_shared<SharedFilterState>();
        state->channels = dfNode->channels();
        state->taskId = ctx->task->taskId();
        // Read max size from session property if available.
        auto& queryConfig = ctx->task->queryCtx()->queryConfig();
        auto maxSizeStr = queryConfig.get<std::string>(
            SessionProperties::kDistributedDynamicFilterMaxSize,
            std::to_string(kDefaultMaxSizeBytes));
        state->maxSizeBytes = static_cast<size_t>(std::stoull(maxSizeStr));
        it = locked->emplace(key, std::move(state)).first;

        // Register filter IDs on the PrestoTask so it knows how many
        // pipelines must flush before operatorCompleted can be true.
        std::unordered_set<std::string> filterIds;
        for (const auto& ch : dfNode->channels()) {
          filterIds.insert(ch.filterId);
        }
        DynamicFilterCallbackRegistry::instance().registerFilterIds(
            it->second->taskId, filterIds);
      }
      sharedState = it->second;
    }
    sharedState->createdCount.fetch_add(1);
    return std::make_unique<DynamicFilterSource>(
        id, ctx, dfNode, std::move(sharedState));
  }
  return nullptr;
}

} // namespace facebook::presto::operators
