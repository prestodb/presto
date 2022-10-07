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

#include "velox/common/base/Exceptions.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::window {

// Types of rank functions.
enum class RankType {
  kRank,
  kDenseRank,
  kPercentRank,
};

namespace {

template <RankType TRank, typename TResult>
class RankFunction : public exec::WindowFunction {
 public:
  explicit RankFunction(const TypePtr& resultType)
      : WindowFunction(resultType, nullptr) {}

  void resetPartition(const exec::WindowPartition* partition) override {
    rank_ = 1;
    currentPeerGroupStart_ = 0;
    previousPeerCount_ = 0;
    numPartitionRows_ = partition->numRows();
  }

  void apply(
      const BufferPtr& peerGroupStarts,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& /*frameStarts*/,
      const BufferPtr& /*frameEnds*/,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    int numRows = peerGroupStarts->size() / sizeof(vector_size_t);
    auto* rawPeerStarts = peerGroupStarts->as<vector_size_t>();
    auto rawValues = result->asFlatVector<TResult>()->mutableRawValues();

    for (int i = 0; i < numRows; i++) {
      auto start = rawPeerStarts[i];
      if (start != currentPeerGroupStart_) {
        currentPeerGroupStart_ = start;
        if constexpr (TRank == RankType::kDenseRank) {
          if (previousPeerCount_ > 0) {
            ++rank_;
          }
        } else {
          rank_ += previousPeerCount_;
        }
        previousPeerCount_ = 0;
      }

      if constexpr (TRank == RankType::kPercentRank) {
        rawValues[resultOffset + i] = (numPartitionRows_ == 1)
            ? 0
            : double(rank_ - 1) / (numPartitionRows_ - 1);
      } else {
        rawValues[resultOffset + i] = rank_;
      }
      previousPeerCount_ += 1;
    }
  }

 private:
  int32_t currentPeerGroupStart_ = 0;
  int32_t previousPeerCount_ = 0;
  int64_t rank_ = 1;
  vector_size_t numPartitionRows_ = 1;
};

} // namespace

template <RankType TRank, typename TResult>
void registerRankInternal(
    const std::string& name,
    const std::string& returnType) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder().returnType(returnType).build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      [name](
          const std::vector<exec::WindowFunctionArg>& /*args*/,
          const TypePtr& resultType,
          velox::memory::MemoryPool* /*pool*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<RankFunction<TRank, TResult>>(resultType);
      });
}

void registerRank(const std::string& name) {
  registerRankInternal<RankType::kRank, int64_t>(name, "bigint");
}
void registerDenseRank(const std::string& name) {
  registerRankInternal<RankType::kDenseRank, int64_t>(name, "bigint");
}
void registerPercentRank(const std::string& name) {
  registerRankInternal<RankType::kPercentRank, double>(name, "double");
}

} // namespace facebook::velox::window
