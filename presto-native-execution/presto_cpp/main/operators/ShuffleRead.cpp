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
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "velox/exec/Exchange.h"
#include "velox/serializers/UnsafeRowSerializer.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
class ShuffleReadOperator : public Exchange {
 public:
  ShuffleReadOperator(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const ShuffleReadNode>& shuffleReadNode,
      std::shared_ptr<ExchangeClient> exchangeClient)
      : Exchange(
            operatorId,
            ctx,
            std::make_shared<core::ExchangeNode>(
                shuffleReadNode->id(),
                shuffleReadNode->outputType()),
            exchangeClient,
            "ShuffleRead"),
        serde_(std::make_unique<serializer::spark::UnsafeRowVectorSerde>()) {}

 protected:
  VectorSerde* getSerde() override {
    return serde_.get();
  }

 private:
  std::unique_ptr<serializer::spark::UnsafeRowVectorSerde> serde_;
};
} // namespace

std::unique_ptr<Operator> ShuffleReadTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node,
    std::shared_ptr<ExchangeClient> exchangeClient) {
  if (auto shuffleReadNode =
          std::dynamic_pointer_cast<const ShuffleReadNode>(node)) {
    return std::make_unique<ShuffleReadOperator>(
        id, ctx, shuffleReadNode, exchangeClient);
  }
  return nullptr;
}
} // namespace facebook::presto::operators
