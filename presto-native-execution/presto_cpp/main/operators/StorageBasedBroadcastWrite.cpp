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
#include "presto_cpp/main/operators/StorageBasedBroadcastWrite.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {

class StorageBasedBroadcastWrite : public Operator {
 public:
  StorageBasedBroadcastWrite(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const StorageBasedBroadcastWriteNode>& planNode)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "StorageBasedBroadcastWrite"),
        writerId_(fmt::format(
            "{}_{}",
            operatorId,
            ctx->driverId)),
        shuffle_(planNode->shuffle()){}

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    std::ostringstream output;

    auto numRows = input->size();

    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }
    auto arena =
        std::make_unique<StreamArena>(memory::MappedMemory::getInstance());
    auto rowType = asRowType(outputType_);
    // Serialize the vector into a page.
    auto serializer =
        serde_.createSerializer(rowType, input->size(), arena.get(), nullptr);
    serializer->append(input, folly::Range(rows.data(), numRows));
    OStreamOutputStream out(&output);
    serializer->flush(&out);
    shuffle_->writeColumnar(std::string_view(output.str().data(), output.str().size()), writerId_);
    return;
    int size = output.str().size();

    // Write the page size followed by its binary content.
    if (file_ == nullptr) {
      file_ = velox::filesystems::getFileSystem(filePath_, {})
                  ->openFileForWrite(filePath_);
      VELOX_CHECK_NOT_NULL(
          file_, "Unable to open the broadcast file {}.", filePath_);
    }

    file_->append(std::string_view((char*)&size, sizeof(size)));
    file_->append(std::string_view(output.str().data(), output.str().size()));
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    file_->flush();
    file_->close();
  }

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  velox::serializer::presto::PrestoVectorSerde serde_;

  std::unique_ptr<velox::WriteFile> file_ = nullptr;

  const std::string filePath_;

  ShuffleInterface* shuffle_;

  const std::string writerId_;
};
} // namespace

std::unique_ptr<Operator> StorageBasedBroadcastWriteTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto broadcastWriteNode =
          std::dynamic_pointer_cast<const StorageBasedBroadcastWriteNode>(
              node)) {
    return std::make_unique<StorageBasedBroadcastWrite>(
        id, ctx, broadcastWriteNode);
  }
  return nullptr;
}
} // namespace facebook::presto::operators