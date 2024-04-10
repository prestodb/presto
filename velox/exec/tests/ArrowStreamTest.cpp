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

#include "velox/vector/arrow/Bridge.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace {
enum class ErrorCode { kNoError, kGetNextFailed, kGetSchemaFailed };
} // namespace

class ArrowStreamTest : public OperatorTestBase {
 protected:
  /// A mock Arrow reader returns an Arrow array for each call of getNext.
  class ArrowReader {
   public:
    ArrowReader(
        std::shared_ptr<memory::MemoryPool> pool,
        const std::vector<RowVectorPtr>& vectors,
        const TypePtr& type,
        bool failGetNext = false,
        bool failGetSchema = false)
        : pool_(pool),
          vectors_(vectors),
          type_(type),
          failGetNext_(failGetNext),
          failGetSchema_(failGetSchema) {}

    int getNext(struct ArrowArray* outArray) {
      if (vectorIndex_ < vectors_.size()) {
        exportToArrow(vectors_[vectorIndex_], *outArray, pool_.get(), options_);
        vectorIndex_ += 1;
      } else {
        // End of stream. Mark the array released.
        outArray->release = nullptr;
      }
      return failGetNext_ ? (int)ErrorCode::kGetNextFailed
                          : (int)ErrorCode::kNoError;
    }

    int getArrowSchema(ArrowSchema& out) {
      exportToArrow(BaseVector::create(type_, 0, pool_.get()), out, options_);
      return failGetSchema_ ? (int)ErrorCode::kGetSchemaFailed
                            : (int)ErrorCode::kNoError;
    }

   private:
    ArrowOptions options_;
    const std::shared_ptr<memory::MemoryPool> pool_;
    const std::vector<RowVectorPtr>& vectors_;
    const TypePtr type_;
    const bool failGetNext_;
    const bool failGetSchema_;
    int vectorIndex_ = 0;
  };

  /// For the export from Arrow reader to Arrow array stream.
  class ExportedArrayStream {
   public:
    ExportedArrayStream() = delete;

    struct PrivateData {
      explicit PrivateData(std::shared_ptr<ArrowReader> reader)
          : reader_(std::move(reader)) {}

      int getSchema(struct ArrowSchema* outSchema) {
        auto errorCode = reader_->getArrowSchema(*outSchema);
        lastError_ = toErrorMessage(static_cast<ErrorCode>(errorCode));
        return errorCode;
      }

      int getNext(struct ArrowArray* outArray) {
        auto errorCode = reader_->getNext(outArray);
        lastError_ = toErrorMessage(static_cast<ErrorCode>(errorCode));
        return errorCode;
      }

      const char* getLastError() {
        return lastError_.empty() ? nullptr : lastError_.c_str();
      }

     private:
      std::shared_ptr<ArrowReader> reader_;
      std::string lastError_;
    };

    static int getSchema(
        struct ArrowArrayStream* stream,
        struct ArrowSchema* outSchema) {
      return privateData(stream)->getSchema(outSchema);
    }

    static int getNext(
        struct ArrowArrayStream* stream,
        struct ArrowArray* outArray) {
      return privateData(stream)->getNext(outArray);
    }

    static void release(struct ArrowArrayStream* stream) {
      if (stream->release == nullptr) {
        // Array stream is released.
        return;
      }
      VELOX_CHECK_NOT_NULL(privateData(stream));
      delete privateData(stream);
      // Mark stream as released.
      stream->release = nullptr;
    }

    static const char* getLastError(struct ArrowArrayStream* stream) {
      return privateData(stream)->getLastError();
    }

   private:
    static PrivateData* privateData(struct ArrowArrayStream* stream) {
      return reinterpret_cast<PrivateData*>(stream->private_data);
    }

    static std::string toErrorMessage(ErrorCode errorCode) {
      switch (errorCode) {
        case ErrorCode::kNoError:
          return "";
        case ErrorCode::kGetNextFailed:
          return "get_next failed.";
        case ErrorCode::kGetSchemaFailed:
          return "get_schema failed.";
        default:
          return "failed.";
      }
    }
  };

  void exportArrowStream(
      std::shared_ptr<ArrowReader> reader,
      struct ArrowArrayStream* out) {
    out->get_schema = ExportedArrayStream::getSchema;
    out->get_next = ExportedArrayStream::getNext;
    out->get_last_error = ExportedArrayStream::getLastError;
    out->release = ExportedArrayStream::release;
    out->private_data = new ExportedArrayStream::PrivateData{std::move(reader)};
  }
};

TEST_F(ArrowStreamTest, basic) {
  vector_size_t size = 1'000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    vectors.push_back(makeRowVector(
        {makeFlatVector<bool>(
             size, [](auto row) { return (row / 10) % 2 == 0; }, nullEvery(3)),
         makeFlatVector<int8_t>(
             size, [](auto row) { return row % 100; }, nullEvery(5)),
         makeFlatVector<int16_t>(
             size, [&](auto row) { return row * i; }, nullEvery(2)),
         makeFlatVector<int32_t>(
             size, [](auto row) { return row; }, nullEvery(5)),
         makeFlatVector<int64_t>(
             size,
             [&](vector_size_t row) { return size * i + row; },
             nullEvery(5)),
         makeFlatVector<float>(
             size, [](vector_size_t row) { return row * 1.1; }, nullEvery(5)),
         makeFlatVector<double>(
             size, [](vector_size_t row) { return row * 1.3; }, nullEvery(11)),
         makeFlatVector<StringView>(
             size,
             [](vector_size_t row) {
               return StringView::makeInline(
                   std::to_string(100000000000 + (uint64_t)(row % 100)));
             },
             nullEvery(7)),
         makeFlatVector<Timestamp>(
             size,
             [&](vector_size_t row) { return Timestamp(row, row * 1000); },
             nullEvery(5))}));
  }
  createDuckDbTable(vectors);
  auto type = asRowType(vectors[0]->type());
  auto reader = std::make_shared<ArrowReader>(pool_, vectors, type);
  struct ArrowArrayStream arrowStream;
  exportArrowStream(reader, &arrowStream);
  auto plan = std::make_shared<core::ArrowStreamNode>(
      "0", type, std::make_shared<ArrowArrayStream>(arrowStream));
  assertQuery(plan, "SELECT * FROM tmp");
}

TEST_F(ArrowStreamTest, error) {
  vector_size_t size = 1'000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    vectors.push_back(makeRowVector({makeFlatVector<int32_t>(
        size, [](auto row) { return row; }, nullEvery(5))}));
  }
  auto type = asRowType(vectors[0]->type());

  // Assert getNext error message.
  struct ArrowArrayStream arrowStream;
  exportArrowStream(
      std::make_shared<ArrowReader>(pool_, vectors, type, true, false),
      &arrowStream);
  auto plan = std::make_shared<core::ArrowStreamNode>(
      "0", type, std::make_shared<ArrowArrayStream>(arrowStream));
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool_.get()),
      "Failed to call get_next on ArrowStream: get_next failed.");

  // Assert getSchema error message.
  exportArrowStream(
      std::make_shared<ArrowReader>(pool_, vectors, type, false, true),
      &arrowStream);
  plan = std::make_shared<core::ArrowStreamNode>(
      "0", type, std::make_shared<ArrowArrayStream>(arrowStream));
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool_.get()),
      "Failed to call get_schema on ArrowStream: get_schema failed.");
}
