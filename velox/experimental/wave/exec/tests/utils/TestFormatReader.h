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

#include "velox/connectors/hive/FileHandle.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/exec/tests/utils/FileFormat.h"
#include "velox/type/Subfield.h"

namespace facebook::velox::wave::test {

class TestFormatData : public wave::FormatData {
 public:
  static constexpr int32_t kNotRegistered = -1;

  TestFormatData(
      OperandId operand,
      int32_t totalRows,
      const test::Column* column)
      : operand_(operand), totalRows_(totalRows), column_(column) {}

  bool hasNulls() const override {
    return column_->nulls != nullptr;
  }

  int32_t totalRows() const override {
    return totalRows_;
  }

  void newBatch(int32_t startRow) override {
    currentRow_ = startRow;
    queued_ = false;
  }

  void griddize(
      int32_t blockSize,
      int32_t numBlocks,
      ResultStaging& deviceStaging,
      ResultStaging& resultStaging,
      SplitStaging& staging,
      DecodePrograms& programs,
      ReadStream& stream) override;

  void startOp(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ResultStaging& deviceStaging,
      ResultStaging& resultStaging,
      SplitStaging& staging,
      DecodePrograms& program,
      ReadStream& stream) override;

 private:
  // Stages movement of nulls to device if any. Returns the id of the buffer or
  // kNotRegisterd.
  int32_t stageNulls(ResultStaging& deviceStaging, SplitStaging& splitStaging);

  const OperandId operand_;

  int32_t totalRows_{0};

  const test::Column* column_;
  bool staged_{false};
  bool nullsStaged_{false};
  bool queued_{false};
  int32_t numStaged_{0};
  // The device side data area start, set after the staged transfer is done.
  void* deviceBuffer_{nullptr};
};

class TestFormatParams : public wave::FormatParams {
 public:
  TestFormatParams(
      memory::MemoryPool& pool,
      dwio::common::ColumnReaderStatistics& stats,
      const test::Stripe* stripe)
      : FormatParams(pool, stats), stripe_(stripe) {}

  std::unique_ptr<FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const velox::common::ScanSpec& scanSpec,
      OperandId operand) override;

  const Stripe* stripe() const {
    return stripe_;
  }

 private:
  const test::Stripe* stripe_;
};

class TestFormatReader {
 public:
  static std::unique_ptr<ColumnReader> build(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      TestFormatParams& params,
      common::ScanSpec& scanSpec,
      std::vector<std::unique_ptr<common::Subfield::PathElement>>& path,
      const DefinesMap& defines,
      bool isRoot = false);
};

} // namespace facebook::velox::wave::test
