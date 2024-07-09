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

#include "velox/dwio/common/TypeWithId.h"
#include "velox/experimental/wave/dwio/FormatData.h"
#include "velox/experimental/wave/exec/Wave.h"

namespace facebook::velox::wave {

class ReadStream;
class StructColumnReader;

/// dwio::SelectiveColumnReader for Wave
class ColumnReader {
 public:
  ColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      AbstractOperand* operand,
      FormatParams& params,
      velox::common::ScanSpec& scanSpec)
      : requestedType_(requestedType),
        fileType_(fileType),
        operand_(operand),
        formatData_(params.toFormatData(
            fileType_,
            scanSpec,
            operand ? operand->id : kNoOperand)),
        scanSpec_(&scanSpec) {}

  virtual ~ColumnReader() = default;

  const common::ScanSpec& scanSpec() const {
    return *scanSpec_;
  }

  const std::vector<ColumnReader*>& children() const {
    return children_;
  }

  bool hasNonNullFilter() const;

  int32_t totalRows() const {
    return formatData_->totalRows();
  }

  AbstractOperand* operand() const {
    return operand_;
  }

  /// Initializes 'op' for the column of 'this'. The op is made once and used
  /// for possibly multiple row ranges later.
  virtual void
  makeOp(ReadStream* readStream, ColumnAction action, ColumnOp& op);

  FormatData* formatData() const {
    return formatData_.get();
  }

  std::vector<std::unique_ptr<SplitStaging>>& splitStaging() {
    return staging_;
  }

  /// Records an event after the first griddize. many decodes may proceed for
  /// different rows on different streams after the griddize.
  void recordGriddize(Stream& stream) {
    VELOX_CHECK_NULL(griddizeEvent_);
    griddizeEvent_ = std::make_unique<Event>();
    griddizeEvent_->record(stream);
  }

  Event* griddizeEvent() const {
    return griddizeEvent_.get();
  }

 protected:
  TypePtr requestedType_;
  std::shared_ptr<const dwio::common::TypeWithId> fileType_;
  AbstractOperand* const operand_;
  std::unique_ptr<FormatData> formatData_;
  // Specification of filters, value extraction, pruning etc. The
  // spec is assigned at construction and the contents may change at
  // run time based on adaptation. Owned by caller.
  velox::common::ScanSpec* scanSpec_;

  std::vector<ColumnReader*> children_;

  // Staging of encoded data on device. Only set in the top struct reader.
  std::vector<std::unique_ptr<SplitStaging>> staging_;

  // Event realized after griddize completes. Non-first ReadStream launches must
  // wait for this.
  std::unique_ptr<Event> griddizeEvent_;
};

class ReadStream : public Executable {
 public:
  ReadStream(
      StructColumnReader* columnReader,
      WaveStream& waveStream,
      const OperandSet* firstColumns = nullptr);

  void setNullable(const AbstractOperand& op, bool nullable) {
    waveStream->setNullable(op, nullable);
  }

  /// Runs a sequence of kernel invocations until all eagerly produced
  /// columns have their last kernel in flight.  Transfers ownership
  /// of 'readStream' to its WaveStream. 'row' is the start relative
  /// to split start. 'rows' are offsets relative to 'row'.
  static void
  launch(std::unique_ptr<ReadStream> readStream, int32_t row, RowSet rows);

  DecodePrograms& programs() {
    return programs_;
  }

  // Prepares the next kernel launch in 'programs_'. Returns true if
  // all non-lazy activity will be complete after the program kernel
  // completes. Sets needSync if the next step(s) depend on the stream
  // being synced first, i.e. a device to host transfer must have
  // completed so that the next step can decide based on data received
  // from device.
  bool makePrograms(bool& needSync);

  bool filtersDone() const {
    return filtersDone_;
  }

  StructColumnReader* reader() const {
    return reader_;
  }

 private:
  // Computes starting points for multiple TBs per column if more rows are
  // needed than is good per TB.
  void makeGrid(Stream* stream);

  // Sets consistent blockStatus and temp across 'programs_'
  void setBlockStatusAndTemp();

  /// Makes column dependencies.
  void makeOps();
  // Sets the ops to 'offset_' and 'rows_'. Call before each batch.
  void prepareRead();
  void makeControl();

  // Makes steps to align values from non-last filters to the selection of the
  // last filter.
  void makeCompact(bool isSerial);

  // True if non-filter columns will be done sequentially in the
  // filters kernel. This will never loose if there is an always read
  // single column. This may loose if it were better to take the
  // launch cost but run all non-filter columns in their own TBs.
  bool decodenonFiltersInFiltersKernel();

  StructColumnReader* reader_;
  std::vector<AbstractOperand*> abstractOperands_;

  // Offset in top level rows from start of split.
  int32_t row_;

  // Row numbers to read relative to 'row_'.
  RowSet rows_;

  // Non-filter columns.
  std::vector<ColumnOp> ops_;

  // Filter columns in filter order.
  std::vector<ColumnOp> filters_;

  // Count of KBlockSize blocks in max top level rows.
  int32_t numBlocks_{0};

  // Pointer to staging owned by reader tree root. Not owned here because
  // lifetime is the split, not the batch.
  SplitStaging* currentStaging_;

  // Data to be copied from device, e.g. filter selectivities.
  ResultStaging resultStaging_;

  // Intermediate data to stay on device, e.g. selected rows.
  ResultStaging deviceStaging_;
  // Owning references to decode programs. Must be live for duration of kernels.
  std::vector<WaveBufferPtr> commands_;
  // Reusable control block for launching decode kernels.
  DecodePrograms programs_;
  // If no filters, the starting RowSet directly initializes the BlockStatus'es
  // at the end of the ReadStream.
  bool hasFilters_{false};
  bool filtersDone_{false};
  //  Sequence number of kernel launch.
  int32_t nthWave_{0};
  LaunchControl* control_{nullptr};

  // Set to true when after first griddize() and akeOps().
  bool inited_{false};
};

} // namespace facebook::velox::wave
