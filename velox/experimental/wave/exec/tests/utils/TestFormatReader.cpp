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

#include "velox/experimental/wave/exec/tests/utils/TestFormatReader.h"
#include "velox/experimental/wave/dwio/StructColumnReader.h"

DECLARE_int32(wave_reader_rows_per_tb);

namespace facebook::velox::wave::test {

using common::Subfield;

std::unique_ptr<FormatData> TestFormatParams::toFormatData(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const velox::common::ScanSpec& scanSpec,
    OperandId operand) {
  auto* column = type->id() == 0 ? nullptr : stripe_->findColumn(*type);
  return std::make_unique<TestFormatData>(
      operand, stripe_->columns[0]->numValues, column);
}

BufferId TestFormatData::stageNulls(
    ResultStaging& deviceStaging,
    SplitStaging& splitStaging) {
  if (!column_->nulls) {
    nullsStaged_ = true;
    return kNotRegistered;
  }

  if (nullsStaged_) {
    splitStaging.addDependency(nullsStagingId_);
    return kNotRegistered;
  }
  nullsStaged_ = true;
  auto* nulls = column_->nulls.get();
  Staging staging(
      nulls->values->as<char>(),
      bits::nwords(column_->numValues) * sizeof(uint64_t),
      column_->region);
  nullsBufferId_ = splitStaging.add(staging);
  nullsStagingId_ = splitStaging.id();
  splitStaging.registerPointer(nullsBufferId_, &grid_.nulls, true);
  return nullsBufferId_;
}

void TestFormatData::griddize(
    int32_t blockSize,
    int32_t numBlocks,
    ResultStaging& deviceStaging,
    ResultStaging& resultStaging,
    SplitStaging& staging,
    DecodePrograms& programs,
    ReadStream& stream) {
  constexpr int32_t kCountStride = 1024;
  if (griddized_) {
    return;
  }
  griddized_ = true;
  if (!column_->nulls) {
    return;
  }
  // If the whole stripe is covered by a single TB, there is no need for a
  // separate griddize kernel.
  if (blockSize >= column_->numValues) {
    return;
  }
  auto id = stageNulls(deviceStaging, staging);

  auto count = std::make_unique<GpuDecode>();
  staging.registerPointer(id, &count->data.countBits.bits, true);
  auto numStrides =
      bits::roundUp(column_->numValues, kCountStride) / kCountStride;
  auto resultId = deviceStaging.reserve(sizeof(int32_t) * numStrides);
  deviceStaging.registerPointer(resultId, &count->result, true);
  deviceStaging.registerPointer(resultId, &grid_.numNonNull, true);
  count->step = DecodeStep::kCountBits;
  count->data.countBits.numBits = column_->numValues;
  count->data.countBits.resultStride = kCountStride;
  programs.programs.emplace_back();
  programs.programs.back().push_back(std::move(count));
}

void TestFormatData::startOp(
    ColumnOp& op,
    const ColumnOp* previousFilter,
    ResultStaging& deviceStaging,
    ResultStaging& resultStaging,
    SplitStaging& splitStaging,
    DecodePrograms& program,
    ReadStream& stream) {
  VELOX_CHECK_NOT_NULL(column_);
  BufferId id = kNoBufferId;
  // If nulls were not staged on device in griddize() they will be moved now for
  // the single TB.
  stageNulls(deviceStaging, splitStaging);
  if (!staged_) {
    staged_ = true;
    Staging staging(
        column_->values->as<char>(), column_->values->size(), column_->region);
    id = splitStaging.add(staging);
    lastStagingId_ = splitStaging.id();
  } else {
    splitStaging.addDependency(lastStagingId_);
  }
  auto rowsPerBlock = FLAGS_wave_reader_rows_per_tb;
  int32_t numBlocks =
      bits::roundUp(op.rows.size(), rowsPerBlock) / rowsPerBlock;
  if (numBlocks > 1) {
    VELOX_CHECK(griddized_);
  }
  VELOX_CHECK_LT(numBlocks, 256 * 256, "Overflow 16 bit block number");
  for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
    auto rowsInBlock = std::min<int32_t>(
        rowsPerBlock, op.rows.size() - (blockIdx * rowsPerBlock));
    auto columnKind = static_cast<WaveTypeKind>(column_->kind);

    auto step = makeStep(
        op,
        previousFilter,
        deviceStaging,
        splitStaging,
        stream,
        columnKind,
        blockIdx);
    if (column_->encoding == Encoding::kFlat) {
      if (column_->baseline == 0 &&
          (column_->bitWidth == 32 || column_->bitWidth == 64)) {
        step->encoding = DecodeStep::kTrivial;
        step->data.trivial.dataType = columnKind;
        step->data.trivial.input = 0;
        step->data.trivial.begin = currentRow_;
        step->data.trivial.end = currentRow_ + rowsInBlock;
        step->data.trivial.input = nullptr;
        if (id != kNoBufferId) {
          splitStaging.registerPointer(id, &step->data.trivial.input, true);
          if (blockIdx == 0) {
            splitStaging.registerPointer(id, &deviceBuffer_, true);
          }
        } else {
          step->data.trivial.input = deviceBuffer_;
        }
        step->data.trivial.result = op.waveVector->values<char>();
      } else {
        step->encoding = DecodeStep::kDictionaryOnBitpack;
        // Just bit pack, no dictionary.
        step->data.dictionaryOnBitpack.alphabet = nullptr;
        step->data.dictionaryOnBitpack.baseline = column_->baseline;
        step->data.dictionaryOnBitpack.bitWidth = column_->bitWidth;
        step->data.dictionaryOnBitpack.indices = nullptr;
        step->data.dictionaryOnBitpack.begin = currentRow_;
        if (id != kNoBufferId) {
          splitStaging.registerPointer(
              id, &step->data.dictionaryOnBitpack.indices, true);
          if (blockIdx == 0) {
            splitStaging.registerPointer(id, &deviceBuffer_, true);
          }
        } else {
          step->data.dictionaryOnBitpack.indices =
              reinterpret_cast<uint64_t*>(deviceBuffer_);
        }
      }
    } else {
      VELOX_NYI("Non flat test encoding");
    }
    op.isFinal = true;
    std::vector<std::unique_ptr<GpuDecode>>* steps;

    // Programs are parallel after filters
    if (stream.filtersDone() || !previousFilter) {
      program.programs.emplace_back();
      steps = &program.programs.back();
    } else {
      steps = &program.programs[blockIdx];
    }
    steps->push_back(std::move(step));
  }
}

class TestStructColumnReader : public StructColumnReader {
 public:
  TestStructColumnReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      TestFormatParams& params,
      common::ScanSpec& scanSpec,
      std::vector<std::unique_ptr<Subfield::PathElement>>& path,
      const DefinesMap& defines,
      bool isRoot)
      : StructColumnReader(
            requestedType,
            fileType,
            pathToOperand(defines, path),
            params,
            scanSpec,
            isRoot) {
    // A reader tree may be constructed while the ScanSpec is being used
    // for another read. This happens when the next stripe is being
    // prepared while the previous one is reading.
    auto& childSpecs = scanSpec.stableChildren();
    for (auto i = 0; i < childSpecs.size(); ++i) {
      auto childSpec = childSpecs[i];
      if (isChildConstant(*childSpec)) {
        VELOX_NYI();
        continue;
      }
      auto childFileType = fileType_->childByName(childSpec->fieldName());
      auto childRequestedType = requestedType_->as<TypeKind::ROW>().findChild(
          folly::StringPiece(childSpec->fieldName()));
      auto childParams = TestFormatParams(
          params.pool(), params.runtimeStatistics(), params.stripe());

      path.push_back(std::make_unique<common::Subfield::NestedField>(
          childSpec->fieldName()));
      addChild(TestFormatReader::build(
          childRequestedType,
          childFileType,
          params,
          *childSpec,
          path,
          defines));
      path.pop_back();
      childSpec->setSubscript(children_.size() - 1);
    }
  }
};

std::unique_ptr<ColumnReader> buildIntegerReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    TestFormatParams& params,
    common::ScanSpec& scanSpec,
    std::vector<std::unique_ptr<Subfield::PathElement>>& path,
    const DefinesMap& defines) {
  return std::make_unique<ColumnReader>(
      requestedType, fileType, pathToOperand(defines, path), params, scanSpec);
}

// static
std::unique_ptr<ColumnReader> TestFormatReader::build(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    TestFormatParams& params,
    common::ScanSpec& scanSpec,
    std::vector<std::unique_ptr<Subfield::PathElement>>& path,
    const DefinesMap& defines,
    bool isRoot) {
  switch (fileType->type()->kind()) {
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return buildIntegerReader(
          requestedType, fileType, params, scanSpec, path, defines);

    case TypeKind::ROW:
      return std::make_unique<TestStructColumnReader>(
          requestedType, fileType, params, scanSpec, path, defines, isRoot);
    default:
      VELOX_UNREACHABLE();
  }
}

} // namespace facebook::velox::wave::test
