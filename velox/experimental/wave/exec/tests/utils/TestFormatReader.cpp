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
  if (!staged_) {
    staged_ = true;
    Staging staging;
    staging.hostData = column_->values->as<char>();
    staging.size = column_->values->size();
    id = splitStaging.add(staging);
  }
  if (!queued_) {
    queued_ = true;
    auto step = std::make_unique<GpuDecode>();
    if (op.waveVector) {
      op.waveVector->resize(op.rows.size(), false);
    }
    auto columnKind = static_cast<WaveTypeKind>(column_->kind);
    if (column_->encoding == Encoding::kFlat) {
      if (column_->baseline == 0 &&
          (column_->bitWidth == 32 || column_->bitWidth == 64)) {
        step->step = DecodeStep::kTrivial;
        step->data.trivial.dataType = columnKind;
        step->data.trivial.input = 0;
        step->data.trivial.begin = currentRow_;
        step->data.trivial.end = currentRow_ + op.rows.back() + 1;
        step->data.trivial.input = nullptr;
        if (id != kNoBufferId) {
          splitStaging.registerPointer(id, &step->data.trivial.input);
          splitStaging.registerPointer(id, &deviceBuffer_);
        } else {
          step->data.trivial.input = deviceBuffer_;
        }
        step->data.trivial.result = op.waveVector->values<char>();
      } else {
        step->step = DecodeStep::kDictionaryOnBitpack;
        // Just bit pack, no dictionary.
        step->data.dictionaryOnBitpack.alphabet = nullptr;
        step->data.dictionaryOnBitpack.dataType = columnKind;
        step->data.dictionaryOnBitpack.baseline = column_->baseline;
        step->data.dictionaryOnBitpack.bitWidth = column_->bitWidth;
        step->data.dictionaryOnBitpack.indices = nullptr;
        step->data.dictionaryOnBitpack.begin = currentRow_;
        step->data.dictionaryOnBitpack.end = currentRow_ + op.rows.back() + 1;
        if (id != kNoBufferId) {
          splitStaging.registerPointer(
              id, &step->data.dictionaryOnBitpack.indices);
          splitStaging.registerPointer(id, &deviceBuffer_);
        } else {
          step->data.dictionaryOnBitpack.indices =
              reinterpret_cast<uint64_t*>(deviceBuffer_);
        }
        step->data.dictionaryOnBitpack.result = op.waveVector->values<char>();
      }
    } else {
      VELOX_NYI("Non flat test encoding");
    }
    op.isFinal = true;
    std::vector<std::unique_ptr<GpuDecode>> steps;
    steps.push_back(std::move(step));
    program.programs.push_back(std::move(steps));
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
