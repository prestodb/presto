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

#include "velox/experimental/wave/dwio/nimble/SelectiveStructColumnReader.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/StructColumnReader.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFormatData.h"

namespace facebook::velox::wave::nimble {

SelectiveStructColumnReader::SelectiveStructColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    NimbleFormatParams& params,
    common::ScanSpec& scanSpec,
    std::vector<std::unique_ptr<AbstractOperand>>&& operands,
    bool isRoot)
    : StructColumnReader(
          requestedType,
          fileType,
          operands[0].get(),
          params,
          scanSpec,
          isRoot),
      operands_(std::move(operands)) {
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
    auto childParams = NimbleFormatParams(
        params.pool(), params.runtimeStatistics(), params.stripe());

    auto childOperand = std::make_unique<AbstractOperand>(
        i + 1, childRequestedType, "c" + std::to_string(i));

    addChild(NimbleFormatReader::build(
        childRequestedType,
        childFileType,
        params,
        *childSpec /*, path, defines*/,
        childOperand.get()));
    childSpec->setSubscript(children_.size() - 1);
    operands_.push_back(std::move(childOperand));
  }
}

// static
std::unique_ptr<ColumnReader> NimbleFormatReader::build(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    NimbleFormatParams& params,
    common::ScanSpec& scanSpec,
    AbstractOperand* operand,
    bool isRoot) {
  VELOX_CHECK(isRoot || operand != nullptr, "Operand is null.");
  std::vector<std::unique_ptr<AbstractOperand>> operands;
  operands.push_back(nullptr); // root does not need an operand
  switch (fileType->type()->kind()) {
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return std::make_unique<ColumnReader>(
          requestedType, fileType, operand, params, scanSpec);

    case TypeKind::ROW:
      return std::make_unique<SelectiveStructColumnReader>(
          requestedType,
          fileType,
          params,
          scanSpec,
          std::move(operands),
          isRoot);
    default:
      VELOX_UNREACHABLE();
  }
}
} // namespace facebook::velox::wave::nimble
