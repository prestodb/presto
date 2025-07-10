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

#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/StructColumnReader.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFormatData.h"

namespace facebook::velox::wave::nimble {

class SelectiveStructColumnReader : public StructColumnReader {
 public:
  SelectiveStructColumnReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      NimbleFormatParams& params,
      common::ScanSpec& scanSpec,
      std::vector<std::unique_ptr<AbstractOperand>>&& operands,
      bool isRoot);

  const std::vector<std::unique_ptr<AbstractOperand>>* getOperands() const {
    return &operands_;
  }

 private:
  std::vector<std::unique_ptr<AbstractOperand>> operands_;
};

class NimbleFormatReader {
 public:
  static std::unique_ptr<ColumnReader> build(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      NimbleFormatParams& params,
      common::ScanSpec& scanSpec,
      AbstractOperand* operand,
      bool isRoot = false);
};

} // namespace facebook::velox::wave::nimble
