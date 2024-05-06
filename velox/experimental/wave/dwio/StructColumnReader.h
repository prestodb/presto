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

namespace facebook ::velox::wave {

class StructColumnReader : public ColumnReader {
 public:
  StructColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      AbstractOperand* operand,
      FormatParams& params,
      velox::common::ScanSpec& scanSpec,
      bool isRoot)
      : ColumnReader(requestedType, fileType, operand, params, scanSpec),
        isRoot_(isRoot) {}

  bool isChildConstant(const velox::common::ScanSpec& childSpec) const;

 protected:
  void addChild(std::unique_ptr<ColumnReader> child) {
    children_.push_back(child.get());
    childrenOwned_.push_back(std::move(child));
  }

  const bool isRoot_;
  std::vector<std::unique_ptr<ColumnReader>> childrenOwned_;
};

} // namespace facebook::velox::wave
