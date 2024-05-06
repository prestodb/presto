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

#include "velox/experimental/wave/dwio/ColumnReader.h"

namespace facebook::velox::wave {

void ColumnReader::makeOp(
    ReadStream* readStream,
    ColumnAction action,
    int32_t offset,
    RowSet rows,
    ColumnOp& op) {
  VELOX_CHECK(action == ColumnAction::kValues, "Only values supported");
  formatData_->newBatch(readOffset_ + offset);
  op.action = action;
  op.reader = this;
  readStream->setNullable(*operand_, formatData_->hasNulls());
  op.waveVector = readStream->operandVector(operand_->id, requestedType_);
  op.rows = rows;
  readOffset_ = offset + rows.back() + 1;
};

} // namespace facebook::velox::wave
