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

#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::dwrf {

class NullColumnReader : public ColumnReader {
 public:
  NullColumnReader(
      const StripeStreams& stripe,
      const std::shared_ptr<const Type>& type)
      : ColumnReader(
            stripe.getMemoryPool(),
            dwio::common::TypeWithId::create(type)) {}
  ~NullColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override {
    return numValues;
  }

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override {
    if (result && result->encoding() == VectorEncoding::Simple::CONSTANT &&
        result->isNullAt(0)) {
      // If vector already exists and contains the right value, resize.
      result->resize(numValues);
    } else {
      auto valueVector = BaseVector::create(fileType_->type(), 1, &memoryPool_);
      valueVector->setNull(0, true);
      result = BaseVector::wrapInConstant(numValues, 0, valueVector);
    }
  }
};

} // namespace facebook::velox::dwrf
