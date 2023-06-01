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
#include "velox/exec/RowNumber.h"
#include "velox/exec/OperatorUtils.h"

namespace facebook::velox::exec {

RowNumber::RowNumber(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::RowNumberNode>& rowNumberNode)
    : Operator(
          driverCtx,
          rowNumberNode->outputType(),
          operatorId,
          rowNumberNode->id(),
          "RowNumber"),
      limit_{rowNumberNode->limit()} {
  const auto& inputType = rowNumberNode->sources()[0]->outputType();
  const auto& keys = rowNumberNode->partitionKeys();
  const auto numKeys = keys.size();

  if (numKeys > 0) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.reserve(numKeys);
    for (const auto& key : keys) {
      const auto channel = exprToChannel(key.get(), inputType);
      VELOX_USER_CHECK_NE(
          channel,
          kConstantChannel,
          "RowNumber operator doesn't allow constant partition keys");
      hashers.push_back(VectorHasher::create(key->type(), channel));
    }

    table_ = std::make_unique<HashTable<false>>(
        std::move(hashers),
        std::vector<std::unique_ptr<Aggregate>>{},
        std::vector<TypePtr>{BIGINT()},
        false, // allowDuplicates
        false, // isJoinBuild
        false, // hasProbedFlag
        pool());
    lookup_ = std::make_unique<HashLookup>(table_->hashers());

    const auto numRowsColumn = table_->rows()->columnAt(numKeys);
    numRowsOffset_ = numRowsColumn.offset();
  }

  identityProjections_.reserve(inputType->size());
  for (auto i = 0; i < inputType->size(); ++i) {
    identityProjections_.emplace_back(i, i);
  }

  resultProjections_.emplace_back(0, inputType->size());
  results_.resize(1);
}

void RowNumber::addInput(RowVectorPtr input) {
  const auto numInput = input->size();

  if (table_) {
    auto& hashers = lookup_->hashers;
    lookup_->reset(numInput);

    SelectivityVector rows(numInput);

    for (auto i = 0; i < hashers.size(); ++i) {
      auto key = input->childAt(hashers[i]->channel())->loadedVector();
      hashers[i]->decode(*key, rows);
    }

    const auto mode = table_->hashMode();
    bool rehash = false;
    for (auto i = 0; i < hashers.size(); ++i) {
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(rows, lookup_->hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(rows, i > 0, lookup_->hashes);
      }
    }

    if (rehash) {
      if (table_->hashMode() != BaseHashTable::HashMode::kHash) {
        table_->decideHashMode(input->size());
      }
      addInput(input);
      return;
    }

    std::iota(lookup_->rows.begin(), lookup_->rows.end(), 0);
    table_->groupProbe(*lookup_);

    // Initialize new partitions with zeros.
    for (auto i : lookup_->newGroups) {
      setNumRows(lookup_->hits[i], 0);
    }
  }

  input_ = std::move(input);
}

FlatVector<int64_t>& RowNumber::getOrCreateRowNumberVector(vector_size_t size) {
  VectorPtr& result = results_[0];
  if (result && result.unique()) {
    BaseVector::prepareForReuse(result, size);
  } else {
    result = BaseVector::create(BIGINT(), size, pool());
  }
  return *result->as<FlatVector<int64_t>>();
}

RowVectorPtr RowNumber::getOutput() {
  if (input_ == nullptr) {
    return nullptr;
  }

  if (!table_) {
    // No partition keys.
    return getOutputForSinglePartition();
  }

  const auto numInput = input_->size();

  BufferPtr mapping;
  vector_size_t* rawMapping;
  vector_size_t index = 0;
  if (limit_) {
    mapping = allocateIndices(numInput, pool());
    rawMapping = mapping->asMutable<vector_size_t>();
  }

  // Compute row numbers.
  auto& rowNumbers = getOrCreateRowNumberVector(numInput);

  for (auto i = 0; i < numInput; ++i) {
    auto* partition = lookup_->hits[i];
    const auto rowNumber = numRows(partition) + 1;

    if (limit_) {
      if (rowNumber > limit_) {
        // Exceeded the limit for this partition. Drop rows.
        continue;
      }
      rawMapping[index++] = i;
    }

    rowNumbers.set(i, rowNumber);
    setNumRows(partition, rowNumber);
  }

  RowVectorPtr output;
  if (limit_) {
    if (index == 0) {
      // Drop all rows.
      output = nullptr;
    } else {
      output = fillOutput(index, mapping);
    }
  } else {
    output = fillOutput(numInput, nullptr);
  }

  input_ = nullptr;
  return output;
}

RowVectorPtr RowNumber::getOutputForSinglePartition() {
  const auto numInput = input_->size();

  vector_size_t numOutput;
  if (limit_) {
    VELOX_CHECK_LT(numTotalInput_, limit_.value());
    numOutput =
        std::min<vector_size_t>(numInput, limit_.value() - numTotalInput_);

    if (numTotalInput_ + numOutput == limit_.value()) {
      finishedEarly_ = true;
    }
  } else {
    numOutput = numInput;
  }

  auto& rowNumbers = getOrCreateRowNumberVector(numOutput);

  for (auto i = 0; i < numOutput; ++i) {
    rowNumbers.set(i, ++numTotalInput_);
  }

  auto output = fillOutput(numOutput, nullptr);
  input_ = nullptr;
  return output;
}

int64_t RowNumber::numRows(char* partition) {
  return *reinterpret_cast<int64_t*>(partition + numRowsOffset_);
}

void RowNumber::setNumRows(char* partition, int64_t numRows) {
  *reinterpret_cast<int64_t*>(partition + numRowsOffset_) = numRows;
}
} // namespace facebook::velox::exec
