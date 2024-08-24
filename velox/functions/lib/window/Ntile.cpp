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

#include "velox/common/base/Exceptions.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::window {

namespace {

template <typename TResult>
class NtileFunction : public exec::WindowFunction {
 public:
  explicit NtileFunction(
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      velox::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool, nullptr) {
    if (args[0].constantValue) {
      auto argBuckets = args[0].constantValue;
      if (!argBuckets->isNullAt(0)) {
        numFixedBuckets_ =
            argBuckets->as<ConstantVector<TResult>>()->valueAt(0);
        VELOX_USER_CHECK_GE(
            numFixedBuckets_.value(), 1, "{}", kBucketErrorString);
      }
      return;
    }

    bucketColumn_ = args[0].index;
    bucketVector_ = BaseVector::create(resultType_, 0, pool);
    bucketFlatVector_ = bucketVector_->asFlatVector<TResult>();
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
    partitionOffset_ = 0;
    numPartitionRows_ = partition->numRows();

    if (numFixedBuckets_.has_value()) {
      auto numBuckets = numFixedBuckets_.value();
      // If there are more buckets than partition rows, then the output bucket
      // number is the same as the row number so no further computation is
      // needed.
      fixedBucketsMoreThanPartition_ = numBuckets > numPartitionRows_;
      if (!fixedBucketsMoreThanPartition_) {
        fixedBucketMetrics_ = computeBucketMetrics(numBuckets);
      }
    }
  }

  void apply(
      const BufferPtr& peerGroupStarts,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& /*frameStarts*/,
      const BufferPtr& /*frameEnds*/,
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    int numRows = peerGroupStarts->size() / sizeof(vector_size_t);

    if (bucketColumn_.has_value()) {
      computeNtileFromColumn(numRows, resultOffset, result);
    } else {
      computeNtileWithConstants(numRows, resultOffset, result);
    }

    partitionOffset_ += numRows;
  }

 private:
  // These are some intermediate values required for bucket computation when the
  // number of rows in the partition exceeds the number of buckets.
  struct BucketMetrics {
    // To compute the bucket number for a row, we find the number of rows in
    // a bucket as the (number of rows in partition) / (number of buckets).
    TResult rowsPerBucket;
    // There could be some buckets with rowsPerBucket + 1 number of rows,
    // as the partition rows might not be exactly divisible
    // by the number of buckets. There are
    // (number of rows in partition) % (number of buckets) such buckets.
    TResult bucketsWithExtraRow;
    // When assigning bucket numbers, the first 'bucketsWithExtraRow' buckets
    // will have (rowsPerBucket + 1) rows. This row number at this boundary is
    // extraBucketsBoundary = bucketsWithExtraRow * (rowsPerBucket + 1). Beyond
    // this row number in the partition, the buckets will have only
    // rowsPerBucket number of rows. This boundary is useful when computing the
    // bucket value.
    TResult extraBucketsBoundary;

    TResult computeBucketValue(vector_size_t rowNumber) const {
      if (rowNumber < extraBucketsBoundary) {
        return rowNumber / (rowsPerBucket + 1) + 1;
      }
      return (rowNumber - bucketsWithExtraRow) / rowsPerBucket + 1;
    }

    // Compute the bucket value for a fixed bucket number for a vector
    // of rows. The vector starts at the partitionOffset index in the
    // partition rows.
    void computeBucketValue(
        vector_size_t numRows,
        int64_t partitionOffset,
        vector_size_t resultOffset,
        TResult* rawResultValues) {
      int64_t i = 0;
      // This loop terminates if it reaches extraBucketBoundary or numRows
      // in the result vector are filled.
      for (int64_t j = partitionOffset; i < numRows && j < extraBucketsBoundary;
           i++, j++) {
        rawResultValues[resultOffset + i] = j / (rowsPerBucket + 1) + 1;
      }
      for (; i < numRows; i++) {
        rawResultValues[resultOffset + i] =
            (partitionOffset + i - bucketsWithExtraRow) / rowsPerBucket + 1;
      }
    }
  };

  BucketMetrics computeBucketMetrics(TResult numBuckets) const {
    auto rowsPerBucket = numPartitionRows_ / numBuckets;
    auto bucketsWithExtraRow = numPartitionRows_ % numBuckets;
    auto extraBucketsBoundary = (rowsPerBucket + 1) * bucketsWithExtraRow;
    return {rowsPerBucket, bucketsWithExtraRow, extraBucketsBoundary};
  }

  void computeNtileFromColumn(
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    bucketVector_->resize(numRows);
    partition_->extractColumn(
        bucketColumn_.value(), partitionOffset_, numRows, 0, bucketVector_);

    auto* resultFlatVector = result->asFlatVector<TResult>();
    auto* rawValues = resultFlatVector->mutableRawValues();
    for (auto i = 0; i < numRows; i++) {
      if (bucketFlatVector_->isNullAt(i)) {
        resultFlatVector->setNull(resultOffset + i, true);
      } else {
        vector_size_t row = i + partitionOffset_;
        auto numBuckets = bucketFlatVector_->valueAt(i);
        VELOX_USER_CHECK_GE(numBuckets, 1, "{}", kBucketErrorString);
        auto bucketsMoreThanPartition = numBuckets > numPartitionRows_;
        if (bucketsMoreThanPartition) {
          rawValues[resultOffset + i] = row + 1;
        } else {
          rawValues[resultOffset + i] =
              computeBucketMetrics(numBuckets).computeBucketValue(row);
        }
      }
    }
  }

  void computeNtileWithConstants(
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    if (numFixedBuckets_.has_value()) {
      auto rawValues = result->asFlatVector<TResult>()->mutableRawValues();
      if (fixedBucketsMoreThanPartition_) {
        std::iota(
            rawValues + resultOffset,
            rawValues + resultOffset + numRows,
            partitionOffset_ + 1);
      } else {
        fixedBucketMetrics_.computeBucketValue(
            numRows, partitionOffset_, resultOffset, rawValues);
      }
    } else {
      // This is a function call with a constant null value. Set all result
      // rows to null.
      auto* resultVector = result->asFlatVector<TResult>();
      auto mutableRawNulls = resultVector->mutableRawNulls();
      bits::fillBits(
          mutableRawNulls, resultOffset, resultOffset + numRows, bits::kNull);
    }
  }

  // Index of the bucket column if the parameter is a field.
  std::optional<column_index_t> bucketColumn_;

  // Number of buckets if a constant value. Is optional as the value could
  // be null.
  std::optional<TResult> numFixedBuckets_;

  // If number of buckets is greater than the partition rows, then the output
  // bucket number is simply row number + 1. So bucket computation can be
  // skipped in this case.
  bool fixedBucketsMoreThanPartition_ = {true};

  // If the number of buckets is fixed and less than the number of rows in the
  // partition, then bucket metrics are precomputed once per partition
  // and reused across apply calls.
  BucketMetrics fixedBucketMetrics_;

  // Current WindowPartition used for accessing rows in the apply method.
  const exec::WindowPartition* partition_;
  TResult numPartitionRows_ = 0;

  // Denotes how far along the partition rows are output already.
  int64_t partitionOffset_ = 0;

  // Vector used to read the bucket column values.
  VectorPtr bucketVector_;
  FlatVector<TResult>* bucketFlatVector_;

  static const std::string kBucketErrorString;
};

template <typename TResult>
const std::string NtileFunction<TResult>::kBucketErrorString =
    "Buckets must be greater than 0";

} // namespace

template <typename TResult>
void registerNtile(const std::string& name, const std::string& type) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .returnType(type)
          .argumentType(type)
          .build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      exec::WindowFunction::Metadata::defaultMetadata(),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          bool /*ignoreNulls*/,
          velox::memory::MemoryPool* pool,
          HashStringAllocator* /*stringAllocator*/,
          const core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<NtileFunction<TResult>>(args, resultType, pool);
      });
}

void registerNtileBigint(const std::string& name) {
  registerNtile<int64_t>(name, "bigint");
}
void registerNtileInteger(const std::string& name) {
  registerNtile<int32_t>(name, "integer");
}

} // namespace facebook::velox::functions::window
