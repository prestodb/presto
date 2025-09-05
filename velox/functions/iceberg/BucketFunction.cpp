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

#include "velox/functions/iceberg/BucketFunction.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/iceberg/Murmur3Hash32.h"

namespace facebook::velox::functions::iceberg {
namespace {

FOLLY_ALWAYS_INLINE int32_t
getBucketIndex(int32_t numBuckets, int32_t hashedValue) {
  return (hashedValue & std::numeric_limits<int32_t>::max()) % numBuckets;
}

// bucket(numBuckets, decimal) -> bucketIndex
// Hash the minimal bytes of the decimal unscaled value, then MOD numBuckets to
// get the bucket index.
template <typename TExec>
struct BucketDecimalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE Status call(int32_t& out, int32_t numBuckets, T input) {
    VELOX_USER_RETURN_LE(numBuckets, 0, "Invalid number of buckets.");
    char bytes[sizeof(int128_t)];
    const auto length = DecimalUtil::toByteArray(input, bytes);
    const auto hash = Murmur3Hash32::hashBytes(bytes, length);
    out = getBucketIndex(numBuckets, hash);
    return Status::OK();
  }
};

// bucket(numBuckets, input) -> bucketIndex
// Bucket all other Iceberg supported types including the integral type,
// varchar, varbinary, date and timestamp type.
template <typename TExec>
struct BucketFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // Value of type INTEGER and BIGINT is treated as unsigned type.
  // For the schema evolution, may promote int to int64, treat int32 as uint64.
  template <typename T>
  FOLLY_ALWAYS_INLINE Status call(int32_t& out, int32_t numBuckets, T input) {
    VELOX_USER_RETURN_LE(numBuckets, 0, "Invalid number of buckets.");
    const auto hash = Murmur3Hash32::hashInt64(input);
    out = getBucketIndex(numBuckets, hash);
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status
  call(int32_t& out, int32_t numBuckets, const arg_type<Varchar>& input) {
    VELOX_USER_RETURN_LE(numBuckets, 0, "Invalid number of buckets.");
    const auto hash = Murmur3Hash32::hashBytes(input.data(), input.size());
    out = getBucketIndex(numBuckets, hash);
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status
  call(int32_t& out, int32_t numBuckets, const arg_type<Timestamp>& input) {
    VELOX_USER_RETURN_LE(numBuckets, 0, "Invalid number of buckets.");
    const auto hash = Murmur3Hash32::hashInt64(input.toMicros());
    out = getBucketIndex(numBuckets, hash);
    return Status::OK();
  }
};

} // namespace

void registerBucketFunctions(const std::string& prefix) {
  registerFunction<BucketFunction, int32_t, int32_t, int32_t>(
      {prefix + "bucket"});
  registerFunction<BucketFunction, int32_t, int32_t, int64_t>(
      {prefix + "bucket"});
  registerFunction<BucketFunction, int32_t, int32_t, Varchar>(
      {prefix + "bucket"});
  registerFunction<BucketFunction, int32_t, int32_t, Varbinary>(
      {prefix + "bucket"});
  registerFunction<BucketFunction, int32_t, int32_t, Date>({prefix + "bucket"});
  registerFunction<BucketFunction, int32_t, int32_t, Timestamp>(
      {prefix + "bucket"});

  registerFunction<
      BucketDecimalFunction,
      int32_t,
      int32_t,
      LongDecimal<P1, S1>>({prefix + "bucket"});

  registerFunction<
      BucketDecimalFunction,
      int32_t,
      int32_t,
      ShortDecimal<P1, S1>>({prefix + "bucket"});
}

} // namespace facebook::velox::functions::iceberg
