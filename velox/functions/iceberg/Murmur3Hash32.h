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

#include <cstdint>
#include "velox/functions/lib/Murmur3Hash32Base.h"

namespace facebook::velox::functions::iceberg {

/// This implementation should align with Iceberg bucket transform hash
/// algorithm which uses Guava hash
/// https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/Murmur3_32HashFunction.java,
/// Bucket partition transforms use a 32-bit hash of the source value. The
/// 32-bit hash implementation is the 32-bit Murmur3 hash, x86 variant, seeded
/// with 0. If not same with Iceberg java version, the partition will be
/// different and can not read with Iceberg java.
class Murmur3Hash32 : Murmur3Hash32Base {
 public:
  /// Value of type INTEGER and BIGINT is treated as unsigned type.
  /// For the schema evolution, promote int to int64, treat int32 as uint64.
  static int32_t hashInt64(uint64_t input);

  /// Hash the bytes every 4 bytes, XOR on remaining bytes. Processing for the
  /// remaining bytes is different with Spark murmur3 which combine with the
  /// remaining bytes.
  static int32_t hashBytes(const char* input, uint32_t len);
};

} // namespace facebook::velox::functions::iceberg
