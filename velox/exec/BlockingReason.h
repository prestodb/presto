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

#include "velox/common/Enums.h"

namespace facebook::velox::exec {

enum class BlockingReason {
  kNotBlocked,
  kWaitForConsumer,
  kWaitForSplit,
  /// Some operators can get blocked due to the producer(s) (they are
  /// currently waiting data from) not having anything produced. Used by
  /// LocalExchange, LocalMergeExchange, Exchange and MergeExchange operators.
  kWaitForProducer,
  kWaitForJoinBuild,
  /// For a build operator, it is blocked waiting for the probe operators to
  /// finish probing before build the next hash table from one of the
  /// previously spilled partition data. For a probe operator, it is blocked
  /// waiting for all its peer probe operators to finish probing before
  /// notifying the build operators to build the next hash table from the
  /// previously spilled data.
  kWaitForJoinProbe,
  /// Used by MergeJoin operator, indicating that it was blocked by the right
  /// side input being unavailable.
  kWaitForMergeJoinRightSide,
  kWaitForMemory,
  kWaitForConnector,
  /// Some operators (like Table Scan) may run long loops and can 'voluntarily'
  /// exit them because Task requested to yield or stop or after a certain time.
  /// This is the blocking reason used in such cases.
  kYield,
  /// Operator is blocked waiting for its associated query memory arbitration to
  /// finish.
  kWaitForArbitration,
  /// For a table scan operator, it is blocked waiting for the scan controller
  /// to increase the number of table scan processing threads to start
  /// processing.
  kWaitForScanScaleUp,
  /// Used by IndexLookupJoin operator, indicating that it was blocked by the
  /// async index lookup.
  kWaitForIndexLookup,
};

VELOX_DECLARE_ENUM_NAME(BlockingReason);
} // namespace facebook::velox::exec

template <>
struct fmt::formatter<facebook::velox::exec::BlockingReason>
    : formatter<std::string_view> {
  auto format(facebook::velox::exec::BlockingReason b, format_context& ctx)
      const {
    return formatter<std::string_view>::format(
        facebook::velox::exec::BlockingReasonName::toName(b), ctx);
  }
};
