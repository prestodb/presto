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

#include "velox/exec/BlockingReason.h"

namespace facebook::velox::exec {

namespace {
const auto& blockingReasonNames() {
  static const folly::F14FastMap<BlockingReason, std::string_view> kNames = {
      {BlockingReason::kNotBlocked, "kNotBlocked"},
      {BlockingReason::kWaitForConsumer, "kWaitForConsumer"},
      {BlockingReason::kWaitForSplit, "kWaitForSplit"},
      {BlockingReason::kWaitForProducer, "kWaitForProducer"},
      {BlockingReason::kWaitForJoinBuild, "kWaitForJoinBuild"},
      {BlockingReason::kWaitForJoinProbe, "kWaitForJoinProbe"},
      {BlockingReason::kWaitForMergeJoinRightSide,
       "kWaitForMergeJoinRightSide"},
      {BlockingReason::kWaitForMemory, "kWaitForMemory"},
      {BlockingReason::kWaitForConnector, "kWaitForConnector"},
      {BlockingReason::kYield, "kYield"},
      {BlockingReason::kWaitForArbitration, "kWaitForArbitration"},
      {BlockingReason::kWaitForScanScaleUp, "kWaitForScanScaleUp"},
      {BlockingReason::kWaitForIndexLookup, "kWaitForIndexLookup"},
  };
  return kNames;
}

} // namespace

VELOX_DEFINE_ENUM_NAME(BlockingReason, blockingReasonNames)

} // namespace facebook::velox::exec
