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

namespace facebook::velox::dwio::common {

struct StripeProgress {
  uint32_t stripeIndex{0};
  uint64_t stripeRowCount{0};
  int64_t totalMemoryUsage{0};
  // hide first stripe special case in customer side.
  int64_t stripeSizeEstimate{0};
};

// Specific formats can extend this interface to do additional
// checks and customize how the decisions are combined.
class FlushPolicy {
 public:
  virtual ~FlushPolicy() = default;
  virtual bool shouldFlush(const StripeProgress& stripeProgress) = 0;
  // The flush policy might need to trigger external side effects upon
  // writer close. e.g. signaling to a coorditor of concurrent writers.
  virtual void onClose() = 0;
};

} // namespace facebook::velox::dwio::common
