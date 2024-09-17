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

#include <memory>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

/// Used to dynamically cast a unique pointer from 'SourceType' to
/// 'DestinationType'. The raw object is moved from 'srcPtr' to 'dstPtr' on
/// success. The function throws if the cast fails, and the raw object is also
/// freed.
template <typename SourceType, typename DestinationType>
inline void castUniquePointer(
    std::unique_ptr<SourceType>&& srcPtr,
    std::unique_ptr<DestinationType>& dstPtr) {
  auto* rawSrcPtr = srcPtr.release();
  VELOX_CHECK_NOT_NULL(rawSrcPtr);
  try {
    auto* rawDstPtr = dynamic_cast<DestinationType*>(rawSrcPtr);
    VELOX_CHECK_NOT_NULL(rawDstPtr);
    dstPtr.reset(rawDstPtr);
  } catch (const std::exception& e) {
    srcPtr.reset(rawSrcPtr);
    throw;
  }
}
} // namespace facebook::velox
