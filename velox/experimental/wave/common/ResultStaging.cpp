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

#include "velox/experimental/wave/common/ResultStaging.h"
#include "velox/common/base/BitUtil.h"
#include "velox/experimental/wave/common/GpuArena.h"

namespace facebook::velox::wave {

BufferId ResultStaging::reserve(int32_t bytes) {
  offsets_.push_back(fill_);
  fill_ += bits::roundUp(bytes, 8);
  return offsets_.size() - 1;
}

void ResultStaging::registerPointerInternal(
    BufferId id,
    void** pointer,
    bool clear) {
  VELOX_CHECK_LT(id, offsets_.size());
  VELOX_CHECK_NOT_NULL(pointer);
#ifndef NDEBUG
  for (auto& pair : patch_) {
    VELOX_CHECK(
        pair.second != pointer, "Must not register the same pointer twice");
  }
#endif
  if (clear) {
    *pointer = nullptr;
  }
  patch_.push_back(std::make_pair(id, pointer));
}

void ResultStaging::makeDeviceBuffer(GpuArena& arena) {
  if (fill_ == 0) {
    return;
  }
  WaveBufferPtr buffer = arena.allocate<char>(fill_);
  auto address = reinterpret_cast<int64_t>(buffer->as<char>());
  // Patch all the registered pointers to point to buffer at offset offset_[id]
  // + the offset already in the registered pointer.
  for (auto& pair : patch_) {
    int64_t* pointer = reinterpret_cast<int64_t*>(pair.second);
    *pointer += address + offsets_[pair.first];
  }
  patch_.clear();
  offsets_.clear();
  fill_ = 0;
  buffers_.push_back(std::move(buffer));
}

void ResultStaging::setReturnBuffer(GpuArena& arena, ResultBuffer& result) {
  if (fill_ == 0) {
    result.result = nullptr;
    result.hostResult = nullptr;
    return;
  }
  result.result = arena.allocate<char>(fill_);
  auto address = reinterpret_cast<int64_t>(result.result->as<char>());
  // Patch all the registered pointers to point to buffer at offset offset_[id]
  // + the offset already in the registered pointer.
  for (auto& pair : patch_) {
    int64_t* pointer = reinterpret_cast<int64_t*>(pair.second);
    *pointer += address + offsets_[pair.first];
  }
  patch_.clear();
  offsets_.clear();
  fill_ = 0;
}

} // namespace facebook::velox::wave
