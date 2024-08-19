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

#include <vector>

#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/Cuda.h"

namespace facebook::velox::wave {

struct ResultBuffer {
  void clear() {
    result = nullptr;
    hostResult = nullptr;
  }

  void transfer(Stream& stream) const {
    if (result) {
      if (!hostResult) {
        stream.prefetch(nullptr, result->as<char>(), result->size());
      } else {
        stream.deviceToHostAsync(
            hostResult->as<char>(), result->as<char>(), hostResult->size());
      }
    }
  }

  WaveBufferPtr result;
  WaveBufferPtr hostResult;
};

using BufferId = int32_t;
constexpr BufferId kNoBufferId = -1;

class ResultStaging {
 public:
  /// Reserves 'bytes' bytes in result buffer to be brought to host after
  /// the next kernel completes on device. The caller of the kernel calls
  /// transfer().
  BufferId reserve(int32_t bytes);

  /// Registers '*pointer' to be patched to the buffer. The starting address of
  /// the buffer is added to *pointer, so that if *pointer was 16, *pointer will
  /// come to point to the 16th byte in the buffer. If 'clear' is true, *ptr is
  /// set to nullptr first.
  template <typename T>
  void registerPointer(BufferId id, T pointer, bool clear) {
    registerPointerInternal(
        id,
        reinterpret_cast<void**>(reinterpret_cast<uint64_t>(pointer)),
        clear);
  }

  /// Creates a device side buffer for the reserved space and patches all the
  /// registered pointers to offsets inside the device side buffer.  Retains
  /// ownership of the device side buffer. Clears any reservations and
  /// registrations so that new ones can be reserved and registered. This cycle
  /// may repeat multiple times.  The device side buffers are freed on
  /// destruction.
  void makeDeviceBuffer(GpuArena& arena);

  void setReturnBuffer(GpuArena& arena, ResultBuffer& result);

 private:
  void registerPointerInternal(BufferId id, void** pointer, bool clear);

  // Offset of each result in either buffer.
  std::vector<int32_t> offsets_;
  // Patch addresses. The int64_t* is updated to point to the result buffer once
  // it is allocated.
  std::vector<std::pair<int32_t, void**>> patch_;
  int32_t fill_{0};
  WaveBufferPtr deviceBuffer_;
  WaveBufferPtr hostBuffer_;
  std::vector<WaveBufferPtr> buffers_;
};

/// Manages parameters of kernel launch. Provides pinned host buffer and a
/// device side copy destination.
struct LaunchParams {
  LaunchParams(GpuArena& arena) : arena(arena) {}

  /// Returns a host, device address pair of 'size' bytes.
  std::pair<char*, char*> setup(size_t size);

  void transfer(Stream& stream);

  GpuArena& arena;
  WaveBufferPtr host;
  WaveBufferPtr device;
};

// Arena for decode and program launch param blocks. Lifetime of allocations is
// a wave in WaveStream, typical size under 1MB, e.g. 1K blocks with 1K of
// params.
GpuArena& getSmallTransferArena();

} // namespace facebook::velox::wave
