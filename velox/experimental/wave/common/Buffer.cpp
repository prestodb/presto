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

#include "velox/experimental/wave/common/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/experimental/wave/common/Exception.h"
#include "velox/experimental/wave/common/GpuArena.h"

DECLARE_bool(wave_buffer_end_guard);

namespace facebook::velox::wave {

void Buffer::check() const {
  if (!FLAGS_wave_buffer_end_guard) {
    return;
  }
  if (*magicPtr() != kMagic) {
    VELOX_FAIL("Buffer tail overrun: {}", toString());
  }
}

void Buffer::setMagic() {
  if (!FLAGS_wave_buffer_end_guard) {
    return;
  }
  *magicPtr() = kMagic;
}

std::string Buffer::toString() const {
  return fmt::format(
      "<Buffer {} capacity={} ref={} pin={} dbg={}>",
      ptr_,
      capacity_,
      referenceCount_,
      pinCount_,
      debugInfo_);
}

void Buffer::release() {
  check();
  if (referenceCount_.fetch_sub(1) == 1) {
    arena_->free(this);
  }
}

} // namespace facebook::velox::wave
