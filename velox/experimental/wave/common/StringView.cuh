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

#include "velox/experimental/wave/common/StringView.h"

namespace facebook::velox::wave {

__device__ StringView StringView::cas(StringView compare, StringView val) {
  StringView old;
  old.data_ = atomicCAS(&data_, compare.data_, val.data_);
  return old;
}

} // namespace facebook::velox::wave
