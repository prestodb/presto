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

#include "velox/dwio/common/Range.h"

namespace facebook::velox::common {

Ranges Ranges::filter(std::function<bool(size_t)> func) const {
  Ranges ret;
  for (auto& r : ranges_) {
    bool inRun = false;
    size_t runStart = 0;
    for (auto cur = std::get<0>(r), end = std::get<1>(r); cur != end; ++cur) {
      if (func(cur)) {
        if (!inRun) {
          inRun = true;
          runStart = cur;
        }
      } else if (inRun) {
        ret.ranges_.emplace_back(runStart, cur);
        ret.size_ += (cur - runStart);
        inRun = false;
      }
    }
    if (inRun) {
      ret.ranges_.emplace_back(runStart, std::get<1>(r));
      ret.size_ += (std::get<1>(r) - runStart);
    }
  }
  return ret;
}

} // namespace facebook::velox::common
