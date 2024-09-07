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

#include "velox/dwio/dwrf/reader/StreamLabels.h"

namespace facebook::velox::dwrf {

std::string_view StreamLabels::label() const {
  return label_.empty() ? "/" : label_;
}

StreamLabels StreamLabels::append(std::string_view suffix) const {
  const size_t bufSize = label_.size() + suffix.size() + 1;
  auto* buffer = pool_.allocateFixed(bufSize);
  if (!label_.empty()) {
    ::memcpy(buffer, label_.data(), label_.size());
  }
  buffer[label_.size()] = '/';
  if (!suffix.empty()) {
    ::memcpy(buffer + label_.size() + 1, suffix.data(), suffix.size());
  }
  return StreamLabels(pool_, {buffer, bufSize});
}

} // namespace facebook::velox::dwrf
