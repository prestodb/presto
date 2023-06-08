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

#include "velox/common/memory/AllocationPool.h"

namespace facebook::velox::dwrf {

class StreamLabels {
 public:
  explicit StreamLabels(AllocationPool& pool) : pool_(pool) {}

  StreamLabels append(std::string_view suffix) const;

  std::string_view label() const;

 private:
  StreamLabels(AllocationPool& pool, std::string_view label)
      : pool_{pool}, label_{label} {}

  AllocationPool& pool_;
  std::string_view label_;
};

} // namespace facebook::velox::dwrf
