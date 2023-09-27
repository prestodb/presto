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

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryArbitrator.h"

namespace facebook::velox::exec {
/// Provides the default memory reclaimer implementation for velox task
/// execution.
class DefaultMemoryReclaimer : public memory::MemoryReclaimer {
 public:
  virtual ~DefaultMemoryReclaimer() = default;

  static std::unique_ptr<MemoryReclaimer> create();

  void enterArbitration() override;

  void leaveArbitration() noexcept override;

 protected:
  DefaultMemoryReclaimer() = default;
};
} // namespace facebook::velox::exec
