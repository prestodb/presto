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

#include <chrono>
#include <functional>

#include "velox/dwio/common/UnitLoader.h"

namespace facebook::velox::dwio::common {

class OnDemandUnitLoaderFactory
    : public velox::dwio::common::UnitLoaderFactory {
 public:
  explicit OnDemandUnitLoaderFactory(
      std::function<void(std::chrono::high_resolution_clock::duration)>
          blockedOnIoCallback)
      : blockedOnIoCallback_{std::move(blockedOnIoCallback)} {}
  ~OnDemandUnitLoaderFactory() override = default;

  std::unique_ptr<velox::dwio::common::UnitLoader> create(
      std::vector<std::unique_ptr<velox::dwio::common::LoadUnit>> loadUnits)
      override;

 private:
  std::function<void(std::chrono::high_resolution_clock::duration)>
      blockedOnIoCallback_;
};

} // namespace facebook::velox::dwio::common
