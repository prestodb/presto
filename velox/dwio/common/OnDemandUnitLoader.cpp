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

#include "velox/dwio/common/OnDemandUnitLoader.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/MeasureTime.h"
#include "velox/dwio/common/UnitLoaderTools.h"

using facebook::velox::dwio::common::measureTimeIfCallback;

namespace facebook::velox::dwio::common {

namespace {

class OnDemandUnitLoader : public UnitLoader {
 public:
  OnDemandUnitLoader(
      std::vector<std::unique_ptr<LoadUnit>> loadUnits,
      std::function<void(std::chrono::high_resolution_clock::duration)>
          blockedOnIoCallback)
      : loadUnits_{std::move(loadUnits)},
        blockedOnIoCallback_{std::move(blockedOnIoCallback)} {}

  ~OnDemandUnitLoader() override = default;

  LoadUnit& getLoadedUnit(uint32_t unit) override {
    VELOX_CHECK(unit < loadUnits_.size(), "Unit out of range");

    if (loadedUnit_) {
      if (*loadedUnit_ == unit) {
        return *loadUnits_[unit];
      } else {
        loadUnits_[*loadedUnit_]->unload();
        loadedUnit_.reset();
      }
    }

    {
      auto measure = measureTimeIfCallback(blockedOnIoCallback_);
      loadUnits_[unit]->load();
    }
    loadedUnit_ = unit;

    return *loadUnits_[unit];
  }

  void onRead(
      uint32_t /* unit */,
      uint64_t /* rowOffsetInUnit */,
      uint64_t /* rowCount */) override {}

 private:
  std::vector<std::unique_ptr<LoadUnit>> loadUnits_;
  std::function<void(std::chrono::high_resolution_clock::duration)>
      blockedOnIoCallback_;
  std::optional<uint32_t> loadedUnit_;
};

} // namespace

std::unique_ptr<UnitLoader> OnDemandUnitLoaderFactory::create(
    std::vector<std::unique_ptr<LoadUnit>> loadUnits) {
  return std::make_unique<OnDemandUnitLoader>(
      std::move(loadUnits), blockedOnIoCallback_);
}

} // namespace facebook::velox::dwio::common
