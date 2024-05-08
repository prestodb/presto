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

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/UnitLoader.h"

namespace facebook::velox::dwio::common::test {

class LoadUnitMock : public LoadUnit {
 public:
  LoadUnitMock(
      uint64_t rowCount,
      uint64_t ioSize,
      std::vector<std::atomic_bool>& unitsLoaded,
      size_t unitId)
      : rowCount_{rowCount},
        ioSize_{ioSize},
        unitsLoaded_{unitsLoaded},
        unitId_{unitId} {}

  ~LoadUnitMock() override = default;

  void load() override {
    VELOX_CHECK(!isLoaded());
    unitsLoaded_[unitId_] = true;
  }

  void unload() override {
    VELOX_CHECK(isLoaded());
    unitsLoaded_[unitId_] = false;
  }

  uint64_t getNumRows() override {
    return rowCount_;
  }

  uint64_t getIoSize() override {
    return ioSize_;
  }

  bool isLoaded() const {
    return unitsLoaded_[unitId_];
  }

 private:
  uint64_t rowCount_;
  uint64_t ioSize_;
  std::vector<std::atomic_bool>& unitsLoaded_;
  size_t unitId_;
};

class ReaderMock {
 public:
  ReaderMock(
      std::vector<uint64_t> rowsPerUnit,
      std::vector<uint64_t> ioSizes,
      UnitLoaderFactory& factory,
      uint64_t rowsToSkip);

  bool read(uint64_t maxRows);

  void seek(uint64_t rowNumber);

  std::vector<bool> unitsLoaded() const {
    return {unitsLoaded_.begin(), unitsLoaded_.end()};
  }

 private:
  bool loadUnit();

  std::vector<std::unique_ptr<LoadUnit>> getUnits();

  std::vector<uint64_t> rowsPerUnit_;
  std::vector<uint64_t> ioSizes_;
  std::vector<std::atomic_bool> unitsLoaded_;
  std::unique_ptr<UnitLoader> loader_;
  size_t currentUnit_;
  size_t currentRowInUnit_;
  std::optional<size_t> lastUnitLoaded_;
};

std::vector<std::atomic_bool> getUnitsLoadedWithFalse(size_t count);

} // namespace facebook::velox::dwio::common::test
