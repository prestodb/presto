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

#include "velox/dwio/common/tests/utils/UnitLoaderTestTools.h"

using facebook::velox::dwio::common::LoadUnit;

namespace facebook::velox::dwio::common::test {

ReaderMock::ReaderMock(
    std::vector<uint64_t> rowsPerUnit,
    std::vector<uint64_t> ioSizes,
    UnitLoaderFactory& factory)
    : rowsPerUnit_{std::move(rowsPerUnit)},
      ioSizes_{std::move(ioSizes)},
      unitsLoaded_(std::vector<std::atomic_bool>(rowsPerUnit_.size())),
      loader_{factory.create(getUnits())} {
  VELOX_CHECK(rowsPerUnit_.size() == ioSizes_.size());
}

bool ReaderMock::read(uint64_t maxRows) {
  if (!loadUnit()) {
    return false;
  }
  const auto rowsToRead =
      std::min(maxRows, rowsPerUnit_[currentUnit_] - currentRowInUnit_);
  loader_->onRead(currentUnit_, currentRowInUnit_, rowsToRead);
  currentRowInUnit_ += rowsToRead;
  return true;
}

bool ReaderMock::loadUnit() {
  VELOX_CHECK(currentRowInUnit_ <= rowsPerUnit_[currentUnit_]);
  if (currentRowInUnit_ == rowsPerUnit_[currentUnit_]) {
    currentRowInUnit_ = 0;
    ++currentUnit_;
    if (currentUnit_ >= rowsPerUnit_.size()) {
      return false;
    }
  }
  if (currentRowInUnit_ == 0) {
    auto& unit = loader_->getLoadedUnit(currentUnit_);
    auto& unitMock = dynamic_cast<LoadUnitMock&>(unit);
    VELOX_CHECK(unitMock.isLoaded());
  }
  return true;
}

std::vector<std::unique_ptr<LoadUnit>> ReaderMock::getUnits() {
  std::vector<std::unique_ptr<LoadUnit>> units;
  for (size_t i = 0; i < rowsPerUnit_.size(); ++i) {
    units.emplace_back(std::make_unique<LoadUnitMock>(
        rowsPerUnit_[i], ioSizes_[i], unitsLoaded_, i));
  }
  return units;
}

std::vector<std::atomic_bool> getUnitsLoadedWithFalse(size_t count) {
  std::vector<std::atomic_bool> unitsLoaded(count);
  for (auto& unit : unitsLoaded) {
    unit = false;
  }
  return unitsLoaded;
}

} // namespace facebook::velox::dwio::common::test
