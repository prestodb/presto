// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "velox/dwio/common/tests/utils/UnitLoaderTestTools.h"

using facebook::velox::dwio::common::LoadUnit;

namespace facebook::velox::dwio::common::test {

ReaderMock::ReaderMock(
    std::vector<uint64_t> rowsPerUnit,
    std::vector<uint64_t> ioSizes,
    UnitLoaderFactory& factory)
    : rowsPerUnit_{std::move(rowsPerUnit)},
      ioSizes_{std::move(ioSizes)},
      unitsLoaded_(rowsPerUnit_.size(), false),
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

} // namespace facebook::velox::dwio::common::test
