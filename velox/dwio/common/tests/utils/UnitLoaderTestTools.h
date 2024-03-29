// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

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
      std::vector<bool>& unitsLoaded,
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
  std::vector<bool>& unitsLoaded_;
  size_t unitId_;
};

class ReaderMock {
 public:
  ReaderMock(
      std::vector<uint64_t> rowsPerUnit,
      std::vector<uint64_t> ioSizes,
      UnitLoaderFactory& factory);

  bool read(uint64_t maxRows);

  const std::vector<bool>& unitsLoaded() const {
    return unitsLoaded_;
  }

 private:
  bool loadUnit();

  std::vector<std::unique_ptr<LoadUnit>> getUnits();

  std::vector<uint64_t> rowsPerUnit_;
  std::vector<uint64_t> ioSizes_;
  std::vector<bool> unitsLoaded_;
  std::unique_ptr<UnitLoader> loader_;
  size_t currentUnit_{0};
  size_t currentRowInUnit_{0};
  std::optional<size_t> lastUnitLoaded_;
};

} // namespace facebook::velox::dwio::common::test
