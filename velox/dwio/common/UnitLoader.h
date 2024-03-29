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

#include <cstdint>
#include <memory>
#include <vector>

namespace facebook::velox::dwio::common {

class LoadUnit {
 public:
  virtual ~LoadUnit() = default;

  // Perform the IO (read)
  virtual void load() = 0;

  // Unload the unit to free memory
  virtual void unload() = 0;

  // Number of rows in the unit
  virtual uint64_t getNumRows() = 0;

  // Number of bytes that the IO will read
  virtual uint64_t getIoSize() = 0;
};

class UnitLoader {
 public:
  virtual ~UnitLoader() = default;

  // Must block until the unit is loaded.
  // This call could unload other units. So the returned LoadUnit& is only
  // guaranteed to remain loaded until the next call
  virtual LoadUnit& getLoadedUnit(uint32_t unit) = 0;

  // Reader reports progress calling this method
  virtual void
  onRead(uint32_t unit, uint64_t rowOffsetInUnit, uint64_t rowCount) = 0;
};

class UnitLoaderFactory {
 public:
  virtual ~UnitLoaderFactory() = default;
  virtual std::unique_ptr<UnitLoader> create(
      std::vector<std::unique_ptr<LoadUnit>> loadUnits) = 0;
};

} // namespace facebook::velox::dwio::common
