/*
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
#include <re2/re2.h>

namespace facebook::presto::protocol {

enum class DataUnit { BYTE, KILOBYTE, MEGABYTE, GIGABYTE, TERABYTE, PETABYTE };

class DataSizeDataUnitUnsupported : public std::exception {};
class DataSizeStringInvalid : public std::exception {};

class DataSize {
 public:
  DataSize() = default;

  explicit DataSize(const std::string& string);

  DataSize(double value, DataUnit dataUnit)
      : value_(value), dataUnit_(dataUnit) {}

  double getValue() const {
    return value_;
  }

  DataUnit getDataUnit() const {
    return dataUnit_;
  }

  std::string toString() const;

  double getValue(DataUnit dataUnit) const {
    return value_ *
        (toBytesPerDataUnit(this->dataUnit_) / toBytesPerDataUnit(dataUnit));
  }

  static double toBytesPerDataUnit(DataUnit dataUnit);

  DataUnit valueOfDataUnit(const std::string& dataUnitString) const;

  std::string dataUnitToString(DataUnit dataUnit) const;

 private:
  double value_ = 0;
  DataUnit dataUnit_ = DataUnit::BYTE;
};

} // namespace facebook::presto::protocol
