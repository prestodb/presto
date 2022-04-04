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
#include "presto_cpp/presto_protocol/DataSize.h"
#include <math.h>

namespace facebook::presto::protocol {

DataSize::DataSize(const std::string& string) {
  static const RE2 kPattern(R"(^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$)");
  double value;
  std::string unit;
  if (!RE2::FullMatch(string, kPattern, &value, &unit)) {
    throw DataSizeStringInvalid();
  }

  value_ = value;
  dataUnit_ = valueOfDataUnit(unit);
}

std::string DataSize::toString() const {
  char buffer[32];
  snprintf(
      buffer,
      sizeof(buffer),
      "%f%s",
      round(value_ * 100.0) / 100.0,
      dataUnitToString(dataUnit_).c_str());
  return std::string(buffer);
}

double DataSize::toBytesPerDataUnit(DataUnit dataUnit) {
  switch (dataUnit) {
    case DataUnit::BYTE:
      return 1;
    case DataUnit::KILOBYTE:
      return exp2(10);
    case DataUnit::MEGABYTE:
      return exp2(20);
    case DataUnit::GIGABYTE:
      return exp2(30);
    case DataUnit::TERABYTE:
      return exp2(40);
    case DataUnit::PETABYTE:
      return exp2(50);
    default:
      throw DataSizeDataUnitUnsupported();
  }
}

DataUnit DataSize::valueOfDataUnit(const std::string& dataUnitString) const {
  if (dataUnitString == "B") {
    return DataUnit::BYTE;
  }
  if (dataUnitString == "kB") {
    return DataUnit::KILOBYTE;
  }
  if (dataUnitString == "MB") {
    return DataUnit::MEGABYTE;
  }
  if (dataUnitString == "GB") {
    return DataUnit::GIGABYTE;
  }
  if (dataUnitString == "TB") {
    return DataUnit::TERABYTE;
  }
  if (dataUnitString == "PB") {
    return DataUnit::PETABYTE;
  }
  throw DataSizeDataUnitUnsupported();
}

std::string DataSize::dataUnitToString(DataUnit dataUnit) const {
  switch (dataUnit) {
    case DataUnit::BYTE:
      return "B";
    case DataUnit::KILOBYTE:
      return "kB";
    case DataUnit::MEGABYTE:
      return "MB";
    case DataUnit::GIGABYTE:
      return "GB";
    case DataUnit::TERABYTE:
      return "TB";
    case DataUnit::PETABYTE:
      return "PB";
    default:
      throw DataSizeDataUnitUnsupported();
  }
}

} // namespace facebook::presto::protocol
