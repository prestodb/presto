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

#include "velox/functions/prestosql/types/fuzzer_utils/TimestampWithTimeZoneInputGenerator.h"

#include "velox/common/fuzzer/Utils.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Variant.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::fuzzer {
namespace {
// We exclude certain time zones that may be inconsistent between Presto and
// Velox. These are primarily time zones that have been downgraded to links or
// otherwise had their histories corrected/updated, or have had recent updates
// (e.g. no longer using daylight savings time). The time zone information in
// the versions of Java and Joda may not have gotten these updates.
std::vector<int16_t> excludeProblematicTimeZoneIds(
    std::vector<int16_t> timeZoneIds) {
  static const std::unordered_set<int16_t> kExcludedTimeZones = {
      1110, 1682, 1683, 1685, 1686, 1687, 1688, 1689, 1691, 1692, 1693, 1697,
      1698, 1699, 1700, 1701, 1703, 1704, 1705, 1707, 1708, 1710, 1711, 1713,
      1714, 1715, 1716, 1717, 1718, 1719, 1720, 1721, 1722, 1726, 1727, 1728,
      1729, 1730, 1731, 1737, 1738, 1753, 1754, 1755, 1757, 1758, 1762, 1769,
      1773, 1775, 1776, 1780, 1786, 1790, 1796, 1797, 1798, 1804, 1816, 1822,
      1827, 1831, 1834, 1837, 1839, 1842, 1845, 1846, 1854, 1860, 1871, 1879,
      1881, 1882, 1883, 1884, 1889, 1891, 1893, 1900, 1903, 1906, 1907, 1908,
      1909, 1910, 1919, 1924, 1926, 1933, 1955, 1957, 1962, 1963, 1969, 1994,
      1999, 2005, 2006, 2007, 2009, 2049, 2060, 2063, 2065, 2067, 2070, 2071,
      2073, 2078, 2081, 2089, 2091, 2093, 2098, 2104, 2107, 2109, 2110, 2111,
      2112, 2113, 2116, 2117, 2118, 2119, 2120, 2124, 2130, 2140, 2142, 2150,
      2151, 2153, 2154, 2159, 2160, 2161, 2162, 2179, 2176, 2177, 2181, 2188,
      2195, 2197, 2202, 2204, 2209, 2210, 2233,
  };

  timeZoneIds.erase(
      std::remove_if(
          timeZoneIds.begin(),
          timeZoneIds.end(),
          [](int16_t timeZoneId) {
            return kExcludedTimeZones.count(timeZoneId) > 0;
          }),
      timeZoneIds.end());

  return timeZoneIds;
}
} // namespace

TimestampWithTimeZoneInputGenerator::TimestampWithTimeZoneInputGenerator(
    size_t seed,
    double nullRatio)
    : AbstractInputGenerator(
          seed,
          TIMESTAMP_WITH_TIME_ZONE(),
          nullptr,
          nullRatio),
      timeZoneIds_(excludeProblematicTimeZoneIds(tz::getTimeZoneIDs())) {}

variant TimestampWithTimeZoneInputGenerator::generate() {
  if (coinToss(rng_, nullRatio_)) {
    return variant::null(type_->kind());
  }

  int16_t timeZoneId =
      timeZoneIds_[rand<size_t>(rng_, 0, timeZoneIds_.size() - 1)];

  return pack(rand<int64_t>(rng_, kMinMillisUtc, kMaxMillisUtc), timeZoneId);
}
} // namespace facebook::velox::fuzzer
