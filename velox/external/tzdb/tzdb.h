// -*- C++ -*-
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

// For information see https://libcxx.llvm.org/DesignDocs/TimeZone.html

#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include "velox/external/tzdb/leap_second.h"
#include "velox/external/tzdb/time_zone.h"
#include "velox/external/tzdb/time_zone_link.h"

namespace facebook::velox::tzdb {

struct tzdb {
  std::string version;
  std::vector<time_zone> zones;
  std::vector<time_zone_link> links;

  std::vector<leap_second> leap_seconds;

  [[nodiscard]] const time_zone* __locate_zone(std::string_view __name) const {
    if (const time_zone* __result = __find_in_zone(__name))
      return __result;

    if (auto __it = std::lower_bound(
            links.begin(),
            links.end(),
            __name,
            [](const time_zone_link& link, const std::string_view& name) {
              return link.name() < name;
            });
        __it != links.end() && __it->name() == __name)
      if (const time_zone* __result = __find_in_zone(__it->target()))
        return __result;

    return nullptr;
  }

  [[nodiscard]] const time_zone* locate_zone(std::string_view __name) const {
    if (const time_zone* __result = __locate_zone(__name))
      return __result;

    std::__throw_runtime_error("tzdb: requested time zone not found");
  }

  [[nodiscard]] const time_zone* current_zone() const {
    return __current_zone();
  }

 private:
  const time_zone* __find_in_zone(std::string_view __name) const noexcept {
    if (auto __it = std::lower_bound(
            zones.begin(),
            zones.end(),
            __name,
            [](const time_zone& zone, const std::string_view& name) {
              return zone.name() < name;
            });
        __it != zones.end() && __it->name() == __name)
      return std::addressof(*__it);

    return nullptr;
  }

  [[nodiscard]] const time_zone* __current_zone() const;
};

} // namespace facebook::velox::tzdb
