//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "velox/external/tzdb/exception.h"

namespace facebook::velox::tzdb {

nonexistent_local_time::~nonexistent_local_time() = default; // key function

ambiguous_local_time::~ambiguous_local_time() = default; // key function

invalid_time_zone::~invalid_time_zone() = default; // key function

[[noreturn]] void __throw_invalid_time_zone(
    [[maybe_unused]] const std::string_view& __tz_name) {
  throw invalid_time_zone(__tz_name);
}

} // namespace facebook::velox::tzdb
