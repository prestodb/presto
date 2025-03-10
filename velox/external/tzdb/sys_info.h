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

#include <chrono>
#include <string>
#include "velox/external/date/date.h"

namespace facebook::velox::tzdb {

struct sys_info {
  date::sys_seconds begin;
  date::sys_seconds end;
  std::chrono::seconds offset;
  std::chrono::minutes save;
  std::string abbrev;
};

} // namespace facebook::velox::tzdb
