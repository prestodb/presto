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

#include "velox/external/tzdb/sys_info.h"

namespace facebook::velox::tzdb {

struct local_info {
  static constexpr int unique = 0;
  static constexpr int nonexistent = 1;
  static constexpr int ambiguous = 2;

  int result;
  sys_info first;
  sys_info second;
};

} // namespace facebook::velox::tzdb
