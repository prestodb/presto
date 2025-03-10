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

#include <forward_list>
#include <string_view>
#include "velox/external/tzdb/time_zone.h"
#include "velox/external/tzdb/tzdb.h"

namespace facebook::velox::tzdb {

// TODO TZDB
// Libc++ recently switched to only export __ugly_names from the dylib.
// Since the library is still experimental the functions in this header
// should be adapted to this new style. The other tzdb headers should be
// evaluated too.

class tzdb_list {
 public:
  class __impl; // public to allow construction in dylib
  explicit tzdb_list(__impl* __p) : __impl_(__p) {
    // _LIBCPP_ASSERT_NON_NULL(__impl_ != nullptr, "initialized time_zone
    // without a valid pimpl object");
  }
  ~tzdb_list();

  tzdb_list(const tzdb_list&) = delete;
  tzdb_list& operator=(const tzdb_list&) = delete;

  using const_iterator = std::forward_list<tzdb>::const_iterator;

  [[nodiscard]] const tzdb& front() const noexcept {
    return __front();
  }

  const_iterator erase_after(const_iterator __p) {
    return __erase_after(__p);
  }

  [[nodiscard]] const_iterator begin() const noexcept {
    return __begin();
  }
  [[nodiscard]] const_iterator end() const noexcept {
    return __end();
  }

  [[nodiscard]] const_iterator cbegin() const noexcept {
    return __cbegin();
  }
  [[nodiscard]] const_iterator cend() const noexcept {
    return __cend();
  }

  [[nodiscard]] __impl& __implementation() {
    return *__impl_;
  }

 private:
  [[nodiscard]] const tzdb& __front() const noexcept;

  const_iterator __erase_after(const_iterator __p);

  [[nodiscard]] const_iterator __begin() const noexcept;
  [[nodiscard]] const_iterator __end() const noexcept;

  [[nodiscard]] const_iterator __cbegin() const noexcept;
  [[nodiscard]] const_iterator __cend() const noexcept;

  __impl* __impl_;
};

[[nodiscard]] tzdb_list& get_tzdb_list();

[[nodiscard]] inline const tzdb& get_tzdb() {
  return get_tzdb_list().front();
}

[[nodiscard]] inline const time_zone* locate_zone(std::string_view __name) {
  return get_tzdb().locate_zone(__name);
}

[[nodiscard]] inline const time_zone* current_zone() {
  return get_tzdb().current_zone();
}

const tzdb& reload_tzdb();

[[nodiscard]] std::string remote_version();

} // namespace facebook::velox::tzdb
