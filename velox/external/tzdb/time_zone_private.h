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

#include <string>
#include <vector>

#include "velox/external/date/tz.h"
#include "velox/external/tzdb/time_zone.h"
#include "velox/external/tzdb/types_private.h"

namespace facebook::velox::tzdb {

class time_zone::__impl {
 public:
  explicit __impl(std::string&& __name, const __rules_storage_type& __rules_db)
      : __name_(std::move(__name)), __rules_db_(__rules_db) {}

  [[nodiscard]] std::string_view __name() const noexcept {
    return __name_;
  }

  [[nodiscard]] std::vector<facebook::velox::tzdb::__continuation>& __continuations() {
    return __continuations_;
  }
  [[nodiscard]] const std::vector<facebook::velox::tzdb::__continuation>& __continuations() const {
    return __continuations_;
  }

  [[nodiscard]] const __rules_storage_type& __rules_db() const {
    return __rules_db_;
  }

  [[nodiscard]] std::vector<date::transition>& transitions() {
    return transitions_;
  }

  [[nodiscard]] const std::vector<date::transition>& transitions() const {
    return transitions_;
  }

  [[nodiscard]] std::vector<date::expanded_ttinfo>& ttinfos() {
    return ttinfos_;
  }

  [[nodiscard]] const std::vector<date::expanded_ttinfo>& ttinfos() const {
    return ttinfos_;
  }

 private:
  std::string __name_;
  // Note the first line has a name + __continuation, the other lines
  // are just __continuations. So there is always at least one item in
  // the vector.
  std::vector<facebook::velox::tzdb::__continuation> __continuations_;

  // Continuations often depend on a set of rules. The rules are stored in
  // parallel data structurs in tzdb_list. From the time_zone it's not possible
  // to find its associated tzdb entry and thus not possible to find its
  // associated rules. Therefore a link to the rules in stored in this class.
  const __rules_storage_type& __rules_db_;

  std::vector<date::transition>      transitions_;
  std::vector<date::expanded_ttinfo> ttinfos_;
};

} // namespace facebook::velox::tzdb
