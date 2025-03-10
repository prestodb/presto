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
#include "velox/external/tzdb/types_private.h"
#include "velox/external/tzdb/tzdb_list.h"
#include "velox/external/tzdb/tzdb_private.h"

#if _LIBCPP_HAS_THREADS
#include <mutex>
#endif

namespace facebook::velox::tzdb {

//===----------------------------------------------------------------------===//
//                          Private API
//===----------------------------------------------------------------------===//

// The tzdb_list stores a list of "tzdb" entries.
//
// The public tzdb database does not store the RULE entries of the IANA
// database. These entries are considered an implementation detail. Since most
// of the tzdb_list interface is exposed as "a list of tzdb entries" it's not
// possible to use a helper struct that stores a tzdb and the RULE database.
// Instead this class stores these in parallel forward lists.
//
// Since the nodes of a forward_list are stable it's possible to store pointers
// and references to these nodes.
class tzdb_list::__impl {
 public:
  __impl() {
    __load_no_lock();
  }

  [[nodiscard]] const tzdb& __load() {
#if _LIBCPP_HAS_THREADS
    unique_lock __lock{__mutex_};
#endif
    __load_no_lock();
    return __tzdb_.front();
  }

  using const_iterator = tzdb_list::const_iterator;

  const tzdb& __front() const noexcept {
#if _LIBCPP_HAS_THREADS
    unique_lock __lock{__mutex_};
#endif
    return __tzdb_.front();
  }

  const_iterator __erase_after(const_iterator __p) {
#if _LIBCPP_HAS_THREADS
    unique_lock __lock{__mutex_};
#endif

    __rules_.erase_after(
        std::next(__rules_.cbegin(), std::distance(__tzdb_.cbegin(), __p)));
    return __tzdb_.erase_after(__p);
  }

  const_iterator __begin() const noexcept {
#if _LIBCPP_HAS_THREADS
    unique_lock __lock{__mutex_};
#endif
    return __tzdb_.begin();
  }
  const_iterator __end() const noexcept {
    //  forward_list<T>::end does not access the list, so no need to take a
    //  lock.
    return __tzdb_.end();
  }

 private:
  // Loads the tzdbs
  // pre: The caller ensures the locking, if needed, is done.
  void __load_no_lock() {
    __init_tzdb(__tzdb_.emplace_front(), __rules_.emplace_front());
  }

#if _LIBCPP_HAS_THREADS
  mutable mutex __mutex_;
#endif
  std::forward_list<tzdb> __tzdb_;

  std::forward_list<__rules_storage_type> __rules_;
};

} // namespace facebook::velox::tzdb
