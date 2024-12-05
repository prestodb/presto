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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "breeze/utils/device_allocator.h"
#include "breeze/utils/device_vector.h"

namespace breeze {

template <typename T, typename Allocator = utils::device_allocator<T>>
class device_column : public utils::device_vector<T, Allocator> {
 public:
  explicit device_column(const Allocator& allocator = Allocator())
      : utils::device_vector<T, Allocator>(allocator) {}
  explicit device_column(utils::size_type size,
                         const Allocator& allocator = Allocator())
      : utils::device_vector<T, Allocator>(size, allocator) {}
};

template <typename T, typename Allocator = utils::device_allocator<T>>
class device_column_buffered {
  typedef typename Allocator::template rebind<T*>::other PtrAllocator;

 public:
  explicit device_column_buffered(const Allocator& allocator = Allocator())
      : buffers_{device_column<T, Allocator>(allocator),
                 device_column<T, Allocator>(allocator)},
        ptrs_(utils::device_vector<T*, PtrAllocator>(2,
                                                     PtrAllocator(allocator))) {
    UpdatePtrs();
  }
  explicit device_column_buffered(utils::size_type size,
                                  const Allocator& allocator = Allocator())
      : buffers_{device_column<T, Allocator>(size, allocator),
                 device_column<T, Allocator>(size, allocator)},
        ptrs_(utils::device_vector<T*, PtrAllocator>(2,
                                                     PtrAllocator(allocator))) {
    UpdatePtrs();
  }

  void resize(size_t size) {
    buffers_[0].resize(size);
    buffers_[1].resize(size);
    UpdatePtrs();
  }
  size_t size() const { return buffers_[0].size(); }
  device_column<T, Allocator>& buffer(int index) { return buffers_[index]; }
  utils::device_vector<T*, PtrAllocator>& ptrs() { return ptrs_; }

 private:
  void UpdatePtrs() {
    T* ptrs[] = {buffers_[0].data(), buffers_[1].data()};
    ptrs_.copy_from_host(ptrs, 2);
  }

  device_column<T, Allocator> buffers_[2];
  utils::device_vector<T*, PtrAllocator> ptrs_;
};

}  // namespace breeze
