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

#include <CL/sycl.hpp>
#include <type_traits>
#include <vector>

template <typename T, typename Buffer, typename Cgh>
auto get_access_by_type(Buffer& buffer, Cgh& cgh) {
  using namespace cl;
  if constexpr (std::is_const_v<T>) {
    return buffer.template get_access<sycl::access::mode::read>(cgh);
  } else {
    return buffer.template get_access<sycl::access::mode::read_write>(cgh);
  }
}

template <int BLOCK_THREADS, typename K, typename... Vectors>
void SyclTestSubmit(int num_blocks, K&& kernel, Vectors&... vecs) {
  using namespace cl;

  sycl::queue queue{sycl::default_selector{}};

  std::tuple buffers(sycl::buffer<typename Vectors::value_type, 1>(
      vecs.data(), vecs.size())...);

  queue.submit([&](sycl::handler& cgh) {
    std::tuple accesses = std::apply(
        [&](auto... buffers) {
          return std::tuple(get_access_by_type<Vectors>(buffers, cgh)...);
        },
        buffers);

    auto block_execution_range =
        sycl::nd_range<1>(num_blocks * BLOCK_THREADS, BLOCK_THREADS);

    std::apply(
        [&](auto... accesses) {
          cgh.parallel_for(block_execution_range, [=](sycl::nd_item<1> item) {
            kernel(item, (decltype(vecs.data()))accesses.get_pointer()...);
          });
        },
        accesses);
  });
}

template <int BLOCK_THREADS, typename S, typename K, typename... Vectors>
void SyclTestSubmit(int num_blocks, K&& kernel, Vectors&... vecs) {
  using namespace cl;

  sycl::queue queue{sycl::default_selector{}};

  std::tuple buffers(sycl::buffer<typename Vectors::value_type, 1>(
      vecs.data(), vecs.size())...);

  queue.submit([&](sycl::handler& cgh) {
    sycl::accessor<S, 1, sycl::access::mode::discard_read_write,
                   sycl::access::target::local>
        shared_mem(sycl::range<1>(1), cgh);

    std::tuple accesses = std::apply(
        [&](auto... buffers) {
          return std::tuple(get_access_by_type<Vectors>(buffers, cgh)...);
        },
        buffers);

    auto block_execution_range =
        sycl::nd_range<1>(num_blocks * BLOCK_THREADS, BLOCK_THREADS);

    std::apply(
        [&](auto... accesses) {
          cgh.parallel_for(block_execution_range, [=](sycl::nd_item<1> item) {
            S* shared = shared_mem.get_pointer();
            kernel(item, (decltype(vecs.data()))accesses.get_pointer()...,
                   shared);
          });
        },
        accesses);
  });
}
