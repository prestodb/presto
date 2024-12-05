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

#include <cuda.h>

#include <type_traits>
#include <vector>

namespace {
// For the host-side interface pointers should instead be vectors
template <typename T>
using ToHostType = std::conditional_t<
    !std::is_pointer_v<T>, T,
    std::conditional_t<
        std::is_const_v<std::remove_pointer_t<T>>,
        const std::vector<std::remove_const_t<std::remove_pointer_t<T>>>&,
        std::vector<std::remove_pointer_t<T>>&>>;

// Remove top-level or one pointer deep consts
template <typename T>
using Deconst = std::conditional_t<
    std::is_pointer_v<T>,
    std::add_pointer_t<std::remove_const_t<std::remove_pointer_t<T>>>,
    std::remove_const_t<T>>;

template <typename DeviceType, typename HostType, bool False = false>
void PrepareArgs(DeviceType* d_param, HostType& param) {
  if constexpr (std::is_pointer_v<DeviceType>) {
    using BaseType = std::remove_pointer_t<DeviceType>;
    *d_param = nullptr;
    if constexpr (!std::is_same_v<BaseType, breeze::utils::NullType>) {
      static_assert(std::is_arithmetic_v<BaseType>);
      static_assert(
          std::is_same_v<std::remove_const_t<HostType>, std::vector<BaseType>>);
      // Allocate and initialize device pointers
      cudaMalloc((void**)d_param, sizeof(BaseType) * param.size());
      cudaMemcpy(*d_param, param.data(), sizeof(BaseType) * param.size(),
                 cudaMemcpyHostToDevice);
    }
  } else if constexpr (std::is_arithmetic_v<DeviceType>) {
    static_assert(std::is_arithmetic_v<HostType>);
    *d_param = param;
  } else {
    static_assert(False, "Unexpected kernel argument");
  }
}

template <typename DeviceType, typename HostType, bool False = false>
void FinishArgs(HostType& param, DeviceType d_param) {
  if constexpr (std::is_pointer_v<DeviceType>) {
    using BaseType = std::remove_pointer_t<DeviceType>;
    if constexpr (!std::is_same_v<BaseType, breeze::utils::NullType>) {
      static_assert(std::is_arithmetic_v<BaseType>);
      static_assert(
          std::is_same_v<std::remove_const_t<HostType>, std::vector<BaseType>>);
      if constexpr (!std::is_const_v<HostType>) {
        cudaMemcpy(param.data(), d_param, sizeof(BaseType) * param.size(),
                   cudaMemcpyDeviceToHost);
      }
      cudaFree(d_param);
    }
  } else if constexpr (std::is_arithmetic_v<DeviceType>) {
    static_assert(std::is_arithmetic_v<HostType>);
    // Nothing to do here. Arithmetic types neither need to be cleaned up nor
    // trigger a writeback
    (void)d_param;
  } else {
    static_assert(False, "Unexpected kernel argument");
  }
}
};  // namespace

// This helper only supports kernels with the following argument types:
// (where T is an arithmetic type)
//   1. T
//   2. T*
//   3. const T*
// It presents an interface such that pointer arguments are taken as vectors
// instead. If an argument is pointer-to-const it is considered input-only and
// if it is pointer-to-non-const it is considered an output and the device
// buffer will be written back into the vector.
template <int BLOCK_THREADS, typename... Params>
void CudaTestLaunch(int num_blocks, void (*kernel)(Params...),
                    ToHostType<Params>... params) {
  static_assert(
      ((std::is_same_v<std::remove_const_t<std::remove_pointer_t<Params>>,
                       breeze::utils::NullType> ||
        std::is_arithmetic_v<Params> ||
        std::is_arithmetic_v<std::remove_pointer_t<Params>>) &&
       ...));

  // This lambda (which is immediately evaluated) allows us to get a parameter
  // pack for the local variables that will hold the data pointers for the
  // device-side memory.
  [&](Deconst<Params>... d_params) {
    (PrepareArgs(&d_params, params), ...);
    void* args[] = {&d_params...};
    cudaLaunchKernel(reinterpret_cast<const void*>(kernel), dim3(num_blocks),
                     dim3(BLOCK_THREADS), args, 0, nullptr);
    cudaDeviceSynchronize();
    (FinishArgs(params, d_params), ...);
  }(Deconst<Params>{}...);
}
