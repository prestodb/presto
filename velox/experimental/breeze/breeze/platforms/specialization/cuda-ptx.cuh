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

template <int WARP_THREADS>
__device__ __forceinline__ int CudaSpecialization::lane_idx() {
  return threadIdx.x % WARP_THREADS;
}

template <int WARP_THREADS>
__device__ __forceinline__ int CudaSpecialization::warp_idx() {
  return threadIdx.x / WARP_THREADS;
}

__device__ __forceinline__ unsigned LANEMASK_LT() {
  unsigned result;
  asm("mov.u32 %0, %%lanemask_lt;" : "=r"(result));
  return result;
}

template <int WARP_THREADS>
__device__ __forceinline__ unsigned CudaSpecialization::lower_rank_lanemask() {
  return LANEMASK_LT();
}

__device__ __forceinline__ unsigned LANEMASK_GT() {
  unsigned result;
  asm("mov.u32 %0, %%lanemask_gt;" : "=r"(result));
  return result;
}

template <int WARP_THREADS>
__device__ __forceinline__ unsigned CudaSpecialization::higher_rank_lanemask() {
  return LANEMASK_GT();
}

__device__ __forceinline__ unsigned BFE(unsigned value, int start_bit,
                                        int num_bits) {
  asm("bfe.u32 %0, %1, %2, %3;"
      : "=r"(value)
      : "r"(value), "r"(start_bit), "r"(num_bits));
  return value;
}

// specialization for T=unsigned
template <>
__device__ __forceinline__ unsigned CudaSpecialization::extract_bits(
    unsigned value, int start_bit, int num_bits) {
  return BFE(value, start_bit, num_bits);
}
