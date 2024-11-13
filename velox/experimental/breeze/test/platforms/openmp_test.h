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

#include <omp.h>

#include <climits>
#include <cstdint>

// The nested-name-specifier of a qualified-id is a non-deduced context.
// We take advantage of that to make sure `Params` is deduced only from the
// kernel signature and not the subsequent arguments passed in.
template <typename T>
struct DisableDeduction {
  using type = T;
};
template <typename T>
using disable_deduction_t = typename DisableDeduction<T>::type;

template <int BLOCK_THREADS, typename SharedMemType, typename PlatformT,
          typename... Params>
void OpenMPTestLaunch(int num_blocks,
                      void (*kernel)(PlatformT, SharedMemType*, Params...),
                      disable_deduction_t<Params>... args) {
  SharedMemType shared_mem;
  char scratch[sizeof(uintmax_t)];
#pragma omp parallel shared(shared_mem, scratch) num_threads(BLOCK_THREADS)
  {
    for (int b = 0; b < num_blocks; ++b) {
      PlatformT p{b, scratch};
      kernel(p, &shared_mem, args...);
      // each block must end with a barrier
#pragma omp barrier
    }
  }
}

template <int BLOCK_THREADS, typename PlatformT, typename... Params>
void OpenMPTestLaunch(int num_blocks, void (*kernel)(PlatformT, Params...),
                      disable_deduction_t<Params>... args) {
  char scratch[sizeof(uintmax_t)];
#pragma omp parallel shared(scratch) num_threads(BLOCK_THREADS)
  {
    for (int b = 0; b < num_blocks; ++b) {
      PlatformT p{b, scratch};
      kernel(p, args...);
      // each block must end with a barrier
#pragma omp barrier
    }
  }
}

template <int BLOCK_THREADS, typename... Params>
void OpenMPTestLaunch(int num_blocks, void (*kernel)(Params...),
                      disable_deduction_t<Params>... args) {
#pragma omp parallel num_threads(BLOCK_THREADS)
  {
    for (int b = 0; b < num_blocks; ++b) {
      kernel(args...);
      // each block must end with a barrier
#pragma omp barrier
    }
  }
}
