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
#pragma once

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

constexpr int kSortingNetworkMaxSize = 16;

template <typename T, typename LessThan = std::less<T>>
void sortingNetwork(T* data, int size, LessThan&& lt = {});

namespace detail {

// Compile time generated Bose-Nelson sorting network.
//
// https://bertdobbelaere.github.io/sorting_networks.html
// https://github.com/Vectorized/Static-Sort/blob/master/include/static_sort.h
template <int kSize>
class SortingNetworkImpl {
 public:
  template <typename T, typename LessThan>
  static void apply(T* data, LessThan&& lt) {
    PS<T, LessThan, 1, kSize, (kSize <= 1)> ps(data, lt);
  }

 private:
  template <int I, int J, typename T, typename LessThan>
  static void compareExchange(T* data, LessThan lt) {
    // This is branchless if `lt' is branchless.
    auto c = lt(data[I], data[J]);
    auto min = c ? data[I] : data[J];
    data[J] = c ? data[J] : data[I];
    data[I] = min;
  }

  template <typename T, typename LessThan, int I, int J, int X, int Y>
  struct PB {
    PB(T* data, LessThan lt) {
      enum {
        L = X >> 1,
        M = (X & 1 ? Y : Y + 1) >> 1,
        IAddL = I + L,
        XSubL = X - L,
      };
      PB<T, LessThan, I, J, L, M> p0(data, lt);
      PB<T, LessThan, IAddL, J + M, XSubL, Y - M> p1(data, lt);
      PB<T, LessThan, IAddL, J, XSubL, M> p2(data, lt);
    }
  };

  template <typename T, typename LessThan, int I, int J>
  struct PB<T, LessThan, I, J, 1, 1> {
    PB(T* data, LessThan lt) {
      compareExchange<I - 1, J - 1>(data, lt);
    }
  };

  template <typename T, typename LessThan, int I, int J>
  struct PB<T, LessThan, I, J, 1, 2> {
    PB(T* data, LessThan lt) {
      compareExchange<I - 1, J>(data, lt);
      compareExchange<I - 1, J - 1>(data, lt);
    }
  };

  template <typename T, typename LessThan, int I, int J>
  struct PB<T, LessThan, I, J, 2, 1> {
    PB(T* data, LessThan lt) {
      compareExchange<I - 1, J - 1>(data, lt);
      compareExchange<I, J - 1>(data, lt);
    }
  };

  template <typename T, typename LessThan, int I, int M, bool kStop>
  struct PS {
    PS(T* data, LessThan lt) {
      enum { L = M >> 1, IAddL = I + L, MSubL = M - L };
      PS<T, LessThan, I, L, (L <= 1)> ps0(data, lt);
      PS<T, LessThan, IAddL, MSubL, (MSubL <= 1)> ps1(data, lt);
      PB<T, LessThan, I, IAddL, L, MSubL> pb(data, lt);
    }
  };

  template <typename T, typename LessThan, int I, int M>
  struct PS<T, LessThan, I, M, true> {
    PS(T* /*data*/, LessThan /*lt*/) {}
  };
};

} // namespace detail

template <typename T, typename LessThan>
void sortingNetwork(T* data, int size, LessThan&& lt) {
  switch (size) {
    case 0:
    case 1:
      return;

#ifdef VELOX_SORTING_NETWORK_IMPL_APPLY_CASE
#error "Macro name clash: VELOX_SORTING_NETWORK_IMPL_APPLY_CASE"
#endif
#define VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(_n)                            \
  case _n:                                                                   \
    detail::SortingNetworkImpl<_n>::apply(data, std::forward<LessThan>(lt)); \
    return;

      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(2)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(3)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(4)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(5)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(6)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(7)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(8)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(9)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(10)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(11)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(12)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(13)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(14)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(15)
      VELOX_SORTING_NETWORK_IMPL_APPLY_CASE(16)

#undef VELOX_SORTING_NETWORK_IMPL_APPLY_CASE

    default:
      VELOX_UNREACHABLE();
  }
}

} // namespace facebook::velox
