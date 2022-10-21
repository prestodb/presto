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

#include <limits>
#include <optional>
#include <vector>

#include "velox/type/Variant.h"

namespace facebook::velox::functions::sparksql {

namespace {
template <typename T>
using NestedVector = std::vector<std::vector<T>>;

template <typename T>
T lowest() {
  return std::numeric_limits<T>::lowest();
}

template <typename T>
T max() {
  return std::numeric_limits<T>::max();
}

template <typename T>
T inf() {
  return std::numeric_limits<T>::infinity();
}

template <typename T>
T nan() {
  return std::numeric_limits<T>::quiet_NaN();
}
} // namespace

template <typename T>
inline NestedVector<T> reverseNested(NestedVector<T> data) {
  for (auto& v : data) {
    std::reverse(v.begin(), v.end());
  }
  return data;
}

template <typename T>
inline std::vector<std::optional<std::vector<T>>> reverseNested(
    std::vector<std::optional<std::vector<T>>> data) {
  for (auto& v : data) {
    std::reverse(v->begin(), v->end());
  }
  return data;
}

template <typename T>
inline NestedVector<std::optional<T>> intInput() {
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {9, 8, 12},
      {5, 6, 1, std::nullopt, 0, 99, -99},
      {lowest<T>(), max<T>(), -1, 1, 0, std::nullopt},
  };
}

template <typename T>
inline NestedVector<std::optional<T>> intAscNullSmallest() {
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {8, 9, 12},
      {std::nullopt, -99, 0, 1, 5, 6, 99},
      {std::nullopt, lowest<T>(), -1, 0, 1, max<T>()},
  };
}

template <typename T>
inline NestedVector<std::optional<T>> intAscNullLargest() {
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {8, 9, 12},
      {-99, 0, 1, 5, 6, 99, std::nullopt},
      {lowest<T>(), -1, 0, 1, max<T>(), std::nullopt},
  };
}

template <typename T>
inline NestedVector<std::optional<T>> floatingPointInput() {
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {1.0001, std::nullopt, 1.0, -2.0, 3.03, std::nullopt},
      {max<T>(), lowest<T>(), nan<T>(), inf<T>(), -9.9, 9.9, std::nullopt, 0.},
  };
}

template <typename T>
inline NestedVector<std::optional<T>> floatingPointAscNullSmallest() {
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, -2.0, 1.0, 1.0001, 3.03},
      {std::nullopt, lowest<T>(), -9.9, 0., 9.9, max<T>(), inf<T>(), nan<T>()},
  };
}

template <typename T>
inline NestedVector<std::optional<T>> floatingPointAscNullLargest() {
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {-2.0, 1.0, 1.0001, 3.03, std::nullopt, std::nullopt},
      {lowest<T>(), -9.9, 0., 9.9, max<T>(), inf<T>(), nan<T>(), std::nullopt},
  };
}

inline NestedVector<std::optional<std::string>> stringInput() {
  return NestedVector<std::optional<std::string>>{
      {},
      {std::nullopt, std::nullopt},
      {"spiderman", "captainamerica", "ironman", "hulk", "deadpool", "thor"},
      {"s", "c", "", std::nullopt, "h", "d"},
  };
}

inline NestedVector<std::optional<std::string>> stringAscNullSmallest() {
  return NestedVector<std::optional<std::string>>{
      {},
      {std::nullopt, std::nullopt},
      {"captainamerica", "deadpool", "hulk", "ironman", "spiderman", "thor"},
      {std::nullopt, "", "c", "d", "h", "s"},
  };
}

inline NestedVector<std::optional<std::string>> stringAscNullLargest() {
  return NestedVector<std::optional<std::string>>{
      {},
      {std::nullopt, std::nullopt},
      {"captainamerica", "deadpool", "hulk", "ironman", "spiderman", "thor"},
      {"", "c", "d", "h", "s", std::nullopt},
  };
}

inline NestedVector<std::optional<Timestamp>> timestampInput() {
  using T = Timestamp;
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {T{0, 1}, T{1, 0}, std::nullopt, T{4, 20}, T{3, 30}},
  };
}

inline NestedVector<std::optional<Timestamp>> timestampAscNullSmallest() {
  using T = Timestamp;
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {std::nullopt, T{0, 1}, T{1, 0}, T{3, 30}, T{4, 20}},
  };
}

inline NestedVector<std::optional<Timestamp>> timestampAscNullLargest() {
  using T = Timestamp;
  return NestedVector<std::optional<T>>{
      {},
      {std::nullopt, std::nullopt},
      {T{0, 1}, T{1, 0}, T{3, 30}, T{4, 20}, std::nullopt},
  };
}

inline NestedVector<std::optional<Date>> dateInput() {
  using D = Date;
  return NestedVector<std::optional<D>>{
      {},
      {std::nullopt, std::nullopt},
      {D{0}, D{1}, std::nullopt, D{4}, D{3}},
  };
}

inline NestedVector<std::optional<Date>> dateAscNullSmallest() {
  using D = Date;
  return NestedVector<std::optional<D>>{
      {},
      {std::nullopt, std::nullopt},
      {std::nullopt, D{0}, D{1}, D{3}, D{4}},
  };
}

inline NestedVector<std::optional<Date>> dateAscNullLargest() {
  using D = Date;
  return NestedVector<std::optional<D>>{
      {},
      {std::nullopt, std::nullopt},
      {D{0}, D{1}, D{3}, D{4}, std::nullopt},
  };
}

inline NestedVector<std::optional<bool>> boolInput() {
  return NestedVector<std::optional<bool>>{
      {},
      {std::nullopt, std::nullopt},
      {true, false, true},
      {true, false, std::nullopt, true, false},
  };
}

inline NestedVector<std::optional<bool>> boolAscNullSmallest() {
  return NestedVector<std::optional<bool>>{
      {},
      {std::nullopt, std::nullopt},
      {false, true, true},
      {std::nullopt, false, false, true, true},
  };
}

inline NestedVector<std::optional<bool>> boolAscNullLargest() {
  return NestedVector<std::optional<bool>>{
      {},
      {std::nullopt, std::nullopt},
      {false, true, true},
      {false, false, true, true, std::nullopt},
  };
}

inline std::vector<std::optional<
    std::vector<std::optional<std::vector<std::optional<int32_t>>>>>>
arrayInput() {
  using A = std::vector<std::optional<int32_t>>;
  return std::vector<std::optional<std::vector<std::optional<A>>>>{
      // Empty.
      {{}},
      // All nulls.
      {{std::nullopt, std::nullopt}},
      // Same prefix.
      {{A({1, 3}), A({2, 1}), A({1, 3, 5})}},
      // Top level null elements.
      {{A({1, 3}), std::nullopt, A({2, 1})}},
      // Array with null values.
      {{A({std::nullopt, 6}), A({3, std::nullopt}), A({std::nullopt, 8})}},
  };
}

inline std::vector<std::optional<
    std::vector<std::optional<std::vector<std::optional<int32_t>>>>>>
arrayAscNullSmallest() {
  using A = std::vector<std::optional<int32_t>>;
  return std::vector<std::optional<std::vector<std::optional<A>>>>{
      {{}},
      {{std::nullopt, std::nullopt}},
      {{A({1, 3}), A({1, 3, 5}), A({2, 1})}},
      {{std::nullopt, A({1, 3}), A({2, 1})}},
      {{A({std::nullopt, 6}), A({std::nullopt, 8}), A({3, std::nullopt})}},
  };
}

inline std::vector<std::optional<
    std::vector<std::optional<std::vector<std::optional<int32_t>>>>>>
arrayAscNullLargest() {
  using A = std::vector<std::optional<int32_t>>;
  return std::vector<std::optional<std::vector<std::optional<A>>>>{
      {{}},
      {{std::nullopt, std::nullopt}},
      {{A({1, 3}), A({1, 3, 5}), A({2, 1})}},
      {{A({1, 3}), A({2, 1}), std::nullopt}},
      {{A({3, std::nullopt}), A({std::nullopt, 6}), A({std::nullopt, 8})}},
  };
}

inline NestedVector<std::vector<std::pair<int32_t, std::optional<int32_t>>>>
mapInput() {
  using M = std::vector<std::pair<int32_t, std::optional<int32_t>>>;
  return NestedVector<M>{
      // Empty.
      {},
      // Sort on normalized keys.
      {M{{1, 11}, {3, 10}}, M{{2, 11}, {0, 10}}, M{{1, 11}, {1, 10}}},
      // Sort on values when keys are same.
      {M{{1, 11}, {3, 10}}, M{{1, 13}, {3, 12}}},
      // Null values in map.
      {M{{0, std::nullopt}}, M{{0, 10}}},
  };
}

inline NestedVector<std::vector<std::pair<int32_t, std::optional<int32_t>>>>
mapAscNullSmallest() {
  using M = std::vector<std::pair<int32_t, std::optional<int32_t>>>;
  return NestedVector<M>{
      {},
      {M{{2, 11}, {0, 10}}, M{{1, 11}, {1, 10}}, M{{1, 11}, {3, 10}}},
      {M{{1, 11}, {3, 10}}, M{{1, 13}, {3, 12}}},
      {M{{0, std::nullopt}}, M{{0, 10}}},
  };
}

inline NestedVector<std::vector<std::pair<int32_t, std::optional<int32_t>>>>
mapAscNullLargest() {
  using M = std::vector<std::pair<int32_t, std::optional<int32_t>>>;
  return NestedVector<M>{
      {},
      {M{{2, 11}, {0, 10}}, M{{1, 11}, {1, 10}}, M{{1, 11}, {3, 10}}},
      {M{{1, 11}, {3, 10}}, M{{1, 13}, {3, 12}}},
      {M{{0, 10}}, M{{0, std::nullopt}}},
  };
}

inline NestedVector<variant> rowInput() {
  variant nullRow = variant(TypeKind::ROW);
  variant nullInt = variant(TypeKind::INTEGER);
  return NestedVector<variant>{
      // Empty.
      {},
      // All nulls.
      {nullRow, nullRow},
      // Null row.
      {variant::row({2, "red"}), nullRow, variant::row({1, "blue"})},
      // Null values in row.
      {variant::row({1, "green"}), variant::row({nullInt, "red"})},
  };
}

inline NestedVector<variant> rowAscNullSmallest() {
  variant nullRow = variant(TypeKind::ROW);
  variant nullInt = variant(TypeKind::INTEGER);
  return NestedVector<variant>{
      {},
      {nullRow, nullRow},
      {nullRow, variant::row({1, "blue"}), variant::row({2, "red"})},
      {variant::row({nullInt, "red"}), variant::row({1, "green"})},
  };
}

inline NestedVector<variant> rowAscNullLargest() {
  variant nullRow = variant(TypeKind::ROW);
  variant nullInt = variant(TypeKind::INTEGER);
  return NestedVector<variant>{
      {},
      {nullRow, nullRow},
      {variant::row({1, "blue"}), variant::row({2, "red"}), nullRow},
      {variant::row({1, "green"}), variant::row({nullInt, "red"})},
  };
}
} // namespace facebook::velox::functions::sparksql
