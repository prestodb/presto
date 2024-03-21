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

#include "velox/type/CppToType.h"
#include "velox/type/SimpleFunctionApi.h"

namespace facebook::velox {

// Recursively check that T and vectorType associate to the same TypeKind.
template <typename T>
struct CastTypeChecker {
  static_assert(
      CppToType<T>::maxSubTypes == 0,
      "Complex types should be checked separately.");

  static bool check(const TypePtr& vectorType) {
    return CppToType<T>::typeKind == vectorType->kind();
  }
};

template <>
struct CastTypeChecker<DynamicRow> {
  static bool check(const TypePtr& vectorType) {
    return TypeKind::ROW == vectorType->kind();
  }
};

template <typename T>
struct CastTypeChecker<Generic<T>> {
  static bool check(const TypePtr&) {
    return true;
  }
};

template <typename T>
struct CastTypeChecker<Array<T>> {
  static bool check(const TypePtr& vectorType) {
    return TypeKind::ARRAY == vectorType->kind() &&
        CastTypeChecker<T>::check(vectorType->childAt(0));
  }
};

template <typename K, typename V>
struct CastTypeChecker<Map<K, V>> {
  static bool check(const TypePtr& vectorType) {
    return TypeKind::MAP == vectorType->kind() &&
        CastTypeChecker<K>::check(vectorType->childAt(0)) &&
        CastTypeChecker<V>::check(vectorType->childAt(1));
  }
};

template <typename... T>
struct CastTypeChecker<Row<T...>> {
  static bool check(const TypePtr& vectorType) {
    int index = 0;
    return TypeKind::ROW == vectorType->kind() &&
        (CastTypeChecker<T>::check(vectorType->childAt(index++)) && ... &&
         true);
  }
};

} // namespace facebook::velox
