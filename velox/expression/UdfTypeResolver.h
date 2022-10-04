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

#include "velox/type/Type.h"

namespace facebook::velox::exec {

template <bool reuseInput>
class StringWriter;

template <bool nullable, typename V>
class ArrayView;

template <typename T>
using NullFreeArrayView = ArrayView<false, T>;

template <typename T>
using NullableArrayView = ArrayView<true, T>;

template <bool nullable, typename K, typename V>
class MapView;

template <typename K, typename V>
using NullFreeMapView = MapView<false, K, V>;

template <typename K, typename V>
using NullableMapView = MapView<true, K, V>;

template <bool nullable, typename... T>
class RowView;

template <typename... T>
using NullFreeRowView = RowView<false, T...>;

template <typename... T>
using NullableRowView = RowView<true, T...>;

template <bool nullable, typename T>
class VariadicView;

template <typename T>
using NullFreeVariadicView = VariadicView<false, T>;

template <typename T>
using NullableVariadicView = VariadicView<true, T>;

template <typename V>
class ArrayWriter;

template <typename... T>
class RowWriter;

template <typename K, typename V>
class MapWriter;

class GenericView;

namespace detail {
template <typename T>
struct resolver {
  using in_type = typename CppToType<T>::NativeType;
  using null_free_in_type = in_type;
  using out_type = typename CppToType<T>::NativeType;
};

template <typename K, typename V>
struct resolver<Map<K, V>> {
  using in_type = NullableMapView<K, V>;
  using null_free_in_type = NullFreeMapView<K, V>;
  using out_type = MapWriter<K, V>;
};

template <typename... T>
struct resolver<Row<T...>> {
  using in_type = NullableRowView<T...>;
  using null_free_in_type = NullFreeRowView<T...>;
  using out_type = RowWriter<T...>;
};

template <typename V>
struct resolver<Array<V>> {
  using in_type = NullableArrayView<V>;
  using null_free_in_type = NullFreeArrayView<V>;
  using out_type = ArrayWriter<V>;
};

template <>
struct resolver<Varchar> {
  using in_type = StringView;
  using null_free_in_type = in_type;
  using out_type = StringWriter<false>;
};

template <>
struct resolver<Varbinary> {
  using in_type = StringView;
  using null_free_in_type = in_type;
  using out_type = StringWriter<false>;
};

template <typename T>
struct resolver<std::shared_ptr<T>> {
  using in_type = std::shared_ptr<T>;
  using null_free_in_type = in_type;
  using out_type = std::shared_ptr<T>;
};

template <typename T>
struct resolver<Variadic<T>> {
  using in_type = NullableVariadicView<T>;
  using null_free_in_type = NullFreeVariadicView<T>;
  // Variadic cannot be used as an out_type
};

template <typename T>
struct resolver<Generic<T>> {
  using in_type = GenericView;
  using null_free_in_type = in_type;
  using out_type = void; // Not supported as output type yet.
};
} // namespace detail

struct VectorExec {
  template <typename T>
  using resolver = typename detail::template resolver<T>;
};
} // namespace facebook::velox::exec
