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

#include "velox/common/base/Exceptions.h"
#include "velox/expression/VectorReaders.h"
#include "velox/expression/VectorWriters.h"
namespace facebook::velox::exec {

namespace {

// TODO: support fast path for unknown.
bool fastPathSupported(const TypePtr& type) {
  return type->isPrimitiveType() && type->kind() != TypeKind::UNKNOWN;
}

template <TypeKind kind>
bool constexpr fastPathSupportedStatic() {
  return TypeTraits<kind>::isPrimitiveType && kind != TypeKind::UNKNOWN;
}

// Base case for primitives.
template <TypeKind T>
void copy_from_internal(GenericWriter& out, const GenericView& in) {
  // Maybe we should print a warning asking the user to have a fast path
  // specialization for primitives and not to use this?.
  using simple_element_t = typename KindToSimpleType<T>::type;
  out.castTo<simple_element_t>() = in.castTo<simple_element_t>();
}

template <>
void copy_from_internal<TypeKind::VARBINARY>(
    GenericWriter& out,
    const GenericView& in) {
  out.castTo<Varbinary>().copy_from(in.castTo<Varbinary>());
}

template <>
void copy_from_internal<TypeKind::VARCHAR>(
    GenericWriter& out,
    const GenericView& in) {
  out.castTo<Varchar>().copy_from(in.castTo<Varchar>());
}

// Fast path when array elements are primitives.
template <TypeKind T>
void copy_from_internal_array_fast(GenericWriter& out, const GenericView& in) {
  if constexpr (fastPathSupportedStatic<T>()) {
    using simple_element_t = typename KindToSimpleType<T>::type;
    out.castTo<Array<simple_element_t>>().copy_from(
        in.castTo<Array<simple_element_t>>());

  } else {
    VELOX_UNREACHABLE("unsupported type dispatched");
  }
}

template <>
void copy_from_internal<TypeKind::ARRAY>(
    GenericWriter& out,
    const GenericView& view) {
  //   Fast path for when the array element is primitive.
  if (fastPathSupported(out.type()->childAt(0))) {
    TypeKind kind = out.type()->childAt(0)->kind();
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        copy_from_internal_array_fast, kind, out, view);
  } else {
    auto& writer = out.castTo<Array<Any>>();
    auto arrayView = view.castTo<Array<Any>>();
    writer.copy_from(arrayView);
  }
}

template <TypeKind KeyT, TypeKind ValueT>
void copy_from_internal_map_fast_keys_values(
    GenericWriter& out,
    const GenericView& in) {
  if constexpr (
      fastPathSupportedStatic<KeyT>() && fastPathSupportedStatic<ValueT>()) {
    using simple_key_t = typename KindToSimpleType<KeyT>::type;
    using simple_value_t = typename KindToSimpleType<ValueT>::type;

    auto& writer = out.castTo<Map<simple_key_t, simple_value_t>>();
    auto mapView = in.castTo<Map<simple_key_t, simple_value_t>>();
    writer.copy_from(mapView);
  } else {
    VELOX_UNREACHABLE("unsupported type dispatched");
  }
}

template <TypeKind KeyT>
void copy_from_internal_map_fast_keys(
    GenericWriter& out,
    const GenericView& in) {
  if (fastPathSupported(out.type()->childAt(1))) {
    TypeKind kind = out.type()->childAt(1)->kind();
    VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
        copy_from_internal_map_fast_keys_values, KeyT, kind, out, in);
  } else {
    if constexpr (fastPathSupportedStatic<KeyT>()) {
      using simple_key_t = typename KindToSimpleType<KeyT>::type;

      auto& writer = out.castTo<Map<simple_key_t, Any>>();
      auto mapView = in.castTo<Map<simple_key_t, Any>>();
      writer.copy_from(mapView);
    } else {
      VELOX_UNREACHABLE("unsupported type dispatched");
    }
  }
}

template <TypeKind ValueT>
void copy_from_internal_map_fast_values(
    GenericWriter& out,
    const GenericView& in) {
  if constexpr (fastPathSupportedStatic<ValueT>()) {
    using simple_value_t = typename KindToSimpleType<ValueT>::type;
    auto& writer = out.castTo<Map<Any, simple_value_t>>();
    auto mapView = in.castTo<Map<Any, simple_value_t>>();
    writer.copy_from(mapView);
  } else {
    VELOX_UNREACHABLE("unsupported type dispatched");
  }
}

template <>
void copy_from_internal<TypeKind::MAP>(
    GenericWriter& out,
    const GenericView& view) {
  if (fastPathSupported(out.type()->childAt(0))) {
    TypeKind kind = out.type()->childAt(0)->kind();
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        copy_from_internal_map_fast_keys, kind, out, view);
  } else if (fastPathSupported(out.type()->childAt(1))) {
    TypeKind kind = out.type()->childAt(1)->kind();
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        copy_from_internal_map_fast_values, kind, out, view);
  } else {
    auto& writer = out.castTo<Map<Any, Any>>();
    auto mapView = view.castTo<Map<Any, Any>>();
    writer.copy_from(mapView);
  }
}

template <>
void copy_from_internal<TypeKind::ROW>(
    GenericWriter& out,
    const GenericView& in) {
  auto dyanmicRowView = in.castTo<DynamicRow>();
  auto& dynamicRowWriter = out.castTo<DynamicRow>();
  for (int i = 0; i < dyanmicRowView.size(); i++) {
    auto fieldGenericView = dyanmicRowView.at(i);

    if (fieldGenericView.has_value()) {
      TypeKind kind = fieldGenericView->kind();
      VELOX_DYNAMIC_TYPE_DISPATCH(
          copy_from_internal,
          kind,
          dynamicRowWriter.get_writer_at(i),
          fieldGenericView.value());
    } else {
      dynamicRowWriter.set_null_at(i);
    }
  }
}

} // namespace

void GenericWriter::copy_from(const GenericView& view) {
  TypeKind kind = this->kind();
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(copy_from_internal, kind, *this, view);
}
} // namespace facebook::velox::exec
