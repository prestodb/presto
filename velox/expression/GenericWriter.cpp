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

template <TypeKind kind>
struct KindToSimpleType {
  using type = typename TypeTraits<kind>::NativeType;
};

template <>
struct KindToSimpleType<TypeKind::VARCHAR> {
  using type = Varchar;
};

template <>
struct KindToSimpleType<TypeKind::VARBINARY> {
  using type = Varbinary;
};

// Fast path when array elements are primitives.
template <TypeKind T>
void copy_from_internal_array_fast(GenericWriter& out, const GenericView& in) {
  if constexpr (
      T == TypeKind::ARRAY || T == TypeKind::ROW || T == TypeKind::MAP) {
    VELOX_UNREACHABLE(
        "Element type for fast path of copy_from must be primitive.");
  } else {
    using simple_element_t = typename KindToSimpleType<T>::type;
    out.castTo<Array<simple_element_t>>().copy_from(
        in.castTo<Array<simple_element_t>>());
  }
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

template <>
void copy_from_internal<TypeKind::ARRAY>(
    GenericWriter& out,
    const GenericView& view) {
  //   Fast path for when the array element is primitive.
  if (out.type()->childAt(0)->isPrimitiveType()) {
    TypeKind kind = out.type()->childAt(0)->kind();
    VELOX_DYNAMIC_TYPE_DISPATCH(copy_from_internal_array_fast, kind, out, view);
  } else {
    auto& writer = out.castTo<Array<Any>>();
    auto arrayView = view.castTo<Array<Any>>();
    writer.copy_from(arrayView);
  }
}

template <>
void copy_from_internal<TypeKind::MAP>(
    GenericWriter& out,
    const GenericView& view) {
  // TODO: add fast path for map<prim, prim>.
  auto& writer = out.castTo<Map<Any, Any>>();
  auto mapView = view.castTo<Map<Any, Any>>();
  writer.copy_from(mapView);
}

template <>
void copy_from_internal<TypeKind::ROW>(GenericWriter&, const GenericView&) {
  VELOX_UNREACHABLE("not supported yet!");
}

} // namespace

void GenericWriter::copy_from(const GenericView& view) {
  TypeKind kind = this->kind();
  VELOX_DYNAMIC_TYPE_DISPATCH(copy_from_internal, kind, *this, view);
}
} // namespace facebook::velox::exec
