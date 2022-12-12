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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <velox/buffer/StringViewBufferHolder.h>
#include <velox/type/Type.h>
#include <velox/type/Variant.h>
#include <velox/vector/FlatVector.h>
#include "folly/json.h"

namespace facebook::velox::py {

namespace py = pybind11;

std::string serializeType(const std::shared_ptr<const velox::Type>& type);

inline void checkBounds(VectorPtr& v, vector_size_t idx) {
  if (idx < 0 || idx >= v->size()) {
    throw std::out_of_range("Index out of range");
  }
}

template <TypeKind T>
inline auto pyToVariant(const py::handle& obj) {
  using NativeType = typename TypeTraits<T>::DeepCopiedType;
  return velox::variant::create<T>(py::cast<NativeType>(obj));
}

inline velox::variant pyToVariant(const py::handle& obj) {
  if (obj.is_none()) {
    return velox::variant();
  } else if (py::isinstance<py::bool_>(obj)) {
    return pyToVariant<velox::TypeKind::BOOLEAN>(obj);
  } else if (py::isinstance<py::int_>(obj)) {
    return pyToVariant<velox::TypeKind::BIGINT>(obj);
  } else if (py::isinstance<py::float_>(obj)) {
    return pyToVariant<velox::TypeKind::DOUBLE>(obj);
  } else if (py::isinstance<py::str>(obj)) {
    return pyToVariant<velox::TypeKind::VARCHAR>(obj);
  } else {
    throw py::type_error("Invalid type of object");
  }
}

template <TypeKind T>
inline VectorPtr variantsToFlatVector(
    const std::vector<velox::variant>& variants,
    facebook::velox::memory::MemoryPool* pool) {
  using NativeType = typename TypeTraits<T>::NativeType;
  constexpr bool kNeedsHolder =
      (T == TypeKind::VARCHAR || T == TypeKind::VARBINARY);

  TypePtr type = fromKindToScalerType(T);
  auto result =
      BaseVector::create<FlatVector<NativeType>>(type, variants.size(), pool);

  std::conditional_t<
      kNeedsHolder,
      velox::StringViewBufferHolder,
      velox::memory::MemoryPool*>
      holder{pool};
  for (int i = 0; i < variants.size(); i++) {
    if (variants[i].isNull()) {
      result->setNull(i, true);
    } else {
      if constexpr (kNeedsHolder) {
        velox::StringView view =
            holder.getOwnedValue(variants[i].value<std::string>());
        result->set(i, view);
      } else {
        result->set(i, variants[i].value<NativeType>());
      }
    }
  }
  return result;
}

inline VectorPtr pyListToVector(
    const py::list& list,
    facebook::velox::memory::MemoryPool* pool) {
  std::vector<velox::variant> variants;
  variants.reserve(list.size());
  for (auto item : list) {
    variants.push_back(pyToVariant(item));
  }

  if (variants.empty()) {
    throw py::value_error("Can't create a Velox vector from an empty list");
  }

  velox::TypeKind first_kind = velox::TypeKind::INVALID;
  for (velox::variant& var : variants) {
    if (var.hasValue()) {
      if (first_kind == velox::TypeKind::INVALID) {
        first_kind = var.kind();
      } else if (var.kind() != first_kind) {
        throw py::type_error(
            "Velox Vector must consist of items of the same type");
      }
    }
  }

  if (first_kind == velox::TypeKind::INVALID) {
    throw py::value_error(
        "Can't create a Velox vector consisting of only None");
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      variantsToFlatVector, first_kind, variants, pool);
}

template <TypeKind T>
inline py::object getItemFromVector(VectorPtr& v, vector_size_t idx) {
  using NativeType = typename TypeTraits<T>::NativeType;
  if (std::is_same_v<NativeType, velox::StringView>) {
    const auto* flat = v->asFlatVector<velox::StringView>();
    const velox::StringView value = flat->valueAt(idx);
    py::str result = std::string_view(value);
    return result;
  } else {
    const auto* flat = v->asFlatVector<NativeType>();
    py::object result = py::cast(flat->valueAt(idx));
    return result;
  }
}

inline py::object getItemFromVector(VectorPtr& v, vector_size_t idx) {
  checkBounds(v, idx);
  if (v->isNullAt(idx)) {
    return py::none();
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      getItemFromVector, v->typeKind(), v, idx);
}

template <TypeKind T>
inline void
setItemInVector(VectorPtr& v, vector_size_t idx, velox::variant& var) {
  using NativeType = typename TypeTraits<T>::NativeType;
  auto* flat = v->asFlatVector<NativeType>();
  flat->set(idx, NativeType{var.value<NativeType>()});
}

inline void setItemInVector(VectorPtr& v, vector_size_t idx, py::handle& obj) {
  checkBounds(v, idx);

  velox::variant var = pyToVariant(obj);
  if (var.kind() == velox::TypeKind::INVALID) {
    return v->setNull(idx, true);
  }

  if (var.kind() != v->typeKind()) {
    throw py::type_error("Attempted to insert value of mismatched types");
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setItemInVector, v->typeKind(), v, idx, var);
}

inline void appendVectors(VectorPtr& u, VectorPtr& v) {
  if (u->typeKind() != v->typeKind()) {
    throw py::type_error("Tried to append vectors of two different types");
  }
  u->append(v.get());
}

inline void addDataTypeBindings(
    py::module& m,
    bool asModuleLocalDefinitions = true) {
  // Inlining these bindings since adding them to the cpp file results in a
  // ASAN error.
  using namespace velox;

  // Add TypeKind enum.
  py::enum_<velox::TypeKind>(
      m, "TypeKind", py::module_local(asModuleLocalDefinitions))
      .value("BOOLEAN", velox::TypeKind::BOOLEAN)
      .value("TINYINT", velox::TypeKind::TINYINT)
      .value("SMALLINT", velox::TypeKind::SMALLINT)
      .value("INTEGER", velox::TypeKind::INTEGER)
      .value("BIGINT", velox::TypeKind::BIGINT)
      .value("REAL", velox::TypeKind::REAL)
      .value("DOUBLE", velox::TypeKind::DOUBLE)
      .value("VARCHAR", velox::TypeKind::VARCHAR)
      .value("VARBINARY", velox::TypeKind::VARBINARY)
      .value("TIMESTAMP", velox::TypeKind::TIMESTAMP)
      .value("OPAQUE", velox::TypeKind::OPAQUE)
      .value("ARRAY", velox::TypeKind::ARRAY)
      .value("MAP", velox::TypeKind::MAP)
      .value("ROW", velox::TypeKind::ROW)
      .export_values();

  // Create VeloxType bound to velox::Type.
  py::class_<Type, std::shared_ptr<Type>> type(
      m, "VeloxType", py::module_local(asModuleLocalDefinitions));

  // Adding all the derived types of Type here.
  py::class_<BooleanType, Type, std::shared_ptr<BooleanType>> booleanType(
      m, "BooleanType", py::module_local(asModuleLocalDefinitions));
  py::class_<IntegerType, Type, std::shared_ptr<IntegerType>> integerType(
      m, "IntegerType", py::module_local(asModuleLocalDefinitions));
  py::class_<BigintType, Type, std::shared_ptr<BigintType>> bigintType(
      m, "BigintType", py::module_local(asModuleLocalDefinitions));
  py::class_<SmallintType, Type, std::shared_ptr<SmallintType>> smallintType(
      m, "SmallintType", py::module_local(asModuleLocalDefinitions));
  py::class_<TinyintType, Type, std::shared_ptr<TinyintType>> tinyintType(
      m, "TinyintType", py::module_local(asModuleLocalDefinitions));
  py::class_<RealType, Type, std::shared_ptr<RealType>> realType(
      m, "RealType", py::module_local(asModuleLocalDefinitions));
  py::class_<DoubleType, Type, std::shared_ptr<DoubleType>> doubleType(
      m, "DoubleType", py::module_local(asModuleLocalDefinitions));
  py::class_<TimestampType, Type, std::shared_ptr<TimestampType>> timestampType(
      m, "TimestampType", py::module_local(asModuleLocalDefinitions));
  py::class_<VarcharType, Type, std::shared_ptr<VarcharType>> varcharType(
      m, "VarcharType", py::module_local(asModuleLocalDefinitions));
  py::class_<VarbinaryType, Type, std::shared_ptr<VarbinaryType>> varbinaryType(
      m, "VarbinaryType", py::module_local(asModuleLocalDefinitions));
  py::class_<ArrayType, Type, std::shared_ptr<ArrayType>> arrayType(
      m, "ArrayType", py::module_local(asModuleLocalDefinitions));
  py::class_<MapType, Type, std::shared_ptr<MapType>> mapType(
      m, "MapType", py::module_local(asModuleLocalDefinitions));
  py::class_<RowType, Type, std::shared_ptr<RowType>> rowType(
      m, "RowType", py::module_local(asModuleLocalDefinitions));
  py::class_<FixedSizeArrayType, Type, std::shared_ptr<FixedSizeArrayType>>
      fixedArrayType(
          m, "FixedSizeArrayType", py::module_local(asModuleLocalDefinitions));

  // Basic operations on Type.
  type.def("__str__", &Type::toString);
  // Gcc doesnt support the below kind of templatization.
#if defined(__clang__)
  // Adds equality and inequality comparison operators.
  type.def(py::self == py::self);
  type.def(py::self != py::self);
#endif
  type.def(
      "cpp_size_in_bytes",
      &Type::cppSizeInBytes,
      "Return the C++ size in bytes");
  type.def(
      "is_fixed_width",
      &Type::isFixedWidth,
      "Check if the type is fixed width");
  type.def(
      "is_primitive_type",
      &Type::isPrimitiveType,
      "Check if the type is a primitive type");
  type.def("kind", &Type::kind, "Returns the kind of the type");
  type.def("serialize", &serializeType, "Serializes the type as JSON");
  type.def("__eq__", &Type::equivalent);

  booleanType.def(py::init());
  tinyintType.def(py::init());
  smallintType.def(py::init());
  integerType.def(py::init());
  bigintType.def(py::init());
  realType.def(py::init());
  doubleType.def(py::init());
  varcharType.def(py::init());
  varbinaryType.def(py::init());
  timestampType.def(py::init());
  arrayType.def(py::init<std::shared_ptr<Type>>());
  arrayType.def(
      "element_type", &ArrayType::elementType, "Return the element type");
  fixedArrayType.def(py::init<int, velox::TypePtr>())
      .def("element_type", &velox::FixedSizeArrayType::elementType)
      .def("fixed_width", &velox::FixedSizeArrayType::fixedElementsWidth);
  mapType.def(py::init<std::shared_ptr<Type>, std::shared_ptr<Type>>());
  mapType.def("key_type", &MapType::keyType, "Return the key type");
  mapType.def("value_type", &MapType::valueType, "Return the value type");

  rowType.def(py::init<
              std::vector<std::string>,
              std::vector<std::shared_ptr<const Type>>>());
  rowType.def("size", &RowType::size, "Return the number of columns");
  rowType.def(
      "child_at",
      &RowType::childAt,
      "Return the type of the column at a given index",
      py::arg("idx"));
  rowType.def(
      "find_child",
      [](const std::shared_ptr<RowType>& type, const std::string& name) {
        return type->findChild(name);
      },
      "Return the type of the column with the given name",
      py::arg("name"));
  rowType.def(
      "get_child_idx",
      &RowType::getChildIdx,
      "Return the index of the column with the given name",
      py::arg("name"));
  rowType.def(
      "name_of",
      &RowType::nameOf,
      "Return the name of the column at the given index",
      py::arg("idx"));
  rowType.def("names", &RowType::names, "Return the names of the columns");
}

inline void addVectorBindings(
    py::module& m,
    bool asModuleLocalDefinitions = true) {
  using namespace facebook::velox;
  std::shared_ptr<memory::MemoryPool> pool = memory::getDefaultMemoryPool();

  py::enum_<velox::VectorEncoding::Simple>(
      m, "VectorEncodingSimple", py::module_local(asModuleLocalDefinitions))
      .value("BIASED", velox::VectorEncoding::Simple::BIASED)
      .value("CONSTANT", velox::VectorEncoding::Simple::CONSTANT)
      .value("DICTIONARY", velox::VectorEncoding::Simple::DICTIONARY)
      .value("FLAT", velox::VectorEncoding::Simple::FLAT)
      .value("SEQUENCE", velox::VectorEncoding::Simple::SEQUENCE)
      .value("ROW", velox::VectorEncoding::Simple::ROW)
      .value("MAP", velox::VectorEncoding::Simple::MAP)
      .value("ARRAY", velox::VectorEncoding::Simple::ARRAY)
      .value("LAZY", velox::VectorEncoding::Simple::LAZY)
      .value("FUNCTION", velox::VectorEncoding::Simple::FUNCTION);

  py::class_<BaseVector, VectorPtr>(
      m, "BaseVector", py::module_local(asModuleLocalDefinitions))
      .def("__str__", [](VectorPtr& v) { return v->toString(); })
      .def("__len__", &BaseVector::size)
      .def("size", &BaseVector::size)
      .def(
          "__getitem__",
          [](VectorPtr& v, vector_size_t idx) {
            return getItemFromVector(v, idx);
          })
      .def(
          "__setitem__",
          [](VectorPtr& v, vector_size_t idx, py::handle& obj) {
            setItemInVector(v, idx, obj);
          })
      .def("dtype", &BaseVector::type)
      .def("typeKind", &BaseVector::typeKind)
      .def("mayHaveNulls", &BaseVector::mayHaveNulls)
      .def("isLazy", &BaseVector::isLazy)
      .def(
          "isNullAt",
          [](VectorPtr& v, vector_size_t idx) {
            checkBounds(v, idx);
            return v->isNullAt(idx);
          })
      .def(
          "hashValueAt",
          [](VectorPtr& v, vector_size_t idx) {
            checkBounds(v, idx);
            return v->hashValueAt(idx);
          })
      .def("encoding", &BaseVector::encoding)
      .def("append", [](VectorPtr& u, VectorPtr& v) { appendVectors(u, v); });
  m.def("from_list", [pool](const py::list& list) mutable {
    return pyListToVector(list, pool.get());
  });
}

///  Adds Velox Python Bindings to the module m.
///
/// This function adds the following bindings:
///   * velox::TypeKind enum
///   * velox::Type and its derived types
///   * Basic functions on Type and its derived types.
///
///  @param m Module to add bindings too.
///  @param asModuleLocalDefinitions If true then these bindings are only
///  visible inside the module. Refer to
///  https://pybind11.readthedocs.io/en/stable/advanced/classes.html#module-local-class-bindings
///  for further details.
inline void addVeloxBindings(
    py::module& m,
    bool asModuleLocalDefinitions = true) {
  addDataTypeBindings(m, asModuleLocalDefinitions);
  addVectorBindings(m, asModuleLocalDefinitions);
}

} // namespace facebook::velox::py
