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

#include <cassert>

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <velox/buffer/StringViewBufferHolder.h>
#include <velox/expression/Expr.h>
#include <velox/functions/prestosql/registration/RegistrationFunctions.h>
#include <velox/parse/Expressions.h>
#include <velox/parse/ExpressionsParser.h>
#include <velox/parse/TypeResolver.h>
#include <velox/type/Type.h>
#include <velox/type/Variant.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/DictionaryVector.h>
#include <velox/vector/FlatVector.h>
#include "folly/json.h"
#include "velox/vector/VariantToVector.h"

#include "context.h"

namespace facebook::velox::py {

namespace py = pybind11;

static std::string serializeType(
    const std::shared_ptr<const velox::Type>& type);

template <typename VecPtr>
inline void checkBounds(VecPtr& v, vector_size_t idx) {
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
  } else if (py::isinstance<py::list>(obj)) {
    py::list objAsList = py::cast<py::list>(obj);
    std::vector<velox::variant> result;
    for (auto& item : objAsList) {
      result.push_back(pyToVariant(item));
    }
    return velox::variant::array(std::move(result));
  } else {
    throw py::type_error("Invalid type of object");
  }
}

inline velox::variant pyToVariant(const py::handle& obj, const Type& dtype) {
  if (obj.is_none()) {
    return velox::variant(dtype.kind());
  }
  switch (dtype.kind()) {
    case TypeKind::BOOLEAN: {
      return pyToVariant<velox::TypeKind::BOOLEAN>(obj);
    }
    case TypeKind::TINYINT: {
      return pyToVariant<velox::TypeKind::TINYINT>(obj);
    }
    case TypeKind::SMALLINT: {
      return pyToVariant<velox::TypeKind::SMALLINT>(obj);
    }
    case TypeKind::INTEGER: {
      return pyToVariant<velox::TypeKind::INTEGER>(obj);
    }
    case TypeKind::BIGINT: {
      return pyToVariant<velox::TypeKind::BIGINT>(obj);
    }
    case TypeKind::REAL: {
      return pyToVariant<velox::TypeKind::REAL>(obj);
    }
    case TypeKind::DOUBLE: {
      return pyToVariant<velox::TypeKind::DOUBLE>(obj);
    }
    case TypeKind::VARCHAR: {
      return pyToVariant<velox::TypeKind::VARCHAR>(obj);
    }
    case TypeKind::VARBINARY: {
      return pyToVariant<velox::TypeKind::VARBINARY>(obj);
    }
    case TypeKind::TIMESTAMP: {
      return pyToVariant<velox::TypeKind::TIMESTAMP>(obj);
    }
    default:
      throw py::type_error("Unsupported type supplied");
  }
}

inline void checkRowVectorBounds(const RowVectorPtr& v, vector_size_t idx) {
  if (idx < 0 || size_t(idx) >= v->childrenSize()) {
    throw std::out_of_range("Index out of range");
  }
}

bool compareRowVector(const RowVectorPtr& u, const RowVectorPtr& v) {
  CompareFlags compFlags =
      CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

  if (u->size() != v->size()) {
    return false;
  }
  for (size_t i = 0; i < u->size(); i++) {
    if (u->compare(v.get(), i, i, compFlags) != 0) {
      return false;
    }
  }

  return true;
}

inline std::string rowVectorToString(const RowVectorPtr& vector) {
  return vector->toString(0, vector->size());
}

static VectorPtr pyToConstantVector(
    const py::handle& obj,
    vector_size_t length,
    facebook::velox::memory::MemoryPool* pool,
    TypePtr type = nullptr);

template <TypeKind T>
static VectorPtr variantsToFlatVector(
    const std::vector<velox::variant>& variants,
    facebook::velox::memory::MemoryPool* pool);

static inline VectorPtr pyListToVector(
    const py::list& list,
    facebook::velox::memory::MemoryPool* pool);

static inline VectorPtr pyListToVector(
    const py::list& list,
    const Type& dtype,
    facebook::velox::memory::MemoryPool* pool);

template <TypeKind T>
static VectorPtr createDictionaryVector(
    BufferPtr baseVector,
    VectorPtr values,
    facebook::velox::memory::MemoryPool* pool) {
  using NativeType = typename TypeTraits<T>::NativeType;
  size_t length = baseVector->size() / sizeof(vector_size_t);
  return std::make_shared<DictionaryVector<NativeType>>(
      pool,
      /*nulls=*/nullptr,
      length,
      std::move(values),
      std::move(baseVector));
}

template <typename NativeType>
inline py::object getItemFromSimpleVector(
    SimpleVectorPtr<NativeType>& v,
    vector_size_t idx);

template <typename NativeType>
inline void setItemInFlatVector(
    FlatVectorPtr<NativeType>& v,
    vector_size_t idx,
    py::handle& obj);

inline void appendVectors(VectorPtr& u, VectorPtr& v) {
  if (u->typeKind() != v->typeKind()) {
    throw py::type_error("Tried to append vectors of two different types");
  }
  u->append(v.get());
}

static VectorPtr evaluateExpression(
    std::shared_ptr<const facebook::velox::core::IExpr>& expr,
    std::vector<std::string> names,
    std::vector<VectorPtr>& inputs);

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

struct DictionaryIndices {
  const BufferPtr indices;
};

template <>
inline void checkBounds(DictionaryIndices& indices, vector_size_t idx) {
  if (idx < 0 || idx >= (indices.indices->size() / sizeof(vector_size_t))) {
    throw std::out_of_range("Index out of range");
  }
}

// Currently PyVelox will only register vectors for primitive types.
template <TypeKind T>
static void registerTypedVectors(
    py::module& m,
    bool asModuleLocalDefinitions = true) {
  using NativeType = typename TypeTraits<T>::NativeType;
  const std::string typeName = TypeTraits<T>::name;
  py::class_<SimpleVector<NativeType>, SimpleVectorPtr<NativeType>, BaseVector>(
      m,
      ("SimpleVector_" + typeName).c_str(),
      py::module_local(asModuleLocalDefinitions))
      .def(
          "__getitem__",
          [](SimpleVectorPtr<NativeType> v, vector_size_t idx) {
            return getItemFromSimpleVector(v, idx);
          })
      .def(
          "__getitem__",
          [](std::shared_ptr<SimpleVector<NativeType>> v, py::slice slice) {
            size_t start, stop, step, length;
            if (!slice.compute(v->size(), &start, &stop, &step, &length)) {
              throw py::error_already_set();
            }
            if (step != 1) {
              PyErr_SetString(
                  PyExc_NotImplementedError,
                  "Slicing with step other than 1 is not supported");
              throw py::error_already_set();
            }
            return v->slice(start, length);
          });

  py::class_<
      FlatVector<NativeType>,
      FlatVectorPtr<NativeType>,
      SimpleVector<NativeType>>(
      m,
      ("FlatVector_" + typeName).c_str(),
      py::module_local(asModuleLocalDefinitions))
      .def(
          "__setitem__",
          [](FlatVectorPtr<NativeType> v, vector_size_t idx, py::handle& obj) {
            setItemInFlatVector(v, idx, obj);
          });

  py::class_<
      ConstantVector<NativeType>,
      ConstantVectorPtr<NativeType>,
      SimpleVector<NativeType>>(
      m,
      ("ConstantVector_" + typeName).c_str(),
      py::module_local(asModuleLocalDefinitions));

  py::class_<
      DictionaryVector<NativeType>,
      DictionaryVectorPtr<NativeType>,
      SimpleVector<NativeType>>(
      m,
      ("DictionaryVector_" + typeName).c_str(),
      py::module_local(asModuleLocalDefinitions))
      .def(
          "indices",
          [](DictionaryVectorPtr<NativeType> vec) {
            return DictionaryIndices{vec->indices()};
          })
      .def("values", &DictionaryVector<NativeType>::valueVector);
}

static void addVectorBindings(
    py::module& m,
    bool asModuleLocalDefinitions = true) {
  using namespace facebook::velox;

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
      .def("append", [](VectorPtr& u, VectorPtr& v) { appendVectors(u, v); })
      .def("resize", &BaseVector::resize)
      .def(
          "slice",
          [](VectorPtr& u,
             vector_size_t start,
             vector_size_t stop,
             vector_size_t step) {
            if (step != 1) {
              PyErr_SetString(
                  PyExc_NotImplementedError,
                  "Slicing with step other than 1 is not supported");
              throw py::error_already_set();
            }
            return u->slice(start, stop - start);
          },
          py::arg("start"),
          py::arg("stop"),
          py::arg("step") = 1);

  py::class_<ArrayVector, ArrayVectorPtr, BaseVector>(
      m, "ArrayVector", py::module_local(asModuleLocalDefinitions))
      .def("elements", [](ArrayVectorPtr vec) -> VectorPtr {
        return vec->elements();
      });

  constexpr TypeKind supportedTypes[] = {
      TypeKind::BOOLEAN,
      TypeKind::TINYINT,
      TypeKind::SMALLINT,
      TypeKind::INTEGER,
      TypeKind::BIGINT,
      TypeKind::REAL,
      TypeKind::DOUBLE,
      TypeKind::VARBINARY,
      TypeKind::TIMESTAMP};

  for (int i = 0; i < sizeof(supportedTypes) / sizeof(supportedTypes[0]); i++) {
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        registerTypedVectors, supportedTypes[i], m, asModuleLocalDefinitions);
  }

  py::class_<DictionaryIndices>(
      m, "DictionaryIndices", py::module_local(asModuleLocalDefinitions))
      .def(
          "__len__",
          [](DictionaryIndices indices) {
            return (indices.indices->size()) / sizeof(vector_size_t);
          })
      .def("__getitem__", [](DictionaryIndices indices, vector_size_t idx) {
        checkBounds(indices, idx);
        return indices.indices->as<vector_size_t>()[idx];
      });
  m.def(
      "from_list",
      [](const py::list& list, const Type* dtype = nullptr) mutable {
        if (!dtype || py::isinstance<py::none>(py::cast(*dtype))) {
          return pyListToVector(
              list, PyVeloxContext::getSingletonInstance().pool());
        } else {
          return pyListToVector(
              list, *dtype, PyVeloxContext::getSingletonInstance().pool());
        }
      },
      py::arg("list"),
      py::arg("dtype") = nullptr);
  m.def(
      "constant_vector",
      [](const py::handle& obj, vector_size_t length, TypePtr type) {
        return pyToConstantVector(
            obj, length, PyVeloxContext::getSingletonInstance().pool(), type);
      },
      py::arg("value"),
      py::arg("length"),
      py::arg("type") = nullptr);

  m.def(
      "dictionary_vector",
      [](VectorPtr baseVector, const py::list& indices_list) {
        BufferPtr indices_buffer = AlignedBuffer::allocate<vector_size_t>(
            indices_list.size(), PyVeloxContext::getSingletonInstance().pool());
        vector_size_t* indices_ptr = indices_buffer->asMutable<vector_size_t>();
        for (size_t i = 0; i < indices_list.size(); i++) {
          if (!py::isinstance<py::int_>(indices_list[i]))
            throw py::type_error("Found an index that's not an integer");
          vector_size_t idx = py::cast<vector_size_t>(indices_list[i]);
          checkBounds(baseVector, idx);
          indices_ptr[i] = py::cast<vector_size_t>(indices_list[i]);
        }
        return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createDictionaryVector,
            baseVector->typeKind(),
            std::move(indices_buffer),
            std::move(baseVector),
            PyVeloxContext::getSingletonInstance().pool());
      });

  m.def(
      "row_vector",
      [](std::vector<std::string>& names,
         std::vector<VectorPtr>& children,
         const std::optional<py::dict>& nullabilityDict) {
        if (children.size() == 0 || names.size() == 0) {
          throw py::value_error("RowVector must have children.");
        }
        std::vector<std::shared_ptr<const Type>> childTypes;
        childTypes.reserve(children.size());

        size_t vectorSize = children[0]->size();
        for (int i = 0; i < children.size(); i++) {
          if (i > 0 && children[i]->size() != vectorSize) {
            PyErr_SetString(PyExc_ValueError, "Each child must have same size");
            throw py::error_already_set();
          }
          childTypes.push_back(children[i]->type());
        }
        auto rowType = ROW(std::move(names), std::move(childTypes));

        BufferPtr nullabilityBuffer = nullptr;
        if (nullabilityDict.has_value()) {
          auto nullabilityValues = nullabilityDict.value();
          nullabilityBuffer = AlignedBuffer::allocate<bool>(
              vectorSize, PyVeloxContext::getSingletonInstance().pool(), true);
          for (const auto&& item : nullabilityValues) {
            auto row = item.first;
            auto nullability = item.second;
            if (!py::isinstance<py::int_>(row) ||
                !py::isinstance<py::bool_>(nullability)) {
              throw py::type_error(
                  "Nullability must be a dictionary, rowId in int and nullability in boolean.");
            }
            int rowId = py::cast<int>(row);
            if (!(rowId >= 0 && rowId < vectorSize)) {
              throw py::type_error("Nullability index out of bounds.");
            }
            bool nullabilityVal = py::cast<bool>(nullability);
            bits::setBit(
                nullabilityBuffer->asMutable<uint64_t>(),
                rowId,
                bits::kNull ? nullabilityVal : !nullabilityVal);
          }
        }

        return std::make_shared<RowVector>(
            PyVeloxContext::getSingletonInstance().pool(),
            rowType,
            nullabilityBuffer,
            vectorSize,
            children);
      },
      py::arg("names"),
      py::arg("children"),
      py::arg("nullability") = std::nullopt);

  py::class_<RowVector, BaseVector, RowVectorPtr>(
      m, "RowVector", py::module_local(asModuleLocalDefinitions))
      .def(
          "__len__",
          [](RowVectorPtr& v) {
            return v->childrenSize() > 0 ? v->childAt(0)->size() : 0;
          })
      .def("__str__", [](RowVectorPtr& v) { return rowVectorToString(v); })
      .def("__eq__", [](RowVectorPtr& u, RowVectorPtr& v) {
        return compareRowVector(u, v);
      });
}

static void addExpressionBindings(
    py::module& m,
    bool asModuleLocalDefinitions = true);

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
  google::InitGoogleLogging("pyvelox");
  FLAGS_minloglevel = 3; // To disable log spam when throwing an exception
  addDataTypeBindings(m, asModuleLocalDefinitions);
  addVectorBindings(m, asModuleLocalDefinitions);
  addExpressionBindings(m, asModuleLocalDefinitions);
  auto atexit = py::module_::import("atexit");
  atexit.attr("register")(
      py::cpp_function([]() { PyVeloxContext::cleanup(); }));
}

} // namespace facebook::velox::py
