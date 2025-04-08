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

#include "complex.h"
#include "velox/vector/ComplexVector.h"

#include <functional>

namespace facebook::velox::py {

using namespace velox;
namespace py = pybind11;

namespace {
// Structure used to keep check on the number
// of constituent elements. Attributes totalElements
// and insertedElements keeps the length of the vector, and
// the number of elements inserted during the operation
// respectively.
struct ElementCounter {
  vector_size_t insertedElements =
      0; // to track the elements already in the vector
  vector_size_t totalElements = 0;
  std::vector<ElementCounter> children;
};
} // namespace

void checkOrAssignType(TypePtr& type, const TypePtr& expected_type) {
  if (type->kind() == TypeKind::UNKNOWN) {
    type = expected_type;
  } else if (!(type->kindEquals(expected_type))) {
    throw py::type_error(
        "Cannot construct type tree, invalid variant for complex type");
  }
}

template <TypeKind Kind>
void setElementInFlatVector(
    vector_size_t idx,
    const variant& v,
    VectorPtr& vector) {
  using NativeType = typename TypeTraits<Kind>::NativeType;
  auto asFlat = vector->asFlatVector<NativeType>();
  asFlat->set(idx, NativeType{v.value<NativeType>()});
}

// This function determines the type and the number of elements for a variant.
// Takes reference to Type and ElementCounter which will be set after the run.
// It is supposed to run a recursive call with a pre-instantiated TypePtr,
// the target variant and the counter. The passed variant is checked for its
// data type, and for any complex type involved, the function is called again.
// The counter here is used to keep in track of the number of elements inserted
// and the number of types of elements allowed if a complex vector is involved
// in the variant.
void constructType(const variant& v, TypePtr& type, ElementCounter& counter) {
  ++counter.totalElements;

  if (v.isNull()) {
    // since the variant is NULL, we can't infer the data type
    // thus it maybe UNKNOWN or INVALID at this stage
    // which implies further investigation is required
    if (v.kind() != TypeKind::UNKNOWN && v.kind() != TypeKind::INVALID &&
        v.kind() != type->kind()) {
      throw std::invalid_argument("Variant was of an unexpected kind");
    }
    return;
  } else {
    // if a Non-Null variant's type is unknown or not one of the valid
    // types which are supported then the Type tree cannot be constructed
    if (v.kind() == TypeKind::UNKNOWN || v.kind() == TypeKind::INVALID) {
      throw std::invalid_argument(
          "Non-null variant has unknown or invalid kind");
    }

    switch (v.kind()) {
      case TypeKind::ARRAY: {
        counter.children.resize(1);
        auto asArray = v.array();
        TypePtr childType = createType(TypeKind::UNKNOWN, {});
        for (const auto& element : asArray) {
          constructType(element, childType, counter.children[0]);
        }

        // if child's type still remains Unknown, implies all the
        // elements in the array are actually NULL
        if (childType->kind() == TypeKind::UNKNOWN) {
          throw py::value_error("Cannot construct array with all None values");
        }
        checkOrAssignType(type, createType<TypeKind::ARRAY>({childType}));
        break;
      }

      default: {
        checkOrAssignType(type, createScalarType(v.kind()));
        break;
      }
    }
  }
}

// Function is called with the variant to be added,
// the target vector and the element counter. The element counter
// is used to track the number of elements already inserted, so as
// to get the index for the next element to insert. For an array
// vector, the required offset and size is first set into the vector
// then the function is called recursively for the contained elements.
// In the default case where the variant is a scalar type, the
// setElementInFlatVector is called without any further recursion.
static void insertVariantIntoVector(
    const variant& v,
    VectorPtr& vector,
    ElementCounter& counter,
    vector_size_t previous_size,
    vector_size_t previous_offset) {
  if (v.isNull()) {
    vector->setNull(counter.insertedElements, true);
  } else {
    switch (v.kind()) {
      case TypeKind::ARRAY: {
        auto asArray = vector->as<ArrayVector>();
        asArray->elements()->resize(counter.children[0].totalElements);
        const std::vector<variant>& elements = v.array();
        vector_size_t offset = previous_offset + previous_size;
        vector_size_t size = elements.size();
        asArray->setOffsetAndSize(counter.insertedElements, offset, size);
        for (const variant& elt : elements) {
          insertVariantIntoVector(
              elt, asArray->elements(), counter.children[0], offset, size);
        }

        break;
      }
      default: {
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            setElementInFlatVector,
            v.kind(),
            counter.insertedElements,
            v,
            vector);
        break;
      }
    }
  }
  counter.insertedElements += 1;
}

VectorPtr variantsToVector(
    const std::vector<variant>& variants,
    velox::memory::MemoryPool* pool) {
  ElementCounter counter;
  TypePtr type = createType(TypeKind::UNKNOWN, {});
  for (const auto& variant : variants) {
    constructType(variant, type, counter);
  }
  VectorPtr resultVector =
      BaseVector::create(std::move(type), variants.size(), pool);
  for (const variant& v : variants) {
    insertVariantIntoVector(
        v,
        resultVector,
        counter,
        /*previous_size*/ 0,
        /*previous_offset*/ 0);
  }
  return resultVector;
}

} // namespace facebook::velox::py
