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
#include "folly/container/F14Set.h"
#include "folly/hash/Hash.h"

#include "velox/common/memory/Arena.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/sparksql/Comparisons.h"
#include "velox/type/Filter.h"

namespace facebook::velox::functions::sparksql {
namespace {

template <typename T>
class Set : public folly::F14FastSet<T, folly::hasher<T>, Equal<T>> {};

template <>
class Set<StringView> {
 public:
  using value_type = std::string_view;

  void emplace(const StringView& s) {
    std::string_view sv(s.data(), s.size());
    if (!set_.contains(sv)) {
      set_.emplace(arena_.writeString(sv));
    }
  }

  bool contains(const StringView& s) const {
    return set_.contains(std::string_view(s.data(), s.size()));
  }

  void reserve(size_t size) {
    set_.reserve(size);
  }

  size_t size() const {
    return set_.size();
  }

  auto begin() const {
    return set_.begin();
  }

 private:
  Arena arena_;
  folly::F14FastSet<std::string_view> set_;
};

template <typename T, bool rhsHasNull>
class In final : public exec::VectorFunction {
 public:
  explicit In(Set<T> elements) : elements_(std::move(elements)) {}

 private:
  bool isDefaultNullBehavior() const final {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* resultRef) const final {
    // Prepare result.
    BaseVector::ensureWritable(rows, BOOLEAN(), context->pool(), resultRef);
    FlatVector<bool>& result = *(*resultRef)->as<FlatVector<bool>>();
    uint64_t* resultValues = result.mutableRawValues<uint64_t>();
    uint64_t* resultNulls = result.mutableRawNulls();
    // Handle NULLs.
    const SelectivityVector* selected = &rows;
    exec::LocalSelectivityVector localSelected(context);
    if (args[0]->mayHaveNulls()) {
      const uint64_t* const lhsNulls = args[0]->flatRawNulls(rows);
      *localSelected.get(rows.end()) = rows; // Allocate and copy input rows.
      localSelected->deselectNonNulls(lhsNulls, rows.begin(), rows.end());
      localSelected->applyToSelected( // Set result null if input is null.
          [&](vector_size_t i) { bits::setNull(resultNulls, i); });
      *localSelected = rows; // NULLs now handled and can be deselected.
      localSelected->deselectNulls(lhsNulls, rows.begin(), rows.end());
      selected = localSelected.get();
    }
    if (args[0]->isConstant(rows)) {
      auto* lhs = args[0]->as<SimpleVector<T>>();
      if (lhs->isNullAt(rows.begin())) {
        return;
      }
      const bool present = elements_.contains(lhs->valueAt(rows.begin()));
      selected->applyToSelected([&](vector_size_t i) {
        bits::setBit(resultValues, i, present);
        if (rhsHasNull && !present) {
          bits::setNull(resultNulls, i);
        }
      });
      return;
    }
    VELOX_CHECK_EQ(args[0]->encoding(), VectorEncoding::Simple::FLAT);
    FlatVector<T>* lhs = args[0]->as<FlatVector<T>>();
    if (elements_.size() != 1 || rhsHasNull) {
      selected->applyToSelected([&](vector_size_t i) {
        const bool present = elements_.contains(lhs->valueAt(i));
        bits::setBit(resultValues, i, present);
        if (rhsHasNull && !present) {
          bits::setNull(resultNulls, i);
        }
      });
    } else {
      using V = typename Set<T>::value_type;
      V value(*elements_.begin());
      Equal<V> cmp;
      selected->applyToSelected([&](vector_size_t i) {
        bits::setBit(resultValues, i, cmp(value, V(lhs->valueAt(i))));
      });
    }
  }

  const Set<T> elements_;
};

template <typename T>
std::unique_ptr<exec::VectorFunction> createIn(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto* constantInput = dynamic_cast<const ConstantVector<ComplexType>*>(
      inputArgs[1].constantValue.get());
  auto arrayVector = dynamic_cast<const ArrayVector*>(
      constantInput->valueVector()->wrappedVector());
  auto elementsVector = arrayVector->elements()->as<SimpleVector<T>>();
  auto offset = arrayVector->offsetAt(constantInput->index());
  auto size = arrayVector->sizeAt(constantInput->index());
  VELOX_USER_CHECK_GT(size, 0, "IN list must not be empty");

  Set<T> elements;
  elements.reserve(size);
  bool hasNull = false;
  for (auto i = offset; i < offset + size; i++) {
    if (elementsVector->isNullAt(i)) {
      hasNull = true;
    } else {
      elements.emplace(elementsVector->valueAt(i));
    }
  }

  if (hasNull) {
    return std::make_unique<In<T, true>>(std::move(elements));
  } else {
    return std::make_unique<In<T, false>>(std::move(elements));
  }
}

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> inSignatures() {
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("boolean")
              .argumentType("T")
              .argumentType("array(T)")
              .variableArity()
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeIn(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_USER_CHECK_EQ(inputArgs.size(), 2);
  VELOX_USER_CHECK_EQ(inputArgs[1].type->kind(), TypeKind::ARRAY);

  // Type-invariant checks.
  VELOX_USER_CHECK(
      *inputArgs[0].type == *inputArgs[1].type->childAt(0),
      "Second argument to {} must be an ARRAY with element type matching the type of the first argument: {} vs. {}",
      name,
      inputArgs[0].type->toString(),
      inputArgs[1].type->toString());
  VELOX_USER_CHECK_NOT_NULL(
      inputArgs[1].constantValue,
      "Second argument to {} must be constant.",
      name)

  switch (inputArgs[0].type->kind()) {
#define CASE(kind)     \
  case TypeKind::kind: \
    return createIn<TypeTraits<TypeKind::kind>::NativeType>(inputArgs);
    CASE(BOOLEAN);
    CASE(TINYINT);
    CASE(SMALLINT);
    CASE(INTEGER);
    CASE(BIGINT);
    CASE(VARCHAR);
    CASE(VARBINARY);
    CASE(REAL);
    CASE(DOUBLE);
    CASE(TIMESTAMP);
#undef CASE
    default:
      VELOX_NYI("{} does not support {}", name, inputArgs[0].type->toString());
  }
}

} // namespace facebook::velox::functions::sparksql
