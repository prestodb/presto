/*
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

#include <deque>
#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql {
namespace {

/// Wrapper class providing some utility functions to work with ArrayVector
/// elements and meta data
/// \tparam T the expected vector element type
template <typename T>
class ArrayElementsWrapper {
 public:
  /// Initialize the meta data and raw data
  /// \param rows SelectivityVector of the parent array vector
  /// \param context the context used for memory allocation
  /// \param input the input if needed to share buffers
  ArrayElementsWrapper(
      const SelectivityVector& rows,
      exec::EvalCtx* context,
      const VectorPtr& input)
      : elements_capacity_{3 * rows.end() /* Initial estimate size */} {
    // Create the result offsets and sizes
    offsets_ =
        AlignedBuffer::allocate<vector_size_t>(rows.size(), context->pool());
    rawOffsets_ = offsets_->asMutable<vector_size_t>();

    sizes_ =
        AlignedBuffer::allocate<vector_size_t>(rows.size(), context->pool());
    rawSizes_ = sizes_->asMutable<vector_size_t>();

    // Allocate vector of elements

    auto nestedVector = BaseVector::create(
        CppToType<T>::create(), elements_capacity_, context->pool());

    elementsVector_ = std::dynamic_pointer_cast<FlatVector<T>>(nestedVector);
    if constexpr (std::is_same_v<T, StringView>) {
      // Output elements vector produced will reference parts of the input
      // So increment ref count of the string buffer of the input vector
      if (input->asFlatVector<T>()) {
        elementsVector_->setStringBuffers(
            input->asFlatVector<T>()->stringBuffers());
      }
    }

    rawValues_ = elementsVector_->mutableRawValues();
  }

  /// Update a given value in the rawData array and resizes the vector if needed
  /// \param value the input value to be inserted
  /// \param index the index to the raw data
  void updateRawData(const T& value, vector_size_t index) {
    static const vector_size_t kResizeDoublingThreshold = 10000;
    if (index >= elements_capacity_) {
      // Double until reaching the threshold
      // Then slow down and grow only 20% each time
      if (elements_capacity_ < kResizeDoublingThreshold) {
        elements_capacity_ = 2 * index;
      } else {
        elements_capacity_ = index + index / 5;
      }
      elementsVector_->resize(elements_capacity_);
      rawValues_ = elementsVector_->mutableRawValues();
    }
    rawValues_[index] = value;
  }

  const BufferPtr& offsets() const {
    return offsets_;
  }

  const BufferPtr& sizes() const {
    return sizes_;
  }

  vector_size_t* rawOffsets() const {
    return rawOffsets_;
  }

  vector_size_t* rawSizes() const {
    return rawSizes_;
  }

  VectorPtr elementsVector() const {
    return elementsVector_;
  }

  T* rawValues() const {
    return rawValues_;
  }

 private:
  vector_size_t elements_capacity_;
  BufferPtr offsets_;
  BufferPtr sizes_;
  vector_size_t* rawOffsets_;
  vector_size_t* rawSizes_;
  FlatVectorPtr<T> elementsVector_;
  T* rawValues_;
};

/// This class only implements the basic split version in which the pattern is a
/// single character
class SplitCharacter final : public exec::VectorFunction {
 public:
  explicit SplitCharacter(const char pattern) : pattern_{pattern} {
    static const std::string_view kRegexChars = ".$|()[{^?*+\\";
    VELOX_CHECK(
        kRegexChars.find(pattern) == std::string::npos,
        "This version of split support single-length non-regex patterns");
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* unused */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    auto& stringVector = args[0];

    // Decode the input
    exec::LocalDecodedVector input(context, *stringVector, rows);

    ArrayElementsWrapper<StringView> elementsWrapper(
        rows, context, stringVector);

    // Keep track of the result offset
    vector_size_t lastOffset = 0;
    rows.applyToSelected([&](vector_size_t row) {
      // Updating offsets
      elementsWrapper.rawOffsets()[row] = lastOffset;

      // Searching the current string and updating the result nested string
      // vector
      vector_size_t matches = 0;
      auto current = input.get()->valueAt<StringView>(row);
      vector_size_t lastIndex = 0;
      for (auto index = 0; index < current.size(); index++) {
        if (current.data()[index] == pattern_) {
          elementsWrapper.updateRawData(
              StringView(current.data() + lastIndex, index - lastIndex),
              lastOffset + matches);
          lastIndex = index + 1;
          matches++;
        }
      }
      // Add the last string
      elementsWrapper.updateRawData(
          StringView(current.data() + lastIndex, current.size() - lastIndex),
          lastOffset + matches);
      matches++;

      // Updating sizes
      lastOffset += matches;
      elementsWrapper.rawSizes()[row] = matches;
    });

    // Assemble the result vector
    auto arrayResult = std::make_shared<ArrayVector>(
        context->pool(),
        ARRAY(VARCHAR()),
        BufferPtr(nullptr),
        rows.size(),
        elementsWrapper.offsets(),
        elementsWrapper.sizes(),
        elementsWrapper.elementsVector(),
        0);

    context->moveOrCopyResult(arrayResult, rows, result);
  }

 private:
  const char pattern_;
};

/// This class will be updated in the future as we support more variants of
/// split
class Split final : public exec::VectorFunction {
 public:
  Split() {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* unused */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    auto delimiterVector = args[1]->as<ConstantVector<StringView>>();
    VELOX_CHECK(
        delimiterVector, "Split function supports only constant delimiter");
    auto patternString = args[1]->as<ConstantVector<StringView>>()->valueAt(0);
    VELOX_CHECK_EQ(
        patternString.size(),
        1,
        "split only supports only single-character pattern");
    char pattern = patternString.data()[0];
    SplitCharacter splitCharacter(pattern);
    splitCharacter.apply(rows, args, nullptr, context, result);
  }
};

/// The function returns specialized version of split based on the constant
/// inputs.
/// \param inputArgs the inputs types (VARCHAR, VARCHAR, int64) and constant
///     values (if provided).
std::shared_ptr<exec::VectorFunction> createSplit(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  BaseVector* constantPattern = inputArgs[1].constantValue.get();

  if (inputArgs.size() > 3 || inputArgs[0].type->isVarchar() ||
      inputArgs[1].type->isVarchar() || (constantPattern == nullptr)) {
    return std::make_shared<Split>();
  }
  auto pattern = constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
  if (pattern.size() != 1) {
    return std::make_shared<Split>();
  }
  char charPattern = pattern.data()[0];
  // TODO: Add support for zero-length pattern, 2-character pattern
  // TODO: add support for general regex pattern using R2
  return std::make_shared<SplitCharacter>(charPattern);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // varchar, varchar -> array(varchar)
  return {exec::FunctionSignatureBuilder()
              .returnType("array(varchar)")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_regexp_split,
    signatures(),
    createSplit);
} // namespace facebook::velox::functions::sparksql
