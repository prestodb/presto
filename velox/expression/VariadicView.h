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
#include "velox/expression/ComplexViewTypes.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::exec {
template <typename T>
struct VectorReader;

// Represents an arbitrary number of arguments of the same type with an
// interface similar to std::vector.
template <bool returnsOptionalValues, typename T>
class VariadicView {
  using reader_t = VectorReader<T>;
  using element_t = typename std::conditional<
      returnsOptionalValues,
      typename reader_t::exec_in_t,
      typename reader_t::exec_null_free_in_t>::type;

 public:
  VariadicView(
      const std::vector<std::unique_ptr<reader_t>>* readers,
      vector_size_t offset)
      : readers_(readers), offset_(offset) {}

  using Element = typename std::
      conditional<returnsOptionalValues, OptionalAccessor<T>, element_t>::type;

  class ElementAccessor {
   public:
    using element_t = Element;
    using index_t = int;

    ElementAccessor(
        const std::vector<std::unique_ptr<reader_t>>* readers,
        vector_size_t offset)
        : readers_(readers), offset_(offset) {}

    Element operator()(vector_size_t index) const {
      if constexpr (returnsOptionalValues) {
        return Element{(*readers_)[index].get(), offset_};
      } else {
        return (*readers_)[index]->readNullFree(offset_);
      }
    }

   private:
    const std::vector<std::unique_ptr<reader_t>>* readers_;
    // This is only not const to support the assignment operator.
    vector_size_t offset_;
  };

  using Iterator = IndexBasedIterator<ElementAccessor>;

  Iterator begin() const {
    return Iterator{
        0, 0, (int)readers_->size(), ElementAccessor(readers_, offset_)};
  }

  Iterator end() const {
    return Iterator{
        (int)readers_->size(),
        0,
        (int)readers_->size(),
        ElementAccessor(readers_, offset_)};
  }

  struct SkipNullsContainer {
    class SkipNullsBaseIterator : public Iterator {
     public:
      SkipNullsBaseIterator(
          const std::vector<std::unique_ptr<reader_t>>* readers,
          size_t readerIndex,
          vector_size_t offset)
          : Iterator(
                readerIndex,
                0,
                readers->size(),
                ElementAccessor(readers, offset)),
            readers_(readers),
            offset_(offset) {}

      bool hasValue() const {
        const auto& currReader = readers_->operator[](this->index_);
        return currReader->isSet(offset_);
      }

      element_t value() const {
        const auto& currReader = readers_->operator[](this->index_);
        return (*currReader)[offset_];
      }

     private:
      const std::vector<std::unique_ptr<reader_t>>* readers_;
      vector_size_t offset_;
    };

    explicit SkipNullsContainer(const VariadicView* view) : view_(view) {}

    SkipNullsIterator<SkipNullsBaseIterator> begin() {
      return SkipNullsIterator<SkipNullsBaseIterator>::initialize(
          SkipNullsBaseIterator{view_->readers_, 0, view_->offset_},
          SkipNullsBaseIterator{
              view_->readers_, view_->readers_->size(), view_->offset_});
    }

    SkipNullsIterator<SkipNullsBaseIterator> end() {
      return SkipNullsIterator<SkipNullsBaseIterator>{
          SkipNullsBaseIterator{
              view_->readers_, view_->readers_->size(), view_->offset_},
          SkipNullsBaseIterator{
              view_->readers_, view_->readers_->size(), view_->offset_}};
    }

   private:
    const VariadicView* view_;
  };

  // Returns true if any of the arguments in the vector might have null
  // element.
  bool mayHaveNulls() const {
    if constexpr (returnsOptionalValues) {
      for (const auto* reader : readers_) {
        if (reader->mayHaveNulls()) {
          return true;
        }
      }
    }

    return false;
  }

  Element operator[](size_t index) const {
    if constexpr (returnsOptionalValues) {
      return Element{(*readers_)[index].get(), offset_};
    } else {
      return (*readers_)[index]->readNullFree(offset_);
    }
  }

  Element at(size_t index) const {
    return (*this)[index];
  }

  size_t size() const {
    return readers_->size();
  }

  SkipNullsContainer skipNulls() {
    if constexpr (returnsOptionalValues) {
      return SkipNullsContainer{this};
    }

    VELOX_UNSUPPORTED(
        "VariadicViews over NULL-free data do not support skipNulls().  It's "
        "already been checked that this object contains no NULLs, it's more "
        "efficient to use the standard iterator interface.");
  }

 private:
  const std::vector<std::unique_ptr<reader_t>>* readers_;
  const vector_size_t offset_;
};
} // namespace facebook::velox::exec
