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
template <typename T>
class VariadicView {
  using reader_t = VectorReader<T>;
  using element_t = typename reader_t::exec_in_t;

 public:
  VariadicView(
      const std::vector<std::unique_ptr<reader_t>>* readers,
      vector_size_t offset)
      : readers_(readers), offset_(offset) {}

  using Element = VectorOptionalValueAccessor<reader_t>;

  class Iterator : public IndexBasedIterator<Element> {
   public:
    Iterator(
        const std::vector<std::unique_ptr<reader_t>>* readers,
        size_t readerIndex,
        vector_size_t offset)
        : IndexBasedIterator<Element>(readerIndex),
          readers_(readers),
          offset_(offset) {}

    PointerWrapper<Element> operator->() const {
      return PointerWrapper(Element{(*readers_)[this->index_].get(), offset_});
    }

    Element operator*() const {
      return Element{(*readers_)[this->index_].get(), offset_};
    }

   protected:
    const std::vector<std::unique_ptr<reader_t>>* readers_;
    const vector_size_t offset_;
  };

  Iterator begin() const {
    return Iterator{readers_, 0, offset_};
  }

  Iterator end() const {
    return Iterator{readers_, readers_->size(), offset_};
  }

  struct SkipNullsContainer {
    class SkipNullsBaseIterator : public Iterator {
     public:
      SkipNullsBaseIterator(
          const std::vector<std::unique_ptr<reader_t>>* readers,
          size_t readerIndex,
          vector_size_t offset)
          : Iterator(readers, readerIndex, offset) {}

      bool hasValue() const {
        const auto& currReader = this->readers_->operator[](this->index_);
        return currReader->isSet(this->offset_);
      }

      element_t value() const {
        const auto& currReader = this->readers_->operator[](this->index_);
        return (*currReader)[this->offset_];
      }
    };

    explicit SkipNullsContainer(const VariadicView<T>* view) : view_(view) {}

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
    const VariadicView<T>* view_;
  };

  // Returns true if any of the arguments in the vector might have null
  // element.
  bool mayHaveNulls() const {
    for (const auto* reader : readers_) {
      if (reader->mayHaveNulls()) {
        return true;
      }
    }

    return false;
  }

  Element operator[](size_t index) const {
    return Element{(*readers_)[index].get(), offset_};
  }

  Element at(size_t index) const {
    return (*this)[index];
  }

  size_t size() const {
    return readers_->size();
  }

  SkipNullsContainer skipNulls() {
    return SkipNullsContainer{this};
  }

 private:
  const std::vector<std::unique_ptr<reader_t>>* readers_;
  const vector_size_t offset_;
};
} // namespace facebook::velox::exec
