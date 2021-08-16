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

#include "velox/functions/lib/DynamicFlatVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::functions {

// ArrayBuilder is a helper class for building array vectors. The number of
// arrays must be specified up front, but the elements array will resize
// dynamically. The template parameter should be a tag (Varchar, Varbinary)
// not native type.
//
// Start a new array by calling startArray. This returns a Ref proxy object that
// implements a (very small) subset of the std::vector interface. When the Ref
// object goes out of scope, the array is committed. Only one Ref can be alive
// at a time. Expanding coverage of the std::vector interface will require
// corresponding changes to DynamicFlatVector.
//
// Usage example (note buffer management is still required for strings):
//
// ArrayBuilder<Varchar> arrays(numArrays, estimatedNumElements, memoryPool);
// StringViewBufferHolder arena;
// selected.applyToSelected([&](vector_size_t row) {
//   auto array = arrays.startArray(row);
//   array.emplace_back(arena.getOwnedValue(std::to_string(row)));
// });
// arrays.setStringBuffers(arena.moveBuffers());
// std::shared_ptr<ArrayVector> result = std::move(arrays).finish();
//
// TODO:
//   - Expand coverage of std::vector API (e.g. operator[], resize()).
//   - Smooth buffer management.
//   - Adds checks to prevent misuse, i.e. one Ref alive at a time.
//   - Support null arrays and null elements.
template <typename Tag>
class ArrayBuilder {
 public:
  // All arrays are initialized to zero-length arrays.
  ArrayBuilder(
      vector_size_t numArrays,
      vector_size_t estimatedNumElements,
      memory::MemoryPool* pool)
      : numArrays_(numArrays),
        offsetsBuffer_(AlignedBuffer::allocate<vector_size_t>(numArrays, pool)),
        sizesBuffer_(AlignedBuffer::allocate<vector_size_t>(numArrays, pool)),
        elements_(std::max(estimatedNumElements, 16), pool) {
    std::fill(offsets_, offsets_ + numArrays_, 0);
    std::fill(sizes_, sizes_ + numArrays_, 0);
  }

  // Proxy object returned by startArray; only one should be live at a time.
  class Ref {
   public:
    Ref(const Ref&) = delete;
    Ref& operator=(const Ref&) = delete;

    void push_back(const typename CppToType<Tag>::NativeType& value) {
      emplace_back(value);
    }

    template <typename... T>
    void emplace_back(T&&... t) {
      b_->elements_.emplace_back(std::forward<T>(t)...);
    }

    ~Ref() {
      DCHECK_GE(b_->elements_.size(), b_->offsets_[index_]);
      b_->sizes_[index_] = b_->elements_.size() - b_->offsets_[index_];
    }

   private:
    friend ArrayBuilder<Tag>;
    Ref(ArrayBuilder<Tag>* builder, vector_size_t index)
        : b_(builder), index_(index) {}

    ArrayBuilder<Tag>* const b_;
    const vector_size_t index_;
  };

  // Start a new array at the given index. The returned Ref must be destroyed
  // before startArray is called again.
  Ref startArray(vector_size_t arrayIndex) {
    offsets_[arrayIndex] = elements_.size();
    return Ref(this, arrayIndex);
  }

  // Overwrite the string buffers associated with the elements array.
  void setStringBuffers(std::vector<BufferPtr> buffers) {
    elements_.setStringBuffers(std::move(buffers));
  }

  // Convert the collected data into an ArrayVector. It is not valid to call
  // any other methods on this object after this call.
  std::shared_ptr<ArrayVector> finish(memory::MemoryPool* pool) && {
    offsets_ = nullptr;
    sizes_ = nullptr;

    return std::make_shared<ArrayVector>(
        pool,
        ARRAY(CppToType<Tag>::create()),
        /*nulls=*/nullptr,
        /*length=*/numArrays_,
        std::move(offsetsBuffer_),
        std::move(sizesBuffer_),
        std::move(elements_).consumeFlatVector(),
        /*nullCount=*/0);
  }

 private:
  const vector_size_t numArrays_;
  BufferPtr offsetsBuffer_;
  vector_size_t* offsets_{offsetsBuffer_->asMutable<vector_size_t>()};
  BufferPtr sizesBuffer_;
  vector_size_t* sizes_{sizesBuffer_->asMutable<vector_size_t>()};
  DynamicFlatVector<Tag> elements_;
};

} // namespace facebook::velox::functions
