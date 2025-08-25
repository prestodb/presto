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

#include "velox/common/fuzzer/Utils.h"
#include "velox/functions/lib/sfm/SfmSketch.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::velox::fuzzer {

using facebook::velox::UnknownValue;
using facebook::velox::functions::sfm::SfmSketch;

class SfmSketchInputGenerator : public AbstractInputGenerator {
 public:
  SfmSketchInputGenerator(
      const size_t seed,
      const double nullRatio,
      memory::MemoryPool* pool);

  variant generate() override;

 private:
  template <typename T>
  variant generateTyped() {
    HashStringAllocator allocator(pool_);

    // SFM sketch require indexBitLength and precision to be the same for
    // functions such as merge and mergeArray.
    if (!indexBitLength_.has_value() && !precision_.has_value()) {
      indexBitLength_ = rand<int32_t>(rng_, 1, 16);
      numberOfBuckets_ = SfmSketch::numBuckets(*indexBitLength_);
      precision_ = rand<int32_t>(rng_, 1, 64 - *indexBitLength_);
    }

    auto sketch = SfmSketch(&allocator, /* seed */ 1);
    sketch.initialize(*numberOfBuckets_, *precision_);

    // Add values to the sketch
    auto numValues = rand<int32_t>(rng_, 1, 10);
    for (auto i = 0; i < numValues; ++i) {
      if constexpr (
          std::is_same_v<T, std::string> || std::is_same_v<T, StringView>) {
        // Generate a random string directly without using randString since it
        // is deprecated.
        auto size = rand<int32_t>(rng_, 0, 100); // size of the string.
        std::string str;
        str.reserve(size);

        // Generate random ASCII characters.
        for (int j = 0; j < size; ++j) {
          char c = static_cast<char>(rand<int32_t>(rng_, 32, 126));
          str.push_back(c);
        }

        sketch.add(str);
      } else if (std::is_same_v<T, UnknownValue>) {
        // No-op since SfmSketch ignores input nulls.
      } else {
        sketch.add(rand<T>(rng_));
      }
    }

    // Serialize the sketch.
    auto size = sketch.serializedSize();
    std::string buff(size, '\0');
    sketch.serialize(buff.data());
    return variant::create<TypeKind::VARBINARY>(buff);
  }

  TypePtr baseType_;
  memory::MemoryPool* pool_;
  std::optional<int32_t> indexBitLength_;
  std::optional<int32_t> precision_;
  std::optional<int32_t> numberOfBuckets_;
};

} // namespace facebook::velox::fuzzer
