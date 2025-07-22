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
#include "velox/functions/prestosql/aggregates/sfm/SfmSketch.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::velox::fuzzer {

using facebook::velox::UnknownValue;
using facebook::velox::functions::aggregate::SfmSketch;

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

    // Create SfmSketch with random parameters.
    auto indexBitLength = rand<int32_t>(rng_, 1, 16);
    auto numberOfBuckets = SfmSketch::numBuckets(indexBitLength);
    auto precision = rand<int32_t>(rng_, 1, 64 - indexBitLength);

    auto sketch = SfmSketch(&allocator);
    sketch.initialize(numberOfBuckets, precision);

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

    // Randomly enable privacy after adding values.
    if (rand<bool>(rng_)) {
      auto epsilon =
          rand<double>(rng_, 0.1, std::numeric_limits<double>::max());
      sketch.enablePrivacy(epsilon);
    }

    // Serialize the sketch.
    auto size = sketch.serializedSize();
    std::string buff(size, '\0');
    sketch.serialize(buff.data());
    return variant::create<TypeKind::VARBINARY>(buff);
  }

  TypePtr baseType_;
  memory::MemoryPool* pool_;
};

} // namespace facebook::velox::fuzzer
