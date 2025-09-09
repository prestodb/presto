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
#include "velox/functions/prestosql/aggregates/HyperLogLogAggregate.h"
#include "velox/type/Type.h"

namespace facebook::velox::fuzzer {

using facebook::velox::aggregate::prestosql::HllAccumulator;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
class HyperLogLogInputGenerator : public AbstractInputGenerator {
 public:
  HyperLogLogInputGenerator(
      const size_t seed,
      const double nullRatio,
      memory::MemoryPool* pool,
      int32_t minNumValues = 1);

  variant generate() override;

 private:
  template <typename T>
  variant generateTyped() {
    HashStringAllocator allocator{pool_};
    HllAccumulator<T, true> accumulator(&allocator);
    accumulator.setIndexBitLength(common::hll::toIndexBitLength(error_));
    static const std::vector<UTF8CharList> encodings{
        UTF8CharList::ASCII,
        UTF8CharList::UNICODE_CASE_SENSITIVE,
        UTF8CharList::EXTENDED_UNICODE,
        UTF8CharList::MATHEMATICAL_SYMBOLS};

    auto numValues = rand<int32_t>(rng_, minNumValues_, 1000);
    for (auto i = 0; i < numValues; ++i) {
      if constexpr (
          std::is_same_v<T, std::string> || std::is_same_v<T, StringView>) {
        std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t> converter;
        auto size = rand<int32_t>(rng_, 0, 100);
        std::string result;
        accumulator.append(
            randString(rng_, size, encodings, result, converter));
      } else if (std::is_same_v<T, UnknownValue>) {
        // No-op since approx_set ignores input nulls.
      } else {
        accumulator.append(rand<T>(rng_));
      }
    }
    auto size = accumulator.serializedSize();
    std::string buff(size, '\0');
    accumulator.serialize(buff.data());
    return variant::create<TypeKind::VARBINARY>(buff);
  }

  TypePtr baseType_;
  double error_;
  int32_t minNumValues_;

  memory::MemoryPool* pool_;
};
#pragma GCC diagnostic pop

} // namespace facebook::velox::fuzzer
