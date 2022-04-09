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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/sparksql/Arena.h"
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

template <typename TInput>
struct InFunctionOuter {
  template <typename TExecCtx>
  struct InFunctionInner {
    VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

    FOLLY_ALWAYS_INLINE void initialize(
        const core::QueryConfig& /*config*/,
        const arg_type<TInput>* /*searchTerm*/,
        const arg_type<velox::Array<TInput>>* searchElements) {
      if (searchElements == nullptr) {
        return;
      }

      elements_.reserve(searchElements->size());

      for (const auto& entry : *searchElements) {
        if (!entry.has_value()) {
          hasNull_ = true;
          continue;
        }
        elements_.emplace(entry.value());
      }
    }

    FOLLY_ALWAYS_INLINE bool callNullable(
        bool& result,
        const arg_type<TInput>* searchTerm,
        const arg_type<velox::Array<TInput>>* /*array*/) {
      if (searchTerm == nullptr) {
        return false;
      }

      result = elements_.contains(*searchTerm);
      if (hasNull_ && !result) {
        return false;
      }
      return true;
    }

   private:
    Set<TInput> elements_;
    bool hasNull_{false};
  };

  template <typename T>
  using Inner = typename InFunctionOuter<TInput>::template InFunctionInner<T>;
};

template <typename T>
void registerInFn(const std::string& prefix) {
  registerFunction<InFunctionOuter<T>::template Inner, bool, T, Array<T>>(
      {prefix + "in"});
}

} // namespace

void registerIn(const std::string& prefix) {
  registerInFn<int8_t>(prefix);
  registerInFn<int16_t>(prefix);
  registerInFn<int32_t>(prefix);
  registerInFn<int64_t>(prefix);
  registerInFn<float>(prefix);
  registerInFn<double>(prefix);
  registerInFn<bool>(prefix);
  registerInFn<StringView>(prefix);
  registerInFn<Timestamp>(prefix);
  registerInFn<Date>(prefix);
}

} // namespace facebook::velox::functions::sparksql
