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
#include "velox/functions/Udf.h"
#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox::functions {

/// Fast path for constant primitive type keys: map_subset(m, array[1, 2, 3]).
template <typename TExec, typename Key>
struct MapSubsetPrimitiveFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Map<Key, Generic<T1>>>* /*inputMap*/,
      const arg_type<Array<Key>>* keys) {
    if (keys != nullptr) {
      constantSearchKeys_ = true;
      initializeSearchKeys(*keys);
    }
  }

  void call(
      out_type<Map<Key, Generic<T1>>>& out,
      const arg_type<Map<Key, Generic<T1>>>& inputMap,
      const arg_type<Array<Key>>& keys) {
    if (keys.empty()) {
      return;
    }

    if (!constantSearchKeys_) {
      searchKeys_.clear();
      initializeSearchKeys(keys);
    }

    if (searchKeys_.empty()) {
      return;
    }

    auto toFind = searchKeys_.size();

    for (const auto& entry : inputMap) {
      if (!searchKeys_.contains(entry.first)) {
        continue;
      }

      if (!entry.second.has_value()) {
        auto& keyWriter = out.add_null();
        keyWriter = entry.first;
      } else {
        auto [keyWriter, valueWriter] = out.add_item();
        keyWriter = entry.first;
        valueWriter.copy_from(entry.second.value());
      }

      --toFind;
      if (toFind == 0) {
        break;
      }
    }
  }

 private:
  void initializeSearchKeys(const arg_type<Array<Key>>& keys) {
    for (const auto& key : keys.skipNulls()) {
      searchKeys_.emplace(key);
    }
  }

  bool constantSearchKeys_{false};
  util::floating_point::HashSetNaNAware<arg_type<Key>> searchKeys_;
};

/// Fast path for constant string keys: map_subset(m, array['a', 'b', 'c']).
template <typename TExec>
struct MapSubsetVarcharFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Map<Varchar, Generic<T1>>>* /*inputMap*/,
      const arg_type<Array<Varchar>>* keys) {
    if (keys != nullptr) {
      constantSearchKeys_ = true;

      searchKeyStrings_.reserve(keys->size());
      for (const auto& key : keys->skipNulls()) {
        if (key.isInline()) {
          searchKeys_.emplace(key);
        } else if (!searchKeys_.contains(key)) {
          searchKeyStrings_.push_back(key.str());
          searchKeys_.emplace(StringView(searchKeyStrings_.back()));
        }
      }
    }
  }

  void call(
      out_type<Map<Varchar, Generic<T1>>>& out,
      const arg_type<Map<Varchar, Generic<T1>>>& inputMap,
      const arg_type<Array<Varchar>>& keys) {
    if (keys.empty()) {
      return;
    }

    if (!constantSearchKeys_) {
      searchKeys_.clear();
      for (const auto& key : keys.skipNulls()) {
        searchKeys_.emplace(key);
      }
    }

    if (searchKeys_.empty()) {
      return;
    }

    auto toFind = searchKeys_.size();

    for (const auto& entry : inputMap) {
      if (!searchKeys_.contains(entry.first)) {
        continue;
      }

      if (!entry.second.has_value()) {
        auto& keyWriter = out.add_null();
        keyWriter.copy_from(entry.first);
      } else {
        auto [keyWriter, valueWriter] = out.add_item();
        keyWriter.copy_from(entry.first);
        valueWriter.copy_from(entry.second.value());
      }

      --toFind;
      if (toFind == 0) {
        break;
      }
    }
  }

 private:
  bool constantSearchKeys_{false};
  folly::F14FastSet<StringView> searchKeys_;
  std::vector<std::string> searchKeyStrings_;
};

/// Generic implementation. Doesn't provide an optimization for constant search
/// keys.
template <typename TExec>
struct MapSubsetFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  void call(
      out_type<Map<Generic<T1>, Generic<T2>>>& out,
      const arg_type<Map<Generic<T1>, Generic<T2>>>& inputMap,
      const arg_type<Array<Generic<T1>>>& keys) {
    if (keys.empty()) {
      return;
    }

    // TODO Figure out how to implement fast path for constant search keys.
    // Just implementing 'initialize' and populating 'searchKeys_' there doesn't
    // work because GenericView's go out of scope at the end of the method.

    searchKeys_.clear();
    for (const auto& key : keys.skipNulls()) {
      searchKeys_.emplace(key);
    }

    if (searchKeys_.empty()) {
      return;
    }

    auto toFind = searchKeys_.size();

    for (const auto& entry : inputMap) {
      if (!searchKeys_.contains(entry.first)) {
        continue;
      }

      if (!entry.second.has_value()) {
        auto& keyWriter = out.add_null();
        keyWriter.copy_from(entry.first);
      } else {
        auto [keyWriter, valueWriter] = out.add_item();
        keyWriter.copy_from(entry.first);
        valueWriter.copy_from(entry.second.value());
      }

      --toFind;
      if (toFind == 0) {
        break;
      }
    }
  }

 private:
  folly::F14FastSet<exec::GenericView> searchKeys_;
};

} // namespace facebook::velox::functions
