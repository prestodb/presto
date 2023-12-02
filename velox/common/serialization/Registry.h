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
// fork from CAFFE2 registry

#pragma once

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <unordered_map>

#include <glog/logging.h>
#include "folly/Preprocessor.h"
#include "folly/container/F14Map.h"

#include "velox/common/base/Exceptions.h"
#include "velox/core/Metaprogramming.h"

namespace facebook {
namespace velox {

/**
 * @brief A template class that allows one to register function objects by keys.
 *
 * The keys are usually a string specifying the name, but can be anything that
 * can be used in a std::map.
 * It provides `Create` function to directly call with key and arguments.
 */

template <class KeyType, class FunctionSignature>
class Registry {
 public:
  using Creator = std::function<FunctionSignature>;
  using CreatorMap = folly::F14NodeMap<KeyType, Creator>;

  Registry() : Create(creatorMap_) {}

  void Register(
      const KeyType& key,
      Creator creator,
      std::optional<std::string_view> helpMsg = std::nullopt) {
    std::lock_guard<std::mutex> lock(registerutex_);
    creatorMap_[key] = std::move(creator);
    if (helpMsg) {
      helpMessage_[key] = *helpMsg;
    }
  }

  inline bool Has(const KeyType& key) const {
    return (creatorMap_.count(key) != 0);
  }

  /**
   * Returns the keys currently registered as a vector.
   */
  std::vector<KeyType> Keys() const {
    std::vector<KeyType> keys;
    for (const auto& it : creatorMap_) {
      keys.push_back(it.first);
    }
    return keys;
  }

  const std::unordered_map<KeyType, std::string>& HelpMessage() const {
    return helpMessage_;
  }

  const char* HelpMessage(const KeyType& key) const {
    auto it = helpMessage_.find(key);
    if (it == helpMessage_.end()) {
      return nullptr;
    }
    return it->second.c_str();
  }

 private:
  CreatorMap creatorMap_;
  std::unordered_map<KeyType, std::string> helpMessage_;
  std::mutex registerutex_;

  Registry(const Registry& other) = delete;

 public:
  template <typename T>
  struct CreateFunction;

  template <class ReturnType, class... ArgTypes>
  struct CreateFunction<ReturnType(ArgTypes...)> {
    CreateFunction(const CreatorMap& creatorMap) : creatorMap_(creatorMap) {}

    ReturnType operator()(const KeyType& key, ArgTypes... types) const {
      const auto it = creatorMap_.find(key);
      if (it == creatorMap_.end()) {
        if constexpr (facebook::velox::util::is_smart_pointer<
                          ReturnType>::value) {
          return nullptr;
        }

        VELOX_UNSUPPORTED(
            typeid(ReturnType).name(), " is not nullable return type");
      }
      return it->second(types...);
    }

   private:
    const CreatorMap& creatorMap_;
  };

  // Provide Create function as a function object
  // If there is no key found, it will:
  //   Smart pointer types: return nullptr
  //   Value types        : throw invalid_argument exception.
  //
  // Function signature:
  //   ReturnType Create(const KeyType& key, ArgTypes...);
  const CreateFunction<FunctionSignature> Create;
};

} // namespace velox
} // namespace facebook
