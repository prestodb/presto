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

#include "velox/type/Type.h"

namespace facebook::velox::wave {

struct FunctionDefinition {
  /// Text of the function, e.g. "inline __device__ int32_t onePlus(WaveShared*
  /// /*s*/, int32_t a) { return a + 1; }"
  std::string definition;

  // If non-empty, specifies an include to add to the translation unit. The
  // content is the full #include line.
  std::string includeLine;
};

/// Identifies a function by name and argument types.
struct FunctionKey {
  FunctionKey(const std::string& name, const std::vector<TypePtr>& types)
      : name(name), types(types) {}

  const std::string name;
  const std::vector<TypePtr> types;

  bool operator==(const FunctionKey& other) const {
    if (name != other.name || types.size() != other.types.size()) {
      return false;
    }
    for (auto i = 0; i < types.size(); ++i) {
      if (!types[i]->kindEquals(types[i])) {
        return false;
      }
    }
    return true;
  }
};
} // namespace facebook::velox::wave

namespace std {
template <>
struct hash<::facebook::velox::wave::FunctionKey> {
  size_t operator()(const ::facebook::velox::wave::FunctionKey key) const {
    size_t typeHash = 11;
    for (auto i = 0; i < key.types.size(); ++i) {
      typeHash *= key.types[i]->hashKind();
    }
    return std::hash<std::string>()(key.name) * typeHash;
  }
};
} // namespace std

namespace facebook::velox::wave {

/// Wave-specific function properties.
struct FunctionMetadata {
  FunctionMetadata() = default;

  FunctionMetadata(bool maySetStatus, bool maySetShared)
      : maySetStatus(maySetStatus), maySetShared(maySetShared) {}

  /// True if may turn off the lane, e.g. for error.
  bool maySetStatus{false};

  /// True if needs the WaveShared* for context.
  bool maySetShared{false};
};

struct FunctionEntry {
  FunctionMetadata metadata;
  std::string includeLine;
  // Text containing placeholders for argument/return types.
  std::string text;
};

/// Contains registration for Velox functions on GPU. The key is the name and
/// argument type list. The result of retrieval is an inline __device__ function
/// definition to add  to a generated kernel. Some functions like lambdas have a
/// special codegen pattern. These are known separately.
class WaveRegistry {
 public:
  FunctionMetadata metadata(const FunctionKey& key) const;

  // Produces the text to include to the kernel. The return type is resolved at
  // this time, so it does not have to be inferred again and is passed as
  // 'returnType'.
  FunctionDefinition makeDefinition(
      const FunctionKey& key,
      const TypePtr returnType) const;

  void registerFunction(
      const FunctionKey& key,
      FunctionMetadata& metadata,
      const std::string& includeLine,
      const std::string& text);

  bool registerMessage(int32_t key, std::string message);

  std::string message(int32_t key);

 private:
  folly::F14FastMap<FunctionKey, FunctionEntry> data_;
  folly::F14FastMap<int32_t, std::string> messages_;
};

} // namespace facebook::velox::wave

namespace folly {
template <>
struct hasher<::facebook::velox::wave::FunctionKey> {
  size_t operator()(const ::facebook::velox::wave::FunctionKey key) const {
    size_t typeHash = 11;
    for (auto i = 0; i < key.types.size(); ++i) {
      typeHash *= key.types[i]->hashKind();
    }
    return std::hash<std::string>()(key.name) * typeHash;
  }
};
} // namespace folly
