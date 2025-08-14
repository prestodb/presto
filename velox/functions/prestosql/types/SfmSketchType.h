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

#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::velox {
class SfmSketchType final : public VarbinaryType {
  SfmSketchType() = default;

 public:
  static std::shared_ptr<const SfmSketchType> get() {
    VELOX_CONSTEXPR_SINGLETON SfmSketchType kInstance;
    return {std::shared_ptr<const SfmSketchType>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    return this == &other;
  }

  const char* name() const override {
    return "SFMSKETCH";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }
};

inline bool isSfmSketchType(const TypePtr& type) {
  return SfmSketchType::get() == type;
}

inline std::shared_ptr<const SfmSketchType> SFMSKETCH() {
  return SfmSketchType::get();
}

// Type used for function registration. e.g. noisy_approx_set_sfm()
struct SfmSketchTypeT {
  using type = Varbinary;
  static constexpr const char* typeName = "sfmsketch";
};

using SfmSketch = CustomType<SfmSketchTypeT>;
} // namespace facebook::velox
