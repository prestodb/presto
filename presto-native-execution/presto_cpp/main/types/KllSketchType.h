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
#pragma once

#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::presto {

class KllSketchType final : public velox::VarbinaryType {
 public:
  static std::shared_ptr<const KllSketchType> get(
      const velox::TypePtr& dataType);

  bool equivalent(const velox::Type& other) const override {
    if (auto* otherKll = dynamic_cast<const KllSketchType*>(&other)) {
      return parameter_.type->equivalent(*otherKll->parameter_.type);
    }
    return false;
  }

  const char* name() const override {
    return "KLLSKETCH";
  }

  std::span<const velox::TypeParameter> parameters() const override {
    return {&parameter_, 1};
  }

  std::string toString() const override {
    return fmt::format("KLLSKETCH({})", parameter_.type->toString());
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    folly::dynamic children = folly::dynamic::array;
    children.push_back(parameter_.type->serialize());
    obj["cTypes"] = children;
    return obj;
  }

  bool isOrderable() const override {
    return false;
  }

 private:
  explicit KllSketchType(const velox::TypePtr& dataType)
      : parameter_(dataType) {}

  const velox::TypeParameter parameter_;
};

inline bool isKllSketchType(const velox::TypePtr& type) {
  return dynamic_cast<const KllSketchType*>(type.get()) != nullptr;
}

inline std::shared_ptr<const KllSketchType> KLLSKETCH(
    const velox::TypePtr& dataType) {
  return KllSketchType::get(dataType);
}

// Type to use for inputs and outputs of simple functions
template <typename T>
struct SimpleKllSketchT;

template <>
struct SimpleKllSketchT<double> {
  using type = velox::Varbinary;
  static constexpr const char* typeName = "kllsketch(double)";
};

template <>
struct SimpleKllSketchT<int64_t> {
  using type = velox::Varbinary;
  static constexpr const char* typeName = "kllsketch(bigint)";
};

template <>
struct SimpleKllSketchT<velox::StringView> {
  using type = velox::Varbinary;
  static constexpr const char* typeName = "kllsketch(varchar)";
};

template <>
struct SimpleKllSketchT<bool> {
  using type = velox::Varbinary;
  static constexpr const char* typeName = "kllsketch(boolean)";
};

template <typename T>
using SimpleKllSketch = velox::CustomType<SimpleKllSketchT<T>>;

} // namespace facebook::presto
