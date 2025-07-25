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

class QDigestType : public VarbinaryType {
 public:
  static const std::shared_ptr<const QDigestType>& get(
      const TypePtr& dataType) {
    static const auto bigintInstance =
        std::shared_ptr<const QDigestType>(new QDigestType(BIGINT()));
    static const auto realInstance =
        std::shared_ptr<const QDigestType>(new QDigestType(REAL()));
    static const auto doubleInstance =
        std::shared_ptr<const QDigestType>(new QDigestType(DOUBLE()));

    if (dataType->isBigint()) {
      return bigintInstance;
    } else if (dataType->isReal()) {
      return realInstance;
    } else if (dataType->isDouble()) {
      return doubleInstance;
    } else {
      VELOX_UNREACHABLE(
          "Only QDIGEST(BIGINT), QDIGEST(REAL), and QDIGEST(DOUBLE) are supported: QDIGEST({})",
          dataType->toString());
    }
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "QDIGEST";
  }

  const std::vector<TypeParameter>& parameters() const override {
    return parameters_;
  }

  std::string toString() const override {
    return fmt::format("QDIGEST({})", parameters_[0].type->toString());
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    folly::dynamic children = folly::dynamic::array;
    for (auto& param : parameters_) {
      children.push_back(param.type->serialize());
    }
    obj["cTypes"] = children;
    return obj;
  }

  bool isOrderable() const override {
    return false;
  }

 private:
  explicit QDigestType(const TypePtr& dataType)
      : parameters_({TypeParameter(dataType)}) {}

  const std::vector<TypeParameter> parameters_;
};

inline std::shared_ptr<const QDigestType> QDIGEST(const TypePtr& dataType) {
  return QDigestType::get(dataType);
}

inline bool isQDigestType(const TypePtr& type) {
  return QDigestType::get(DOUBLE()) == type ||
      QDigestType::get(REAL()) == type || QDigestType::get(BIGINT()) == type;
}

// Type to use for inputs and outputs of simple functions, e.g.
// arg_type<QDigest> and out_type<QDigest>.
template <typename T>
struct SimpleQDigestT;

template <>
struct SimpleQDigestT<int64_t> {
  using type = Varbinary;
  static constexpr const char* typeName = "qdigest(bigint)";
};

template <>
struct SimpleQDigestT<float> {
  using type = Varbinary;
  static constexpr const char* typeName = "qdigest(real)";
};

template <>
struct SimpleQDigestT<double> {
  using type = Varbinary;
  static constexpr const char* typeName = "qdigest(double)";
};

template <typename T>
using SimpleQDigest = CustomType<SimpleQDigestT<T>>;

} // namespace facebook::velox
