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

namespace facebook::velox::test {
/// A custom type that provides custom comparison and hash functions.
class BigintTypeWithCustomComparison final : public BigintType {
  constexpr BigintTypeWithCustomComparison()
      : BigintType{ProvideCustomComparison{}} {}

 public:
  static std::shared_ptr<const BigintTypeWithCustomComparison> get() {
    VELOX_CONSTEXPR_SINGLETON BigintTypeWithCustomComparison kInstance;
    return {
        std::shared_ptr<const BigintTypeWithCustomComparison>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  /// For the purposes of testing, this type only compares the bottom 8 bits of
  /// values.
  int32_t compare(const int64_t& left, const int64_t& right) const override {
    int64_t leftTruncated = left & 0xff;
    int64_t rightTruncated = right & 0xff;

    return leftTruncated < rightTruncated ? -1
        : leftTruncated == rightTruncated ? 0
                                          : 1;
  }

  uint64_t hash(const int64_t& value) const override {
    return folly::hasher<int64_t>()(value & 0xff);
  }

  const char* name() const override {
    return "BIGINT TYPE WITH CUSTOM COMPARISON";
  }

  const std::vector<TypeParameter>& parameters() const override {
    static const std::vector<TypeParameter> kEmpty = {};
    return kEmpty;
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

inline std::shared_ptr<const BigintTypeWithCustomComparison>
BIGINT_TYPE_WITH_CUSTOM_COMPARISON() {
  return BigintTypeWithCustomComparison::get();
}

/// A custom type that declares it providesCustomComparison but does not
/// implement the compare or hash functions. This is not supported.
class BigintTypeWithInvalidCustomComparison final : public BigintType {
  constexpr BigintTypeWithInvalidCustomComparison()
      : BigintType{ProvideCustomComparison{}} {}

 public:
  static std::shared_ptr<const BigintTypeWithInvalidCustomComparison> get() {
    VELOX_CONSTEXPR_SINGLETON BigintTypeWithInvalidCustomComparison kInstance;
    return {
        std::shared_ptr<const BigintTypeWithInvalidCustomComparison>{},
        &kInstance};
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "BIGINT TYPE WITH INVALID CUSTOM COMPARISON";
  }

  const std::vector<TypeParameter>& parameters() const override {
    static const std::vector<TypeParameter> kEmpty = {};
    return kEmpty;
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

inline std::shared_ptr<const BigintTypeWithInvalidCustomComparison>
BIGINT_TYPE_WITH_INVALID_CUSTOM_COMPARISON() {
  return BigintTypeWithInvalidCustomComparison::get();
}

/// A custom type that is not fixed width that provides custom comparison and
/// hash functions. This is not currently supported and it is checked in compile
/// time (static_assert).
} // namespace facebook::velox::test
