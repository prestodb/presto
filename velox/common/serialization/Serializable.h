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

#include <type_traits>
#include "folly/Optional.h"
#include "folly/json.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/serialization/DeserializationRegistry.h"

namespace facebook {
namespace velox {

namespace details {
template <typename To, typename From>
std::unique_ptr<To> checked_unique_ptr_cast(std::unique_ptr<From> input) {
  VELOX_USER_CHECK_NOT_NULL(input.get());
  auto* released = input.release();
  auto* casted = dynamic_cast<To*>(released);
  VELOX_USER_CHECK_NOT_NULL(casted);
  return std::unique_ptr<To>(casted);
}
} // namespace details

inline const folly::json::serialization_opts getSerializationOptions() {
  folly::json::serialization_opts opts;
  opts.allow_nan_inf = true;
  opts.allow_trailing_comma = true;
  opts.recursion_limit = UINT_MAX;
  opts.sort_keys = true;
  opts.double_fallback = true;
  return opts;
}

template <class T, class... args>
struct is_any_of : std::false_type {};

template <class T, class arg1, class... args>
struct is_any_of<T, arg1, args...> : is_any_of<T, args...> {};

template <class T, class... args>
struct is_any_of<T, T, args...> : std::true_type {};

template <class, class = void>
struct has_static_obj_create_type : std::false_type {};

template <typename T>
using objCreateType = T(const folly::dynamic&);

template <class T>
struct has_static_obj_create_type<
    T,
    std::enable_if_t<
        std::is_same_v<decltype(T::create(std::declval<folly::dynamic>())), T>>>
    : std::true_type {};

template <class>
struct vector_type {
  using type = void;
};

template <class T>
struct vector_type<std::vector<T>> {
  using type = T;
};

template <class T>
struct is_vector_type
    : std::is_same<T, std::vector<typename vector_type<T>::type>> {};

using serilizationformat = folly::dynamic;

template <class, class = void>
struct has_serialize_type : std::false_type {};

template <class T>
struct has_serialize_type<
    T,
    std::enable_if_t<std::is_same_v<
        decltype(std::declval<T>().serialize()),
        serilizationformat>>> : std::true_type {};

class ISerializable {
 public:
  virtual folly::dynamic serialize() const = 0;

  // Serialization for clases derived from ISerializable.
  template <
      typename T,
      typename = std::enable_if_t<std::is_base_of_v<ISerializable, T>>>
  static folly::dynamic serialize(std::shared_ptr<const T> serializable) {
    return serializable->serialize();
  }

  template <
      typename T,
      typename = std::enable_if_t<has_serialize_type<T>::value>>
  static folly::dynamic serialize(T& obj) {
    return obj.serialize();
  }

  template <
      typename T,
      typename = std::enable_if_t<
          is_any_of<T, int64_t, double, std::string, bool>::value>>
  static folly::dynamic serialize(T val) {
    return val;
  }

  static folly::dynamic serialize(int32_t val) {
    return folly::dynamic{(int64_t)val};
  }

  static folly::dynamic serialize(uint64_t val) {
    return folly::dynamic{(int64_t)val};
  }

  // Serialization for standard containers.
  // TODO separate defintions of composite types from declarations.
  template <typename T>
  static folly::dynamic serialize(const std::vector<T>& vec) {
    folly::dynamic arr = folly::dynamic::array;
    for (const auto& member : vec) {
      arr.push_back(ISerializable::serialize(member));
    }
    return arr;
  }

  template <
      class T,
      std::enable_if_t<
          std::is_same_v<T, folly::Optional<typename T::value_type>>>>
  static folly::dynamic serialize(const folly::Optional<T>& val) {
    if (!val.hasValue()) {
      return nullptr;
    }

    return serialize(val.value());
  }

  template <class K, class U>
  static folly::dynamic serialize(const std::map<K, U>& map) {
    folly::dynamic keys = folly::dynamic::array;
    folly::dynamic values = folly::dynamic::array;
    for (auto& pair : map) {
      keys.push_back(ISerializable::serialize(pair.first));
      values.push_back(ISerializable::serialize(pair.second));
    }

    folly::dynamic obj = folly::dynamic::object;
    obj["keys"] = std::move(keys);
    obj["values"] = std::move(values);

    return obj;
  }

  template <
      class T,
      typename = std::enable_if_t<std::is_base_of_v<ISerializable, T>>>
  static std::shared_ptr<const T> deserialize(const folly::dynamic& obj) {
    VELOX_USER_CHECK(obj.isObject());
    // use the key to lookup creator and call it.
    // creator generally be a static method in the class.
    auto name = obj["name"].asString();
    VELOX_USER_CHECK(
        velox::DeserializationRegistryForSharedPtr().Has(name),
        "Deserialization function for class: {} is not registered",
        name);

    return std::dynamic_pointer_cast<const T>(
        velox::DeserializationRegistryForSharedPtr().Create(name, obj));
  }

  template <
      typename T,
      typename = std::enable_if_t<has_static_obj_create_type<T>::value>>
  using createReturnType = decltype(T::create(std::declval<folly::dynamic>()));

  template <
      class T,
      typename = std::enable_if_t<has_static_obj_create_type<T>::value>>
  static createReturnType<T> deserialize(const folly::dynamic& obj) {
    return T::create(obj);
  }

  template <
      class T,
      typename =
          std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>>>
  static T deserialize(const folly::dynamic& obj) {
    auto raw = obj.asInt();
    VELOX_USER_CHECK_GE(raw, std::numeric_limits<T>::min());
    VELOX_USER_CHECK_LE(raw, std::numeric_limits<T>::max());
    return (T)raw;
  }

  template <class T, typename = std::enable_if_t<std::is_same_v<T, bool>>>
  static bool deserialize(const folly::dynamic& obj) {
    return obj.asBool();
  }

  template <class T, typename = std::enable_if_t<std::is_same_v<T, double>>>
  static double deserialize(const folly::dynamic& obj) {
    return obj.asDouble();
  }

  template <
      class T,
      typename = std::enable_if_t<std::is_same_v<T, std::string>>>
  static std::string deserialize(const folly::dynamic& obj) {
    return obj.asString();
  }

  template <
      class T,
      typename = std::enable_if_t<
          std::is_same_v<T, folly::Optional<typename T::value_type>>>>
  static folly::Optional<
      decltype(ISerializable::deserialize<typename T::value_type>(
          std::declval<folly::dynamic>()))>
  deserialize(const folly::dynamic& obj) {
    if (obj.isNull()) {
      return folly::none;
    }
    auto val = deserialize<typename T::value_type>(obj);
    return folly::Optional<
        decltype(ISerializable::deserialize<typename T::value_type>(
            std::declval<folly::dynamic>()))>(move(val));
  }

  // deserialization for standard containers.

  template <typename T>
  using deserializeType =
      decltype(ISerializable::deserialize<T>(std::declval<folly::dynamic>()));

  template <class T, typename = std::enable_if_t<is_vector_type<T>::value>>
  static auto deserialize(const folly::dynamic& array) {
    using deserializeValType =
        decltype(ISerializable::deserialize<typename T::value_type>(
            std::declval<folly::dynamic>()));

    VELOX_USER_CHECK(array.isArray());
    std::vector<deserializeValType> exprs;
    for (auto& obj : array) {
      exprs.push_back(ISerializable::deserialize<typename T::value_type>(obj));
    }
    return exprs;
  }

  template <
      class T,
      typename = std::enable_if_t<std::is_same_v<
          T,
          std::map<typename T::key_type, typename T::mapped_type>>>>
  static std::map<
      decltype(ISerializable::deserialize<typename T::key_type>(
          std::declval<folly::dynamic>())),
      decltype(ISerializable::deserialize<typename T::mapped_type>(
          std::declval<folly::dynamic>()))>
  deserialize(const folly::dynamic& obj) {
    using deserializeKeyType =
        decltype(ISerializable::deserialize<typename T::key_type>(
            std::declval<folly::dynamic>()));

    using deserializeMappedType =
        decltype(ISerializable::deserialize<typename T::mapped_type>(
            std::declval<folly::dynamic>()));

    std::map<deserializeKeyType, deserializeMappedType> map;
    const folly::dynamic& keys = obj["keys"];
    const folly::dynamic& values = obj["values"];
    VELOX_USER_CHECK(keys.isArray() && values.isArray());
    VELOX_USER_CHECK_EQ(keys.size(), values.size());
    for (size_t idx = 0; idx < keys.size(); ++idx) {
      auto first = ISerializable::deserialize<typename T::key_type>(keys[idx]);
      auto second =
          ISerializable::deserialize<typename T::mapped_type>(values[idx]);
      map.insert({first, second});
    }
    return map;
  }

  virtual ~ISerializable() = default;
};

} // namespace velox
} // namespace facebook
