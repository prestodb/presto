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
#include <string>
#include <type_traits>
#include "Registry.h"
#include "folly/dynamic.h"
#include "folly/hash/Hash.h"

namespace facebook {
namespace velox {
class ISerializable;

using DeserializationRegistryType = Registry<
    std::string,
    std::shared_ptr<const ISerializable>(const folly::dynamic&)>;

DeserializationRegistryType& DeserializationRegistryForSharedPtr();

using DeserializationWithContextRegistryType = Registry<
    std::string,
    std::shared_ptr<const ISerializable>(const folly::dynamic&, void* context)>;

DeserializationWithContextRegistryType&
DeserializationWithContextRegistryForSharedPtr();

namespace detail {
template <class, class = void>
struct is_templated_create : std::false_type {};

template <class T>
struct is_templated_create<
    T,
    std::void_t<decltype(T::template create<T>(
        std::declval<folly::dynamic>()))>> : std::true_type {};
} // namespace detail

template <class T>
void registerDeserializer() {
  if constexpr (detail::is_templated_create<T>::value) {
    DeserializationRegistryForSharedPtr().Register(
        T::getClassName(), T::template create<T>);
  } else {
    DeserializationRegistryForSharedPtr().Register(
        T::getClassName(), T::create);
  }
}

} // namespace velox
} // namespace facebook
