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

#include "velox/type/CppToType.h"
#include "velox/type/SimpleFunctionApi.h"

namespace facebook::velox {

template <TypeKind kind>
struct KindToSimpleType {
  using type = typename TypeTraits<kind>::NativeType;
};

template <>
struct KindToSimpleType<TypeKind::VARCHAR> {
  using type = Varchar;
};

template <>
struct KindToSimpleType<TypeKind::VARBINARY> {
  using type = Varbinary;
};

} // namespace facebook::velox
