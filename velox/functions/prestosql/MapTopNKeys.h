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
#include "velox/functions/prestosql/MapTopNImpl.h"

namespace facebook::velox::functions {

template <typename TExec>
struct CompareKeys {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  using It = typename arg_type<Map<Orderable<T1>, Orderable<T2>>>::Iterator;

  bool operator()(const It& l, const It& r) const {
    static const CompareFlags flags{
        false /*nullsFirst*/,
        true /*ascending*/,
        false /*equalsOnly*/,
        CompareFlags::NullHandlingMode::kNullAsIndeterminate};
    return l->first.compare(r->first, flags) > 0;
  }
};

// Returns an array with the top N keys in descending order.
template <typename TExec>
struct MapTopNKeysFunction : MapTopNImpl<TExec, CompareKeys<TExec>> {};

} // namespace facebook::velox::functions
