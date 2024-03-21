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

#include "velox/core/Metaprogramming.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

#define VELOX_DEFINE_FUNCTION_TYPES(__Velox_ExecParams)                 \
  template <typename TArgs>                                             \
  using arg_type =                                                      \
      typename __Velox_ExecParams::template resolver<TArgs>::in_type;   \
                                                                        \
  template <typename TArgs>                                             \
  using out_type =                                                      \
      typename __Velox_ExecParams::template resolver<TArgs>::out_type;  \
                                                                        \
  template <typename TArgs>                                             \
  using opt_arg_type = std::optional<                                   \
      typename __Velox_ExecParams::template resolver<TArgs>::in_type>;  \
                                                                        \
  template <typename TArgs>                                             \
  using opt_out_type = std::optional<                                   \
      typename __Velox_ExecParams::template resolver<TArgs>::out_type>; \
                                                                        \
  DECLARE_CONDITIONAL_TYPE_NAME(                                        \
      null_free_in_type_resolver, null_free_in_type, in_type);          \
  template <typename TArgs>                                             \
  using null_free_arg_type =                                            \
      typename null_free_in_type_resolver::template resolve<            \
          typename __Velox_ExecParams::template resolver<TArgs>>::type; \
                                                                        \
  template <typename TKey, typename TVal>                               \
  using MapVal = arg_type<::facebook::velox::Map<TKey, TVal>>;          \
  template <typename TElement>                                          \
  using ArrayVal = arg_type<::facebook::velox::Array<TElement>>;        \
  using VarcharVal = arg_type<::facebook::velox::Varchar>;              \
  using VarbinaryVal = arg_type<::facebook::velox::Varbinary>;          \
  template <typename... TArgss>                                         \
  using RowVal = arg_type<::facebook::velox::Row<TArgss...>>;           \
  template <typename TKey, typename TVal>                               \
  using MapWriter = out_type<::facebook::velox::Map<TKey, TVal>>;       \
  template <typename TElement>                                          \
  using ArrayWriter = out_type<::facebook::velox::Array<TElement>>;     \
  using VarcharWriter = out_type<::facebook::velox::Varchar>;           \
  using VarbinaryWriter = out_type<::facebook::velox::Varbinary>;       \
  template <typename... TArgss>                                         \
  using RowWriter = out_type<::facebook::velox::Row<TArgss...>>;

#define VELOX_UDF_BEGIN(Name)                         \
  struct udf_##Name {                                 \
    static constexpr auto name = #Name;               \
    template <typename __Velox_ExecParams>            \
    struct udf {                                      \
      VELOX_DEFINE_FUNCTION_TYPES(__Velox_ExecParams) \
      static constexpr auto name = #Name;

#define VELOX_UDF_END() \
  }                     \
  ;                     \
  }                     \
  ;
