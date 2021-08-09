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

#include "folly/Optional.h"
#include "velox/type/Type.h"

#define VELOX_UDF_BEGIN(Name)                                                \
  struct udf_##Name {                                                        \
    template <typename __Koski_ExecParams>                                   \
    struct udf {                                                             \
      template <typename __Koski_TArg>                                       \
      using arg_type = typename __Koski_ExecParams::template resolver<       \
          __Koski_TArg>::in_type;                                            \
                                                                             \
      template <typename __Koski_TArg>                                       \
      using out_type = typename __Koski_ExecParams::template resolver<       \
          __Koski_TArg>::out_type;                                           \
                                                                             \
      template <typename __Koski_TArg>                                       \
      using opt_arg_type =                                                   \
          folly::Optional<typename __Koski_ExecParams::template resolver<    \
              __Koski_TArg>::in_type>;                                       \
                                                                             \
      template <typename __Koski_TArg>                                       \
      using opt_out_type =                                                   \
          folly::Optional<typename __Koski_ExecParams::template resolver<    \
              __Koski_TArg>::out_type>;                                      \
                                                                             \
      template <typename __Koski_TKey, typename __Koski_TVal>                \
      using MapVal =                                                         \
          arg_type<::facebook::velox::Map<__Koski_TKey, __Koski_TVal>>;      \
      template <typename __Koski_TElement>                                   \
      using ArrayVal = arg_type<::facebook::velox::Array<__Koski_TElement>>; \
      using VarcharVal = arg_type<::facebook::velox::Varchar>;               \
      using VarbinaryVal = arg_type<::facebook::velox::Varbinary>;           \
      template <typename... __Koski_TArgs>                                   \
      using RowVal = arg_type<::facebook::velox::Row<__Koski_TArgs...>>;     \
      template <typename __Koski_TKey, typename __Koski_TVal>                \
      using MapWriter =                                                      \
          out_type<::facebook::velox::Map<__Koski_TKey, __Koski_TVal>>;      \
      template <typename __Koski_TElement>                                   \
      using ArrayWriter =                                                    \
          out_type<::facebook::velox::Array<__Koski_TElement>>;              \
      using VarcharWriter = out_type<::facebook::velox::Varchar>;            \
      using VarbinaryWriter = out_type<::facebook::velox::Varbinary>;        \
      template <typename... __Koski_TArgs>                                   \
      using RowWriter = out_type<::facebook::velox::Row<__Koski_TArgs...>>;  \
      static constexpr auto name = #Name;

#define VELOX_UDF_END() \
  }                     \
  ;                     \
  }                     \
  ;

// todo(youknowjack): I like this syntax better, but it's hard to get formatting
// right, and decorating it with extra info is tricky
// #define VELOX_UDF_CLASS() \
// template <template<class, class> class map_type> \
// struct Name
