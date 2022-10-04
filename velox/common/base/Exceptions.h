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

#include <memory>
#include <sstream>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <glog/logging.h>

#include <folly/Conv.h>
#include <folly/Exception.h>
#include <folly/Preprocessor.h>
#include "velox/common/base/VeloxException.h"

namespace facebook {
namespace velox {
namespace detail {

struct VeloxCheckFailArgs {
  const char* file;
  size_t line;
  const char* function;
  const char* expression;
  const char* errorSource;
  const char* errorCode;
  bool isRetriable;
};

struct CompileTimeEmptyString {
  CompileTimeEmptyString() = default;
  constexpr operator const char*() const {
    return "";
  }
  constexpr operator std::string_view() const {
    return {};
  }
  operator std::string() const {
    return {};
  }
};

// veloxCheckFail is defined as a separate helper function rather than
// a macro or inline `throw` expression to allow the compiler *not* to
// inline it when it is large. Having an out-of-line error path helps
// otherwise-small functions that call error-checking macros stay
// small and thus stay eligible for inlining.
template <typename Exception, typename StringType>
[[noreturn]] void veloxCheckFail(const VeloxCheckFailArgs& args, StringType s) {
  static_assert(
      !std::is_same_v<StringType, std::string>,
      "BUG: we should not pass std::string by value to veloxCheckFail");
  LOG(ERROR) << "Line: " << args.file << ":" << args.line
             << ", Function:" << args.function
             << ", Expression: " << args.expression << " " << s
             << ", Source: " << args.errorSource
             << ", ErrorCode: " << args.errorCode;

  throw Exception(
      args.file,
      args.line,
      args.function,
      args.expression,
      s,
      args.errorSource,
      args.errorCode,
      args.isRetriable);
}

// VeloxCheckFailStringType helps us pass by reference to
// veloxCheckFail exactly when the string type is std::string.
template <typename T>
struct VeloxCheckFailStringType;

template <>
struct VeloxCheckFailStringType<CompileTimeEmptyString> {
  using type = CompileTimeEmptyString;
};

template <>
struct VeloxCheckFailStringType<const char*> {
  using type = const char*;
};

template <>
struct VeloxCheckFailStringType<std::string> {
  using type = const std::string&;
};

// Declare explicit instantiations of veloxCheckFail for the given
// exceptionType. Just like normal function declarations (prototypes),
// this allows the compiler to assume that they are defined elsewhere
// and simply insert a function call for the linker to fix up, rather
// than emitting a definition of these templates into every
// translation unit they are used in.
#define DECLARE_CHECK_FAIL_TEMPLATES(exception_type)                           \
  namespace detail {                                                           \
  extern template void veloxCheckFail<exception_type, CompileTimeEmptyString>( \
      const VeloxCheckFailArgs& args,                                          \
      CompileTimeEmptyString);                                                 \
  extern template void veloxCheckFail<exception_type, const char*>(            \
      const VeloxCheckFailArgs& args,                                          \
      const char*);                                                            \
  extern template void veloxCheckFail<exception_type, const std::string&>(     \
      const VeloxCheckFailArgs& args,                                          \
      const std::string&);                                                     \
  } // namespace detail

// Definitions corresponding to DECLARE_CHECK_FAIL_TEMPLATES. Should
// only be used in Exceptions.cpp.
#define DEFINE_CHECK_FAIL_TEMPLATES(exception_type)                     \
  template void veloxCheckFail<exception_type, CompileTimeEmptyString>( \
      const VeloxCheckFailArgs& args, CompileTimeEmptyString);          \
  template void veloxCheckFail<exception_type, const char*>(            \
      const VeloxCheckFailArgs& args, const char*);                     \
  template void veloxCheckFail<exception_type, const std::string&>(     \
      const VeloxCheckFailArgs& args, const std::string&);

// When there is no message passed, we can statically detect this case
// and avoid passing even a single unnecessary argument pointer,
// minimizing size and thus maximizing eligibility for inlining.
inline CompileTimeEmptyString errorMessage() {
  return {};
}

inline const char* errorMessage(const char* s) {
  return s;
}

template <typename... Args>
std::string errorMessage(fmt::string_view fmt, const Args&... args) {
  return fmt::vformat(fmt, fmt::make_format_args(args...));
}

} // namespace detail

#define _VELOX_THROW_IMPL(                                               \
    exception, expr_str, errorSource, errorCode, isRetriable, ...)       \
  {                                                                      \
    /* GCC 9.2.1 doesn't accept this code with constexpr. */             \
    static const ::facebook::velox::detail::VeloxCheckFailArgs           \
        veloxCheckFailArgs = {                                           \
            __FILE__,                                                    \
            __LINE__,                                                    \
            __FUNCTION__,                                                \
            expr_str,                                                    \
            errorSource,                                                 \
            errorCode,                                                   \
            isRetriable};                                                \
    auto message = ::facebook::velox::detail::errorMessage(__VA_ARGS__); \
    ::facebook::velox::detail::veloxCheckFail<                           \
        exception,                                                       \
        typename ::facebook::velox::detail::VeloxCheckFailStringType<    \
            decltype(message)>::type>(veloxCheckFailArgs, message);      \
  }

#define _VELOX_CHECK_AND_THROW_IMPL(                                     \
    expr, expr_str, exception, errorSource, errorCode, isRetriable, ...) \
  if (UNLIKELY(!(expr))) {                                               \
    _VELOX_THROW_IMPL(                                                   \
        exception,                                                       \
        expr_str,                                                        \
        errorSource,                                                     \
        errorCode,                                                       \
        isRetriable,                                                     \
        __VA_ARGS__);                                                    \
  }

#define _VELOX_THROW(exception, ...) \
  _VELOX_THROW_IMPL(exception, "", ##__VA_ARGS__)

DECLARE_CHECK_FAIL_TEMPLATES(::facebook::velox::VeloxRuntimeError);

#define _VELOX_CHECK_IMPL(expr, expr_str, ...)                      \
  _VELOX_CHECK_AND_THROW_IMPL(                                      \
      expr,                                                         \
      expr_str,                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kInvalidState.c_str(),         \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

// If the caller passes a custom message (4 *or more* arguments), we
// have to construct a format string from ours ("({} vs. {})") plus
// theirs by adding a space and shuffling arguments. If they don't (exactly 3
// arguments), we can just pass our own format string and arguments straight
// through.

#define _VELOX_CHECK_OP_WITH_USER_FMT_HELPER(   \
    implmacro, expr1, expr2, op, user_fmt, ...) \
  implmacro(                                    \
      (expr1)op(expr2),                         \
      #expr1 " " #op " " #expr2,                \
      "({} vs. {}) " user_fmt,                  \
      expr1,                                    \
      expr2,                                    \
      ##__VA_ARGS__)

#define _VELOX_CHECK_OP_HELPER(implmacro, expr1, expr2, op, ...) \
  if constexpr (FOLLY_PP_DETAIL_NARGS(__VA_ARGS__) > 0) {        \
    _VELOX_CHECK_OP_WITH_USER_FMT_HELPER(                        \
        implmacro, expr1, expr2, op, __VA_ARGS__);               \
  } else {                                                       \
    implmacro(                                                   \
        (expr1)op(expr2),                                        \
        #expr1 " " #op " " #expr2,                               \
        "({} vs. {})",                                           \
        expr1,                                                   \
        expr2);                                                  \
  }

#define _VELOX_CHECK_OP(expr1, expr2, op, ...) \
  _VELOX_CHECK_OP_HELPER(_VELOX_CHECK_IMPL, expr1, expr2, op, ##__VA_ARGS__)

#define _VELOX_USER_CHECK_IMPL(expr, expr_str, ...)              \
  _VELOX_CHECK_AND_THROW_IMPL(                                   \
      expr,                                                      \
      expr_str,                                                  \
      ::facebook::velox::VeloxUserError,                         \
      ::facebook::velox::error_source::kErrorSourceUser.c_str(), \
      ::facebook::velox::error_code::kInvalidArgument.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define _VELOX_USER_CHECK_OP(expr1, expr2, op, ...) \
  _VELOX_CHECK_OP_HELPER(                           \
      _VELOX_USER_CHECK_IMPL, expr1, expr2, op, ##__VA_ARGS__)

// For all below macros, an additional message can be passed using a
// format string and arguments, as with `fmt::format`.
#define VELOX_CHECK(expr, ...) _VELOX_CHECK_IMPL(expr, #expr, ##__VA_ARGS__)
#define VELOX_CHECK_GT(e1, e2, ...) _VELOX_CHECK_OP(e1, e2, >, ##__VA_ARGS__)
#define VELOX_CHECK_GE(e1, e2, ...) _VELOX_CHECK_OP(e1, e2, >=, ##__VA_ARGS__)
#define VELOX_CHECK_LT(e1, e2, ...) _VELOX_CHECK_OP(e1, e2, <, ##__VA_ARGS__)
#define VELOX_CHECK_LE(e1, e2, ...) _VELOX_CHECK_OP(e1, e2, <=, ##__VA_ARGS__)
#define VELOX_CHECK_EQ(e1, e2, ...) _VELOX_CHECK_OP(e1, e2, ==, ##__VA_ARGS__)
#define VELOX_CHECK_NE(e1, e2, ...) _VELOX_CHECK_OP(e1, e2, !=, ##__VA_ARGS__)
#define VELOX_CHECK_NULL(e, ...) VELOX_CHECK(e == nullptr, ##__VA_ARGS__)
#define VELOX_CHECK_NOT_NULL(e, ...) VELOX_CHECK(e != nullptr, ##__VA_ARGS__)

#define VELOX_UNSUPPORTED(...)                                   \
  _VELOX_THROW(                                                  \
      ::facebook::velox::VeloxUserError,                         \
      ::facebook::velox::error_source::kErrorSourceUser.c_str(), \
      ::facebook::velox::error_code::kUnsupported.c_str(),       \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define VELOX_ARITHMETIC_ERROR(...)                              \
  _VELOX_THROW(                                                  \
      ::facebook::velox::VeloxUserError,                         \
      ::facebook::velox::error_source::kErrorSourceUser.c_str(), \
      ::facebook::velox::error_code::kArithmeticError.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define VELOX_SCHMEA_MISMATCH_ERROR(...)                         \
  _VELOX_THROW(                                                  \
      ::facebook::velox::VeloxUserError,                         \
      ::facebook::velox::error_source::kErrorSourceUser.c_str(), \
      ::facebook::velox::error_code::kSchemaMismatch.c_str(),    \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define VELOX_UNREACHABLE(...)                                      \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kUnreachableCode.c_str(),      \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

#ifndef NDEBUG
#define VELOX_DCHECK(expr, ...) VELOX_CHECK(expr, ##__VA_ARGS__)
#define VELOX_DCHECK_GT(e1, e2, ...) VELOX_CHECK_GT(e1, e2, ##__VA_ARGS__)
#define VELOX_DCHECK_GE(e1, e2, ...) VELOX_CHECK_GE(e1, e2, ##__VA_ARGS__)
#define VELOX_DCHECK_LT(e1, e2, ...) VELOX_CHECK_LT(e1, e2, ##__VA_ARGS__)
#define VELOX_DCHECK_LE(e1, e2, ...) VELOX_CHECK_LE(e1, e2, ##__VA_ARGS__)
#define VELOX_DCHECK_EQ(e1, e2, ...) VELOX_CHECK_EQ(e1, e2, ##__VA_ARGS__)
#define VELOX_DCHECK_NE(e1, e2, ...) VELOX_CHECK_NE(e1, e2, ##__VA_ARGS__)
#define VELOX_DCHECK_NULL(e, ...) VELOX_CHECK_NULL(e, ##__VA_ARGS__)
#define VELOX_DCHECK_NOT_NULL(e, ...) VELOX_CHECK_NOT_NULL(e, ##__VA_ARGS__)
#else
#define VELOX_DCHECK(expr, ...) VELOX_CHECK(true)
#define VELOX_DCHECK_GT(e1, e2, ...) VELOX_CHECK(true)
#define VELOX_DCHECK_GE(e1, e2, ...) VELOX_CHECK(true)
#define VELOX_DCHECK_LT(e1, e2, ...) VELOX_CHECK(true)
#define VELOX_DCHECK_LE(e1, e2, ...) VELOX_CHECK(true)
#define VELOX_DCHECK_EQ(e1, e2, ...) VELOX_CHECK(true)
#define VELOX_DCHECK_NE(e1, e2, ...) VELOX_CHECK(true)
#define VELOX_DCHECK_NOT_NULL(e, ...) VELOX_CHECK(true)
#endif

#define VELOX_FAIL(...)                                             \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kInvalidState.c_str(),         \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

DECLARE_CHECK_FAIL_TEMPLATES(::facebook::velox::VeloxUserError);

// For all below macros, an additional message can be passed using a
// format string and arguments, as with `fmt::format`.
#define VELOX_USER_CHECK(expr, ...) \
  _VELOX_USER_CHECK_IMPL(expr, #expr, ##__VA_ARGS__)
#define VELOX_USER_CHECK_GT(e1, e2, ...) \
  _VELOX_USER_CHECK_OP(e1, e2, >, ##__VA_ARGS__)
#define VELOX_USER_CHECK_GE(e1, e2, ...) \
  _VELOX_USER_CHECK_OP(e1, e2, >=, ##__VA_ARGS__)
#define VELOX_USER_CHECK_LT(e1, e2, ...) \
  _VELOX_USER_CHECK_OP(e1, e2, <, ##__VA_ARGS__)
#define VELOX_USER_CHECK_LE(e1, e2, ...) \
  _VELOX_USER_CHECK_OP(e1, e2, <=, ##__VA_ARGS__)
#define VELOX_USER_CHECK_EQ(e1, e2, ...) \
  _VELOX_USER_CHECK_OP(e1, e2, ==, ##__VA_ARGS__)
#define VELOX_USER_CHECK_NE(e1, e2, ...) \
  _VELOX_USER_CHECK_OP(e1, e2, !=, ##__VA_ARGS__)
#define VELOX_USER_CHECK_NULL(e, ...) \
  VELOX_USER_CHECK(e == nullptr, ##__VA_ARGS__)
#define VELOX_USER_CHECK_NOT_NULL(e, ...) \
  VELOX_USER_CHECK(e != nullptr, ##__VA_ARGS__)

#ifndef NDEBUG
#define VELOX_USER_DCHECK(expr, ...) VELOX_USER_CHECK(expr, ##__VA_ARGS__)
#define VELOX_USER_DCHECK_GT(e1, e2, ...) \
  VELOX_USER_CHECK_GT(e1, e2, ##__VA_ARGS__)
#define VELOX_USER_DCHECK_GE(e1, e2, ...) \
  VELOX_USER_CHECK_GE(e1, e2, ##__VA_ARGS__)
#define VELOX_USER_DCHECK_LT(e1, e2, ...) \
  VELOX_USER_CHECK_LT(e1, e2, ##__VA_ARGS__)
#define VELOX_USER_DCHECK_LE(e1, e2, ...) \
  VELOX_USER_CHECK_LE(e1, e2, ##__VA_ARGS__)
#define VELOX_USER_DCHECK_EQ(e1, e2, ...) \
  VELOX_USER_CHECK_EQ(e1, e2, ##__VA_ARGS__)
#define VELOX_USER_DCHECK_NE(e1, e2, ...) \
  VELOX_USER_CHECK_NE(e1, e2, ##__VA_ARGS__)
#define VELOX_USER_DCHECK_NOT_NULL(e, ...) \
  VELOX_USER_CHECK_NOT_NULL(e, ##__VA_ARGS__)
#define VELOX_USER_DCHECK_NULL(e, ...) VELOX_USER_CHECK_NULL(e, ##__VA_ARGS__)
#else
#define VELOX_USER_DCHECK(expr, ...) VELOX_USER_CHECK(true)
#define VELOX_USER_DCHECK_GT(e1, e2, ...) VELOX_USER_CHECK(true)
#define VELOX_USER_DCHECK_GE(e1, e2, ...) VELOX_USER_CHECK(true)
#define VELOX_USER_DCHECK_LT(e1, e2, ...) VELOX_USER_CHECK(true)
#define VELOX_USER_DCHECK_LE(e1, e2, ...) VELOX_USER_CHECK(true)
#define VELOX_USER_DCHECK_EQ(e1, e2, ...) VELOX_USER_CHECK(true)
#define VELOX_USER_DCHECK_NE(e1, e2, ...) VELOX_USER_CHECK(true)
#define VELOX_USER_DCHECK_NULL(e, ...) VELOX_USER_CHECK(true)
#define VELOX_USER_DCHECK_NOT_NULL(e, ...) VELOX_USER_CHECK(true)
#endif

#define VELOX_USER_FAIL(...)                                     \
  _VELOX_THROW(                                                  \
      ::facebook::velox::VeloxUserError,                         \
      ::facebook::velox::error_source::kErrorSourceUser.c_str(), \
      ::facebook::velox::error_code::kInvalidArgument.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define VELOX_NYI(...)                                              \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kNotImplemented.c_str(),       \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

} // namespace velox
} // namespace facebook
