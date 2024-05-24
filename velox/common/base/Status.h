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

// Adapted from Apache Arrow.

#pragma once

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <folly/Expected.h>
#include <folly/Likely.h>
#include <string>
#include <utility>

namespace facebook::velox {

/// The Status object is an object holding the outcome of an operation (success
/// or error).
///
/// The outcome is represented as a StatusCode, holding either a success
/// (StatusCode::kOK) or an error (any other of the StatusCode enumeration
/// values). If an error occurred, a specific error message is generally
/// attached.
///
/// The status object is commonly allocated in the stack, so it needs be compact
/// to be efficient. For the common success case, its size is a single (nullptr)
/// pointer. For failure cases, it allocates an external object containing the
/// StatusCode and error message. This keeps the object compact and prevents
/// allocations in the common success case. The same strategy is used in Status
/// object from other well-known libraries like Apache Arrow, RocksDB, Kudu, and
/// others.
///
/// Simple usage:
///
///  Status operation() {
///    if (noMoreMemory) {
///      return Status::OutOfMemory("Not enough memory to run 'operation'!");
///    }
///    return Status::OK();
///  }
///
/// Call site:
///
///  auto status = operation();
///  if (status.ok()) {
///    ...
///  } else if (status.isOutOfMemory()) {
///    (gracefully handle out of memory)
///  } else if (status.isNotImplemented()) {
///    (and so on)
///  }
///
/// Other common usage patterns:
///
/// The same logic above can be implemented using helper macros:
///
///  Status operation() {
///    VELOX_RETURN_IF(noMoreMemory, Status::OutOfMemory(
///       "Not enough memory to run 'operation'!"));
///    ...
///    return Status::OK();
///  }
///
/// To ensure operations succeed (or if not, return the same status from the
/// current function):
///
///  ...
///  VELOX_RETURN_NOT_OK(operation1());
///  VELOX_RETURN_NOT_OK(operation2());
///  VELOX_RETURN_NOT_OK(operation3());
///  ...

/// This enum represents common categories of errors found in the library. These
/// are not meant to cover every specific error situation, but rather cover
/// broader categories of errors. Therefore, additions to this list should be
/// infrequent.
///
/// Errors should be further described by attaching an error message to the
/// Status object.
///
/// The error classes are loosely defined as follows:
///
/// - kOk: A successful operation. No errors.
///
/// - kUserError: An error triggered by bad input from an API user. The user
///   in this context usually means the program using Velox (or its end users).
///
/// - kTypeError: An error triggered by a logical type mismatch (e.g. expecting
///   BIGINT but REAL provided).
///
/// - kIndexError: An error triggered by the index of something being invalid or
///   out-of-bounds.
///
/// - kKeyError: An error triggered by the key of something in a map/set being
///   invalid or not found.
///
/// - kAlreadyExists: An error triggered by an operation meant to create some
///   form of resource which already exists.
///
/// - kOutOfMemory: A failure triggered by a lack of available memory to
///   complete the operation.
///
/// - kIOError: An error triggered by IO failures (e.g: network or disk/SSD read
///   error).
///
/// - kCancelled: An error triggered because a certain resource required has
///   been stopped or cancelled.
///
/// - kInvalid: An error triggered by an invalid program state. Usually
///   triggered by bugs.
///
/// - kUnknownError: An error triggered by an unknown cause. Also usually
///   triggered by bugs. Should be used scarcely, favoring a more specific error
///   class above.
///
/// - kNotImplemented: An error triggered by a feature not being implemented
///   yet.
///
enum class StatusCode : int8_t {
  kOK = 0,
  kUserError = 1,
  kTypeError = 2,
  kIndexError = 3,
  kKeyError = 4,
  kAlreadyExists = 5,
  kOutOfMemory = 6,
  kIOError = 7,
  kCancelled = 8,
  kInvalid = 9,
  kUnknownError = 10,
  kNotImplemented = 11,
};
std::string_view toString(StatusCode code);

class [[nodiscard]] Status {
 public:
  // Create a success status.
  constexpr Status() noexcept : state_(nullptr) {}

  ~Status() noexcept {
    if (FOLLY_UNLIKELY(state_ != nullptr)) {
      deleteState();
    }
  }

  explicit Status(StatusCode code);

  Status(StatusCode code, std::string msg);

  // Copy the specified status.
  inline Status(const Status& s);
  inline Status& operator=(const Status& s);

  // Move the specified status.
  inline Status(Status&& s) noexcept;
  inline Status& operator=(Status&& s) noexcept;

  inline bool operator==(const Status& other) const noexcept;
  inline bool operator!=(const Status& other) const noexcept {
    return !(*this == other);
  }

  // AND the statuses.
  inline Status operator&(const Status& s) const noexcept;
  inline Status operator&(Status&& s) const noexcept;
  inline Status& operator&=(const Status& s) noexcept;
  inline Status& operator&=(Status&& s) noexcept;

  inline friend std::ostream& operator<<(std::ostream& ss, const Status& s) {
    return ss << s.toString();
  }

  /// Return a success status.
  static Status OK() {
    return Status();
  }

  // The static factory methods below do not follow the lower camel-case pattern
  // as they are meant to represent classes of errors. For example:
  //
  //   auto st1 = Status::UserError("my error"):
  //   auto st2 = Status::TypeError("my other error"):

  /// Return an error status for user errors.
  template <typename... Args>
  static Status UserError(Args&&... args) {
    return Status::fromArgs(
        StatusCode::kUserError, std::forward<Args>(args)...);
  }

  /// Return an error status for type errors (such as mismatching data types)
  template <typename... Args>
  static Status TypeError(Args&&... args) {
    return Status::fromArgs(
        StatusCode::kTypeError, std::forward<Args>(args)...);
  }

  /// Return an error status when an index is out of bounds
  template <typename... Args>
  static Status IndexError(Args&&... args) {
    return Status::fromArgs(
        StatusCode::kIndexError, std::forward<Args>(args)...);
  }

  /// Return an error status for failed key lookups (e.g. column name in a
  /// table)
  template <typename... Args>
  static Status KeyError(Args&&... args) {
    return Status::fromArgs(StatusCode::kKeyError, std::forward<Args>(args)...);
  }

  /// Return an error status when something already exists (e.g. a file).
  template <typename... Args>
  static Status AlreadyExists(Args&&... args) {
    return Status::fromArgs(
        StatusCode::kAlreadyExists, std::forward<Args>(args)...);
  }

  /// Return an error status for out-of-memory conditions.
  template <typename... Args>
  static Status OutOfMemory(Args&&... args) {
    return Status::fromArgs(
        StatusCode::kOutOfMemory, std::forward<Args>(args)...);
  }

  /// Return an error status when some IO-related operation failed
  template <typename... Args>
  static Status IOError(Args&&... args) {
    return Status::fromArgs(StatusCode::kIOError, std::forward<Args>(args)...);
  }

  /// Return an error status for cancelled operation
  template <typename... Args>
  static Status Cancelled(Args&&... args) {
    return Status::fromArgs(
        StatusCode::kCancelled, std::forward<Args>(args)...);
  }

  /// Return an error status for invalid data (for example a string that fails
  /// parsing)
  template <typename... Args>
  static Status Invalid(Args&&... args) {
    return Status::fromArgs(StatusCode::kInvalid, std::forward<Args>(args)...);
  }

  /// Return an error status for unknown errors
  template <typename... Args>
  static Status UnknownError(Args&&... args) {
    return Status::fromArgs(
        StatusCode::kUnknownError, std::forward<Args>(args)...);
  }

  /// Return an error status when an operation or a combination of operation and
  /// data types is unimplemented
  template <typename... Args>
  static Status NotImplemented(Args&&... args) {
    return Status::fromArgs(
        StatusCode::kNotImplemented, std::forward<Args>(args)...);
  }

  /// Return true iff the status indicates success.
  constexpr bool ok() const {
    return (state_ == nullptr);
  }

  /// Return true iff the status indicates an user error.
  constexpr bool isUserError() const {
    return code() == StatusCode::kUserError;
  }

  /// Return true iff the status indicates a type error.
  constexpr bool isTypeError() const {
    return code() == StatusCode::kTypeError;
  }

  /// Return true iff the status indicates an out of bounds index.
  constexpr bool isIndexError() const {
    return code() == StatusCode::kIndexError;
  }

  /// Return true iff the status indicates a key lookup error.
  constexpr bool isKeyError() const {
    return code() == StatusCode::kKeyError;
  }

  /// Return true iff the status indicates that something already exists.
  constexpr bool isAlreadyExists() const {
    return code() == StatusCode::kAlreadyExists;
  }

  /// Return true iff the status indicates an out-of-memory error.
  constexpr bool isOutOfMemory() const {
    return code() == StatusCode::kOutOfMemory;
  }

  /// Return true iff the status indicates an IO-related failure.
  constexpr bool isIOError() const {
    return code() == StatusCode::kIOError;
  }

  /// Return true iff the status indicates a cancelled operation.
  constexpr bool isCancelled() const {
    return code() == StatusCode::kCancelled;
  }

  /// Return true iff the status indicates invalid data.
  constexpr bool isInvalid() const {
    return code() == StatusCode::kInvalid;
  }

  /// Return true iff the status indicates an unknown error.
  constexpr bool isUnknownError() const {
    return code() == StatusCode::kUnknownError;
  }

  /// Return true iff the status indicates an unimplemented operation.
  constexpr bool isNotImplemented() const {
    return code() == StatusCode::kNotImplemented;
  }

  /// Return a string representation of this status suitable for printing.
  ///
  /// The string "OK" is returned for success.
  std::string toString() const;

  /// Return a string representation of the status code, without the message
  /// text or POSIX code information.
  std::string_view codeAsString() const;
  static std::string_view codeAsString(StatusCode);

  /// Return the StatusCode value attached to this status.
  constexpr StatusCode code() const {
    return ok() ? StatusCode::kOK : state_->code;
  }

  /// Return the specific error message attached to this status.
  const std::string& message() const {
    static const std::string kNoMessage = "";
    return ok() ? kNoMessage : state_->msg;
  }

  /// Return a new Status with changed message, copying the existing status
  /// code.
  template <typename... Args>
  Status withMessage(Args&&... args) const {
    return fromArgs(code(), std::forward<Args>(args)...);
  }

  void warn() const;
  void warn(const std::string_view& message) const;

  [[noreturn]] void abort() const;
  [[noreturn]] void abort(const std::string_view& message) const;

 private:
  template <typename... Args>
  static Status
  fromArgs(StatusCode code, fmt::string_view fmt, Args&&... args) {
    return Status(code, fmt::vformat(fmt, fmt::make_format_args(args...)));
  }

  static Status fromArgs(StatusCode code) {
    return Status(code);
  }

  void deleteState() {
    delete state_;
    state_ = nullptr;
  }

  void copyFrom(const Status& s);
  inline void moveFrom(Status& s);

  struct State {
    StatusCode code;
    std::string msg;
  };

  // OK status has a `nullptr` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  State* state_;
};

// Copy the specified status.
Status::Status(const Status& s)
    : state_((s.state_ == nullptr) ? nullptr : new State(*s.state_)) {}

Status& Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    copyFrom(s);
  }
  return *this;
}

// Move the specified status.
Status::Status(Status&& s) noexcept : state_(s.state_) {
  s.state_ = nullptr;
}

Status& Status::operator=(Status&& s) noexcept {
  moveFrom(s);
  return *this;
}

inline bool Status::operator==(const Status& other) const noexcept {
  if (state_ == other.state_) {
    return true;
  }

  if (ok() || other.ok()) {
    return false;
  }
  return (code() == other.code()) && (message() == other.message());
}

Status Status::operator&(const Status& s) const noexcept {
  if (ok()) {
    return s;
  } else {
    return *this;
  }
}

Status Status::operator&(Status&& s) const noexcept {
  if (ok()) {
    return std::move(s);
  } else {
    return *this;
  }
}

Status& Status::operator&=(const Status& s) noexcept {
  if (ok() && !s.ok()) {
    copyFrom(s);
  }
  return *this;
}

Status& Status::operator&=(Status&& s) noexcept {
  if (ok() && !s.ok()) {
    moveFrom(s);
  }
  return *this;
}

void Status::moveFrom(Status& s) {
  delete state_;
  state_ = s.state_;
  s.state_ = nullptr;
}

// Helper Macros.

#define _VELOX_STRINGIFY(x) #x

/// Return with given status if condition is met.
#define VELOX_RETURN_IF(condition, status) \
  do {                                     \
    if (FOLLY_UNLIKELY(condition)) {       \
      return (status);                     \
    }                                      \
  } while (0)

/// Propagate any non-successful Status to the caller.
#define VELOX_RETURN_NOT_OK(status)                           \
  do {                                                        \
    ::facebook::velox::Status __s =                           \
        ::facebook::velox::internal::genericToStatus(status); \
    VELOX_RETURN_IF(!__s.ok(), __s);                          \
  } while (false)

namespace internal {

/// Common API for extracting Status from either Status or Result<T> (the latter
/// is defined in Result.h).
/// Useful for status check macros such as VELOX_RETURN_NOT_OK.
inline const Status& genericToStatus(const Status& st) {
  return st;
}
inline Status genericToStatus(Status&& st) {
  return std::move(st);
}

} // namespace internal

/// Holds a result or an error. Designed to be used by APIs that do not throw.
///
/// Here is an example of a modulo operation that doesn't throw, but indicates
/// failure using Status.
///
/// Expected<int> modulo(int a, int b) {
///   if (b == 0) {
///     return folly::makeUnexpected(Status::UserError("division by zero"));
///   }
///
///   return a % b;
/// }
///
/// Status should not be OK.
template <typename T>
using Expected = folly::Expected<T, Status>;

} // namespace facebook::velox

template <>
struct fmt::formatter<facebook::velox::Status> : fmt::formatter<std::string> {
  auto format(const facebook::velox::Status& s, format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<facebook::velox::StatusCode>
    : fmt::formatter<std::string_view> {
  auto format(facebook::velox::StatusCode code, format_context& ctx) {
    return formatter<std::string_view>::format(
        facebook::velox::toString(code), ctx);
  }
};
