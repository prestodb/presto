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

#include "velox/common/base/VeloxException.h"

#include <folly/synchronization/AtomicStruct.h>

namespace facebook {
namespace velox {

ExceptionContext& getExceptionContext() {
  thread_local ExceptionContext context;
  return context;
}

VeloxException::VeloxException(
    const char* file,
    size_t line,
    const char* function,
    std::string_view failingExpression,
    std::string_view message,
    std::string_view errorSource,
    std::string_view errorCode,
    bool isRetriable,
    std::string_view exceptionName)
    : VeloxException(State::make([&](auto& state) {
        state.exceptionName = exceptionName;
        state.file = file;
        state.line = line;
        state.function = function;
        state.failingExpression = failingExpression;
        state.message = message;
        state.errorSource = errorSource;
        state.errorCode = errorCode;
        state.context = getExceptionContext().message();
        state.isRetriable = isRetriable;
      })) {}

namespace {

// returns whether VeloxException stacktraces are enabled and whether, if they
// are rate-limited, whether the rate-limit check passes
bool isStackTraceEnabled() {
  using namespace std::literals::chrono_literals;

  if (!FLAGS_velox_exception_stacktrace) {
    // VeloxException stacktraces are disabled
    return false;
  }

  // not static so the gflag can be manipulated at runtime
  if (0 == FLAGS_velox_exception_stacktrace_rate_limit_ms) {
    // VeloxException stacktraces are not rate-limited
    return true;
  }

  static folly::AtomicStruct<std::chrono::steady_clock::time_point> last;

  auto const now = std::chrono::steady_clock::now();
  auto latest = last.load(std::memory_order_relaxed);
  if (now < latest +
          std::chrono::milliseconds(
                FLAGS_velox_exception_stacktrace_rate_limit_ms)) {
    // VeloxException stacktraces are rate-limited and the rate-limit check
    // failed
    return false;
  }

  // VeloxException stacktraces are rate-limited and the rate-limit check
  // passed
  //
  // the cas happens only here, so the rate-limit check in effect gates not only
  // computation of the stacktrace but also contention on this atomic variable
  return last.compare_exchange_strong(latest, now, std::memory_order_relaxed);
}

} // namespace

template <typename F>
std::shared_ptr<const VeloxException::State> VeloxException::State::make(F f) {
  auto state = std::make_shared<VeloxException::State>();
  if (isStackTraceEnabled()) {
    // new v.s. make_unique to avoid any extra frames from make_unique
    state->stackTrace.reset(new process::StackTrace());
  }
  f(*state);
  return state;
}

/*
Not much to say. Constructs the elaborate message from the available
pieces of information.
 */
void VeloxException::State::finalize() const {
  assert(elaborateMessage.empty());

  // Fill elaborateMessage_
  if (!exceptionName.empty()) {
    elaborateMessage += "Exception: ";
    elaborateMessage += exceptionName;
    elaborateMessage += '\n';
  }

  if (!errorSource.empty()) {
    elaborateMessage += "Error Source: ";
    elaborateMessage += errorSource;
    elaborateMessage += '\n';
  }

  if (!errorCode.empty()) {
    elaborateMessage += "Error Code: ";
    elaborateMessage += errorCode;
    elaborateMessage += '\n';
  }

  if (!message.empty()) {
    elaborateMessage += "Reason: ";
    elaborateMessage += message;
    elaborateMessage += '\n';
  }

  elaborateMessage += "Retriable: ";
  elaborateMessage += isRetriable ? "True" : "False";
  elaborateMessage += '\n';

  if (!failingExpression.empty()) {
    elaborateMessage += "Expression: ";
    elaborateMessage += failingExpression;
    elaborateMessage += '\n';
  }

  if (!context.empty()) {
    elaborateMessage += "Context: " + context + "\n";
  }

  if (function) {
    elaborateMessage += "Function: ";
    elaborateMessage += function;
    elaborateMessage += '\n';
  }

  if (file) {
    elaborateMessage += "File: ";
    elaborateMessage += file;
    elaborateMessage += '\n';
  }

  if (line) {
    elaborateMessage += "Line: ";
    auto len = elaborateMessage.size();
    size_t t = line;
    do {
      elaborateMessage += static_cast<char>('0' + t % 10);
      t /= 10;
    } while (t);
    reverse(elaborateMessage.begin() + len, elaborateMessage.end());
    elaborateMessage += '\n';
  }

  elaborateMessage += "Stack trace:\n";
  if (stackTrace) {
    elaborateMessage += stackTrace->toString();
  } else {
    elaborateMessage += "Stack trace has been disabled. ";
    elaborateMessage += "Use --velox_exception_stacktrace=true to enable it.\n";
  }
}

const char* VeloxException::State::what() const noexcept {
  try {
    folly::call_once(once, [&] { finalize(); });
    return elaborateMessage.c_str();
  } catch (...) {
    return "<unknown failure in VeloxException::what>";
  }
}

} // namespace velox
} // namespace facebook
