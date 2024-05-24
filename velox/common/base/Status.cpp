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

#include "velox/common/base/Status.h"

#include <glog/logging.h>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <sstream>

namespace facebook::velox {

std::string_view toString(StatusCode code) {
  switch (code) {
    case StatusCode::kOK:
      return "OK";
    case StatusCode::kUserError:
      return "User error";
    case StatusCode::kTypeError:
      return "Type error";
    case StatusCode::kIndexError:
      return "Index error";
    case StatusCode::kKeyError:
      return "Key error";
    case StatusCode::kAlreadyExists:
      return "Already exists";
    case StatusCode::kOutOfMemory:
      return "Out of memory";
    case StatusCode::kIOError:
      return "IOError";
    case StatusCode::kCancelled:
      return "Cancelled";
    case StatusCode::kInvalid:
      return "Invalid";
    case StatusCode::kUnknownError:
      return "Unknown error";
    case StatusCode::kNotImplemented:
      return "NotImplemented";
  }
  return ""; // no-op
}

Status::Status(StatusCode code) {
  state_ = new State;
  state_->code = code;
}

Status::Status(StatusCode code, std::string msg) {
  if (FOLLY_UNLIKELY(code == StatusCode::kOK)) {
    throw std::invalid_argument("Cannot construct ok status with message");
  }
  state_ = new State;
  state_->code = code;
  state_->msg = std::move(msg);
}

void Status::copyFrom(const Status& s) {
  delete state_;
  if (s.state_ == nullptr) {
    state_ = nullptr;
  } else {
    state_ = new State(*s.state_);
  }
}

std::string_view Status::codeAsString() const {
  if (state_ == nullptr) {
    return "OK";
  }
  return ::facebook::velox::toString(code());
}

std::string Status::toString() const {
  std::string result(codeAsString());
  if (state_ == nullptr) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  return result;
}

void Status::abort() const {
  abort("");
}

void Status::abort(const std::string_view& message) const {
  std::cerr << "-- Velox Fatal Error --\n";
  if (!message.empty()) {
    std::cerr << message << "\n";
  }
  std::cerr << toString() << std::endl;
  std::abort();
}

void Status::warn() const {
  LOG(WARNING) << toString();
}

void Status::warn(const std::string_view& message) const {
  LOG(WARNING) << message << ": " << toString();
}

} // namespace facebook::velox
