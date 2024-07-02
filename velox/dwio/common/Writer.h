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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "velox/vector/ComplexVector.h"

namespace facebook::velox::dwio::common {

/// Abstract writer class.
///
/// Writer object is used to write a single file.
///
/// Writer objects are created through factories implementing
/// WriterFactory interface.
class Writer {
 public:
  /// Defines the states of a file writer.
  enum class State {
    kInit = 0,
    kRunning = 1,
    kAborted = 2,
    kClosed = 3,
  };
  static std::string stateString(State state);

  virtual ~Writer() = default;

  State state() const {
    return state_;
  }

  /// Appends 'data' to writer. Data might still be in memory and not
  /// yet written to the file.
  virtual void write(const VectorPtr& data) = 0;

  /// Forces the writer to flush data to the file.
  /// Does not close the writer.
  virtual void flush() = 0;

  /// Invokes flush and closes the writer.
  ///  Data can no longer be written.
  virtual void close() = 0;

  /// Aborts the writing by closing the writer and dropping everything.
  /// Data can no longer be written.
  virtual void abort() = 0;

 protected:
  bool isRunning() const;

  void checkRunning() const;

  /// Invoked to set writer 'state_' to new 'state'.
  void setState(State state);

  /// Validates the state transition from 'oldState' to 'newState'.
  static void checkStateTransition(State oldState, State newState);

  State state_{State::kInit};
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    Writer::State state) {
  os << Writer::stateString(state);
  return os;
}

} // namespace facebook::velox::dwio::common
