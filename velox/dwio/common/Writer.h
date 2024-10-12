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
    kFinishing = 2,
    kAborted = 3,
    kClosed = 4,
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

  /// Invokes to finish the writing before close call. For logical writer like
  /// hive sorting writer which is built on top of a physical file writer, it
  /// sorts the buffered data and flush them to the physical file writer. This
  /// process might take very long time so we allow to yield in the middle of
  /// this process in favor of the other concurrent running threads in a query
  /// system. It returns false if the finish process needs to continue,
  /// otherwise true. Ihis should be called repeatedly when it returns false
  /// until it returns true. Data can no longer be written after the first
  /// finish call.
  ///
  /// NOTE: this must be called before close().
  virtual bool finish() = 0;

  /// Invokes closes the writer. Data can no longer be written.
  ///
  /// NOTE: this must be called after the last finish() which returns true.
  virtual void close() = 0;

  /// Aborts the writing by closing the writer and dropping everything.
  /// Data can no longer be written.
  virtual void abort() = 0;

 protected:
  bool isRunning() const;
  bool isFinishing() const;

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

template <>
struct fmt::formatter<facebook::velox::dwio::common::Writer::State>
    : formatter<std::string> {
  auto format(
      facebook::velox::dwio::common::Writer::State s,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::dwio::common::Writer::stateString(s), ctx);
  }
};
