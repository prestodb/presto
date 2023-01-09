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

#include <folly/io/async/AsyncSignalHandler.h>

namespace facebook::presto {

class PrestoServer;

class SignalHandler : private folly::AsyncSignalHandler {
 public:
  explicit SignalHandler(PrestoServer* prestoServer);

 private:
  void signalReceived(int signum) noexcept override;

 private:
  PrestoServer* prestoServer_{nullptr};
};

}; // namespace facebook::presto
