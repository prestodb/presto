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

#include "velox/common/future/VeloxPromise.h"

namespace facebook::velox::exec {

class JoinBridge {
 public:
  virtual ~JoinBridge() = default;

  // Sets this to a cancelled state and unblocks any waiting activity. This may
  // happen asynchronously before or after the result has been set.
  void cancel();

 protected:
  static void notify(std::vector<VeloxPromise<bool>> promises);

  std::mutex mutex_;
  std::vector<VeloxPromise<bool>> promises_;
  bool cancelled_{false};
};
} // namespace facebook::velox::exec
