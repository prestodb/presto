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

#include "velox/py/lib/PyInit.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::py {

folly::once_flag initOnceFlag;

void initializeVeloxMemoryOnce() {
  // Enable full Velox stack trace when exceptions are thrown.
  FLAGS_velox_exception_user_stacktrace_enabled = true;

  velox::memory::initializeMemoryManager({});
}

void initializeVeloxMemory() {
  // Initialize Velox once per process.
  folly::call_once(initOnceFlag, initializeVeloxMemoryOnce);
}

} // namespace facebook::velox::py
