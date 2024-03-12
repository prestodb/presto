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

#include "velox/dwio/common/ResultOrActions.h"

namespace facebook::velox::dwio::common {

enum class ReaderStepState {
  // Success, will contain the result
  kSuccess,

  // IO is needed to read more data. There will be one or more actions
  // (callbacks) to do the IO.
  kNeedsIO,
};

template <typename ResultType>
struct StateAndResultOrIoActions {
  ReaderStepState state;
  ResultOrActions<ResultType> resultOrIoActions;
};

using StateAndIoActions = StateAndResultOrIoActions<folly::Unit>;

} // namespace facebook::velox::dwio::common
