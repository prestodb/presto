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
#include "velox/functions/Macros.h"
#include "velox/functions/common/DateTimeImpl.h"

namespace facebook::velox::functions {

VELOX_UDF_BEGIN(to_unixtime)
FOLLY_ALWAYS_INLINE bool call(
    double& result,
    const arg_type<Timestamp>& timestamp) {
  result = toUnixtime(timestamp);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(from_unixtime)
FOLLY_ALWAYS_INLINE bool call(
    Timestamp& result,
    const arg_type<double>& unixtime) {
  result = fromUnixtime(unixtime);
  return true;
}
VELOX_UDF_END();

} // namespace facebook::velox::functions
