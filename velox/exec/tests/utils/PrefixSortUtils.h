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

#include <limits>
#include "velox/common/base/PrefixSortConfig.h"

namespace facebook::velox::exec::test {
/// PrefixSortConfig is obtained from QueryConfig in production code. In many
/// UTs etc. HiveConnector`s UT, there is no QueryConfig. This method returns a
/// default PrefixSortConfig for these cases.
inline velox::common::PrefixSortConfig defaultPrefixSortConfig() {
  return velox::common::PrefixSortConfig{128, 130};
}

} // namespace facebook::velox::exec::test
