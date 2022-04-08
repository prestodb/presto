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

namespace facebook::velox {

// Describes value collation in comparison.
struct CompareFlags {
  // This flag will be ignored if stopAtNull is true.
  bool nullsFirst = true;

  bool ascending = true;

  // When true, comparison should return non-0 early when sizes mismatch.
  bool equalsOnly = false;

  // When true, the compare returns std::nullopt if null encountered.
  bool stopAtNull = false;
};

} // namespace facebook::velox
