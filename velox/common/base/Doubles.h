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

/// Comparing double against int64:max() like the following would get compile
/// time error on some compilers(e.g. clang 12 and above):
///
/// ```
/// double_value <= int64::max()
/// double_value >= int64::max()
/// ```
///
/// Here `int64::max()` will be implicitly converted to double, but due to the
/// floating point nature of double, converting `int64::max()` to double lose
/// precision(see [1]), so instead of comparing double with int64:max(), we
/// suggest compare it with the max double value below int64:max() 2 ^ 63 -
/// 1024 for < or <=, 2 ^ 63 for >, >=.
///
/// [1].https://en.wikipedia.org/wiki/Double-precision_floating-point_format#Precision_limitations_on_integer_values

/// 2 ^ 63 - 1024
static constexpr double kMaxDoubleBelowInt64Max = 9223372036854774784.0;
/// 2 ^ 63
static constexpr double kMinDoubleAboveInt64Max = 9223372036854775808.0;
} // namespace facebook::velox
