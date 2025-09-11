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

namespace facebook::velox::expression {

inline constexpr const char* kAnd = "and";
inline constexpr const char* kOr = "or";
inline constexpr const char* kSwitch = "switch";
inline constexpr const char* kIf = "if";
inline constexpr const char* kCoalesce = "coalesce";
inline constexpr const char* kCast = "cast";
inline constexpr const char* kTryCast = "try_cast";
inline constexpr const char* kTry = "try";
inline constexpr const char* kRowConstructor = "row_constructor";

} // namespace facebook::velox::expression
