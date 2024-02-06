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

#include "velox/expression/StringWriter.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::exec {

/// This class provides cast hooks to allow different behaviors of CastExpr and
/// SparkCastExpr. The main purpose is crate customized cast implementation by
/// taking full usage of existing cast expression.
class CastHooks {
 public:
  virtual ~CastHooks() = default;

  virtual Timestamp castStringToTimestamp(const StringView& view) const = 0;

  virtual int32_t castStringToDate(const StringView& dateString) const = 0;

  // Cast from timestamp to string and write the result to string writer.
  virtual void castTimestampToString(
      const Timestamp& timestamp,
      StringWriter<false>& out,
      const date::time_zone* timeZone = nullptr) const = 0;

  // Returns whether legacy cast semantics are enabled.
  virtual bool legacy() const = 0;

  // Trims all leading and trailing UTF8 whitespaces.
  virtual StringView removeWhiteSpaces(const StringView& view) const = 0;

  // Returns whether to cast to int by truncate.
  virtual bool truncate() const = 0;
};
} // namespace facebook::velox::exec
