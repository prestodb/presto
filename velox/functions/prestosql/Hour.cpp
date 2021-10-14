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

#include <folly/CppAttributes.h>
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/external/date/tz.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {
namespace {

class HourFunction : public exec::VectorFunction {
 public:
  const date::time_zone* FOLLY_NULLABLE
  getTimeZoneIfNeeded(const core::QueryConfig& config) const {
    const date::time_zone* timeZone = nullptr;
    if (config.adjustTimestampToTimezone()) {
      auto sessionTzName = config.sessionTimezone();
      if (!sessionTzName.empty()) {
        timeZone = date::locate_zone(sessionTzName);
      }
    }
    return timeZone;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /*caller*/,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    const auto* timestamps =
        static_cast<const Timestamp*>(args[0]->valuesAsVoid());

    // Initialize flat results vector.
    BaseVector::ensureWritable(rows, BIGINT(), context->pool(), result);
    auto rawResults = (*result)->as<FlatVector<int64_t>>()->mutableRawValues();

    // Check if we need to adjust the current UTC timestamps to
    // the user provided session timezone.
    const auto* timeZone =
        getTimeZoneIfNeeded(context->execCtx()->queryCtx()->config());
    if (timeZone != nullptr) {
      rows.applyToSelected([&](int row) {
        auto timestamp = timestamps[row];
        timestamp.toTimezoneUTC(*timeZone);
        int64_t seconds = timestamp.getSeconds();
        std::tm dateTime;
        gmtime_r((const time_t*)&seconds, &dateTime);
        rawResults[row] = dateTime.tm_hour;
      });
    } else {
      rows.applyToSelected([&](int row) {
        int64_t seconds = timestamps[row].getSeconds();
        std::tm dateTime;
        gmtime_r((const time_t*)&seconds, &dateTime);
        rawResults[row] = dateTime.tm_hour;
      });
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // timestamp -> bigint
    return {exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("timestamp")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_hour,
    HourFunction::signatures(),
    std::make_unique<HourFunction>());

} // namespace facebook::velox::functions
