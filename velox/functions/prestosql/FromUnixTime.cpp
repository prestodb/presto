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
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions {
namespace {

inline int64_t toMillis(double unixtime) {
  if (UNLIKELY(std::isnan(unixtime))) {
    return 0;
  }
  return std::floor(unixtime * 1'000);
}

class FromUnixtimeFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decodedArgs(rows, args, context);

    auto unixtimes = decodedArgs.at(0);
    auto timezoneNames = decodedArgs.at(1);

    const auto size = rows.end();
    auto* pool = context.pool();

    auto timestamps = BaseVector::create(BIGINT(), size, pool);
    auto* rawTimestamps =
        timestamps->asFlatVector<int64_t>()->mutableRawValues();

    VectorPtr timezones;
    if (timezoneNames->isConstantMapping()) {
      auto timezoneName = timezoneNames->valueAt<StringView>(rows.begin());

      int16_t timezoneId = util::getTimeZoneID(
          std::string_view(timezoneName.data(), timezoneName.size()));
      timezones = BaseVector::createConstant(timezoneId, size, pool);

      rows.applyToSelected([&](auto row) {
        rawTimestamps[row] = toMillis(unixtimes->valueAt<double>(row));
      });
    } else {
      timezones = BaseVector::create(SMALLINT(), size, pool);
      auto* rawTimezones =
          timezones->asFlatVector<int16_t>()->mutableRawValues();

      rows.applyToSelected([&](auto row) {
        rawTimestamps[row] = toMillis(unixtimes->valueAt<double>(row));

        auto timezoneName = timezoneNames->valueAt<StringView>(row);
        rawTimezones[row] = util::getTimeZoneID(
            std::string_view(timezoneName.data(), timezoneName.size()));
      });
    }

    auto localResult = std::make_shared<RowVector>(
        pool,
        outputType,
        BufferPtr(nullptr),
        rows.size(),
        std::vector<VectorPtr>{timestamps, timezones},
        0 /*nullCount*/);

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // double, varchar -> timestamp with time zone
    return {exec::FunctionSignatureBuilder()
                .returnType("timestamp with time zone")
                .argumentType("double")
                .argumentType("varchar")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_from_unixtime,
    FromUnixtimeFunction::signatures(),
    std::make_unique<FromUnixtimeFunction>());

} // namespace facebook::velox::functions
