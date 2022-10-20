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
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/expression/VectorFunction.h"
#include "velox/external/date/tz.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

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
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    const auto* timestamps =
        static_cast<const Timestamp*>(args[0]->valuesAsVoid());

    // Initialize flat results vector.
    BaseVector::ensureWritable(rows, BIGINT(), context.pool(), result);
    auto rawResults = result->as<FlatVector<int64_t>>()->mutableRawValues();

    // Check if we need to adjust the current UTC timestamps to
    // the user provided session timezone.
    const auto* timeZone =
        getTimeZoneIfNeeded(context.execCtx()->queryCtx()->config());
    if (timeZone != nullptr) {
      rows.applyToSelected([&](int row) {
        auto timestamp = timestamps[row];
        timestamp.toTimezone(*timeZone);
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

class DateTimeBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  DateTimeBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerDateTimeFunctions();
    registerVectorFunction(
        "hour_vector",
        HourFunction::signatures(),
        std::make_unique<HourFunction>());
  }

  void run(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    constexpr vector_size_t size = 10'000;

    VectorFuzzer::Options opts;
    opts.vectorSize = 10'000;
    auto data = vectorMaker_.rowVector(
        {VectorFuzzer(opts, pool()).fuzzFlat(TIMESTAMP())});
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", functionName), data->type());
    suspender.dismiss();

    doRun(exprSet, data);
  }

  void runDateTrunc(const std::string& unit) {
    folly::BenchmarkSuspender suspender;
    VectorFuzzer::Options opts;
    opts.vectorSize = 10'000;
    auto data = vectorMaker_.rowVector(
        {VectorFuzzer(opts, pool()).fuzzFlat(TIMESTAMP())});
    auto exprSet = compileExpression(
        fmt::format("date_trunc('{}', c0)", unit), data->type());
    suspender.dismiss();

    doRun(exprSet, data);
  }

  void doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(truncYear) {
  DateTimeBenchmark benchmark;
  benchmark.runDateTrunc("year");
}

BENCHMARK(truncMonth) {
  DateTimeBenchmark benchmark;
  benchmark.runDateTrunc("month");
}

BENCHMARK(truncDay) {
  DateTimeBenchmark benchmark;
  benchmark.runDateTrunc("day");
}

BENCHMARK(truncHour) {
  DateTimeBenchmark benchmark;
  benchmark.runDateTrunc("hour");
}

BENCHMARK(truncMinute) {
  DateTimeBenchmark benchmark;
  benchmark.runDateTrunc("minute");
}

BENCHMARK(truncSecond) {
  DateTimeBenchmark benchmark;
  benchmark.runDateTrunc("second");
}

BENCHMARK(year) {
  DateTimeBenchmark benchmark;
  benchmark.run("year");
}

BENCHMARK(month) {
  DateTimeBenchmark benchmark;
  benchmark.run("month");
}

BENCHMARK(day) {
  DateTimeBenchmark benchmark;
  benchmark.run("day");
}

BENCHMARK(hour) {
  DateTimeBenchmark benchmark;
  benchmark.run("hour");
}

BENCHMARK_RELATIVE(hour_vector) {
  DateTimeBenchmark benchmark;
  benchmark.run("hour_vector");
}

BENCHMARK(minute) {
  DateTimeBenchmark benchmark;
  benchmark.run("minute");
}

BENCHMARK(second) {
  DateTimeBenchmark benchmark;
  benchmark.run("second");
}
} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}
