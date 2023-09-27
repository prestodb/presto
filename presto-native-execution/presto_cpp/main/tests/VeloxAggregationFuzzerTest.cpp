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

#include <boost/random/uniform_int_distribution.hpp>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <unordered_set>
#include "presto_cpp/main/tests/PrestoQueryRunner.h"
#include "velox/exec/tests/utils/AggregationFuzzerRunner.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DEFINE_string(
    only,
    "",
    "If specified, Fuzzer will only choose functions from "
    "this comma separated list of function names "
    "(e.g: --only \"min\" or --only \"sum,avg\").");

namespace facebook::velox::exec::test {
namespace {

class MinMaxInputGenerator : public InputGenerator {
 public:
  MinMaxInputGenerator(const std::string& name) : indexOfN_{indexOfN(name)} {}

  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    // TODO Generate inputs free of nested nulls.
    if (types.size() <= indexOfN_) {
      return {};
    }

    // Make sure to use the same value of 'n' for all batches in a given Fuzzer
    // iteration.
    if (!n_.has_value()) {
      n_ = boost::random::uniform_int_distribution<int64_t>(0, 9'999)(rng);
    }

    const auto size = fuzzer.getOptions().vectorSize;

    std::vector<VectorPtr> inputs;
    inputs.reserve(types.size());
    for (auto i = 0; i < types.size() - 1; ++i) {
      inputs.push_back(fuzzer.fuzz(types[i]));
    }

    VELOX_CHECK(
        types.back()->isBigint(),
        "Unexpected type: {}",
        types.back()->toString())
    inputs.push_back(
        BaseVector::createConstant(BIGINT(), n_.value(), size, pool));
    return inputs;
  }

  void reset() override {
    n_.reset();
  }

 private:
  // Returns zero-based index of the 'n' argument, 1 for min and max. 2 for
  // min_by and max_by.
  static int32_t indexOfN(const std::string& name) {
    if (name == "min" || name == "max") {
      return 1;
    }

    if (name == "min_by" || name == "max_by") {
      return 2;
    }

    VELOX_FAIL("Unexpected function name: {}", name)
  }

  // Zero-based index of the 'n' argument.
  const int32_t indexOfN_;
  std::optional<int64_t> n_;
};

class ApproxDistinctInputGenerator : public InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    if (types.size() != 2) {
      return {};
    }

    // Make sure to use the same value of 'e' for all batches in a given Fuzzer
    // iteration.
    if (!e_.has_value()) {
      // Generate value in [0.0040625, 0.26] range.
      static constexpr double kMin = 0.0040625;
      static constexpr double kMax = 0.26;
      e_ = kMin + (kMax - kMin) * boost::random::uniform_01<double>()(rng);
    }

    const auto size = fuzzer.getOptions().vectorSize;

    VELOX_CHECK(
        types.back()->isDouble(),
        "Unexpected type: {}",
        types.back()->toString())
    return {
        fuzzer.fuzz(types[0]),
        BaseVector::createConstant(DOUBLE(), e_.value(), size, pool)};
  }

  void reset() override {
    e_.reset();
  }

 private:
  std::optional<double> e_;
};

class ApproxPercentileInputGenerator : public InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    // The arguments are: x, [w], percentile(s), [accuracy].
    //
    // First argument is always 'x'. If second argument's type is BIGINT, then
    // it is 'w'. Otherwise, it is percentile(x).

    const auto size = fuzzer.getOptions().vectorSize;

    std::vector<VectorPtr> inputs;
    inputs.reserve(types.size());
    inputs.push_back(fuzzer.fuzz(types[0]));

    if (types[1]->isBigint()) {
      velox::test::VectorMaker vectorMaker{pool};
      auto weight = vectorMaker.flatVector<int64_t>(size, [&](auto row) {
        return boost::random::uniform_int_distribution<int64_t>(1, 1'000)(rng);
      });

      inputs.push_back(weight);
    }

    const int percentileTypeIndex = types[1]->isBigint() ? 2 : 1;
    const TypePtr& percentileType = types[percentileTypeIndex];
    if (percentileType->isDouble()) {
      if (!percentile_.has_value()) {
        // Generate value in [0, 1] range.
        percentile_ = boost::random::uniform_01<double>()(rng);
      }

      inputs.push_back(BaseVector::createConstant(
          DOUBLE(), percentile_.value(), size, pool));
    } else {
      VELOX_CHECK(percentileType->isArray());
      VELOX_CHECK(percentileType->childAt(0)->isDouble());

      if (percentiles_.empty()) {
        percentiles_.push_back(boost::random::uniform_01<double>()(rng));
        percentiles_.push_back(boost::random::uniform_01<double>()(rng));
        percentiles_.push_back(boost::random::uniform_01<double>()(rng));
      }

      auto arrayVector =
          BaseVector::create<ArrayVector>(ARRAY(DOUBLE()), 1, pool);
      auto elementsVector = arrayVector->elements()->asFlatVector<double>();
      elementsVector->resize(percentiles_.size());
      for (auto i = 0; i < percentiles_.size(); ++i) {
        elementsVector->set(i, percentiles_[i]);
      }
      arrayVector->setOffsetAndSize(0, 0, percentiles_.size());

      inputs.push_back(BaseVector::wrapInConstant(size, 0, arrayVector));
    }

    if (types.size() > percentileTypeIndex + 1) {
      // Last argument is 'accuracy'.
      VELOX_CHECK(types.back()->isDouble());
      if (!accuracy_.has_value()) {
        accuracy_ = boost::random::uniform_01<double>()(rng);
      }

      inputs.push_back(
          BaseVector::createConstant(DOUBLE(), accuracy_.value(), size, pool));
    }

    return inputs;
  }

  void reset() override {
    percentile_.reset();
    percentiles_.clear();
    accuracy_.reset();
  }

 private:
  std::optional<double> percentile_;
  std::vector<double> percentiles_;
  std::optional<double> accuracy_;
};

std::unordered_map<std::string, std::shared_ptr<InputGenerator>>
getCustomInputGenerators() {
  return {
      {"min", std::make_shared<MinMaxInputGenerator>("min")},
      {"min_by", std::make_shared<MinMaxInputGenerator>("min_by")},
      {"max", std::make_shared<MinMaxInputGenerator>("max")},
      {"max_by", std::make_shared<MinMaxInputGenerator>("max_by")},
      {"approx_distinct", std::make_shared<ApproxDistinctInputGenerator>()},
      {"approx_set", std::make_shared<ApproxDistinctInputGenerator>()},
      {"approx_percentile", std::make_shared<ApproxPercentileInputGenerator>()},
  };
}

} // namespace
} // namespace facebook::velox::exec::test

int main(int argc, char** argv) {
  facebook::velox::aggregate::prestosql::registerAllAggregateFunctions();
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::functions::prestosql::registerInternalFunctions();

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::init(&argc, &argv);

  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;

  // List of functions that have known bugs that cause crashes or failures.
  static const std::unordered_set<std::string> skipFunctions = {
      // https://github.com/facebookincubator/velox/issues/3493
      "stddev_pop",
      // https://github.com/facebookincubator/velox/issues/6344
      "avg_merge",
      "avg_merge_extract_double",
      "avg_merge_extract_real",
      // Lambda functions are not supported yet.
      "reduce_agg",
      // TODO Allow skipping all companion functions, perhaps, by adding a flag
      // to register API to disable registration of companion functions.
      "approx_distinct_partial",
      "approx_distinct_merge",
      "approx_set_partial",
      "approx_percentile_partial",
      "approx_percentile_merge",
  };

  // Functions whose results verification should be skipped. These can be
  // order-dependent functions whose results depend on the order of input rows,
  // or functions that return complex-typed results containing floating-point
  // fields. For some functions, the result can be transformed to a value that
  // can be verified. If such transformation exists, it can be specified to be
  // used for results verification. If no transformation is specified, results
  // are not verified.
  static const std::unordered_map<std::string, std::string>
      customVerificationFunctions = {
          // Order-dependent functions.
          {"approx_distinct", ""},
          {"approx_distinct_partial", ""},
          {"approx_distinct_merge", ""},
          {"approx_set", ""},
          {"approx_set_partial", ""},
          {"approx_set_merge", ""},
          {"approx_percentile", ""},
          {"approx_percentile_partial", ""},
          {"approx_percentile_merge", ""},
          {"arbitrary", ""},
          {"array_agg", "\"$internal$canonicalize\"({})"},
          {"array_agg_partial", "\"$internal$canonicalize\"({})"},
          {"array_agg_merge", "\"$internal$canonicalize\"({})"},
          {"array_agg_merge_extract", "\"$internal$canonicalize\"({})"},
          {"set_agg", "\"$internal$canonicalize\"({})"},
          {"set_union", "\"$internal$canonicalize\"({})"},
          {"map_agg", "\"$internal$canonicalize\"(map_keys({}))"},
          {"map_union", "\"$internal$canonicalize\"(map_keys({}))"},
          {"map_union_sum", "\"$internal$canonicalize\"(map_keys({}))"},
          {"max_by", ""},
          {"min_by", ""},
          {"multimap_agg",
           "transform_values({}, (k, v) -> \"$internal$canonicalize\"(v))"},
          // TODO: Skip result verification of companion functions that return
          // complex types that contain floating-point fields for now, until we
          // fix
          // test utilities in QueryAssertions to tolerate floating-point
          // imprecision in complex types.
          // https://github.com/facebookincubator/velox/issues/4481
          {"avg_partial", ""},
          {"avg_merge", ""},
          // Semantically inconsistent functions
          {"skewness", ""},
          {"kurtosis", ""},
          {"entropy", ""},
          // https://github.com/facebookincubator/velox/issues/6330
          {"max_data_size_for_stats", ""},
          {"sum_data_size_for_stats", ""},
      };

  using Runner = facebook::velox::exec::test::AggregationFuzzerRunner;

  Runner::Options options;
  options.onlyFunctions = FLAGS_only;
  options.skipFunctions = skipFunctions;
  options.customVerificationFunctions = customVerificationFunctions;
  options.customInputGenerators =
      facebook::velox::exec::test::getCustomInputGenerators();
  options.timestampPrecision =
      facebook::velox::VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  options.queryConfigs = {{"presto.array_agg.ignore_nulls", "true"}};

  auto prestoQueryRunner =
      std::make_unique<facebook::presto::test::PrestoQueryRunner>(
          "http://127.0.0.1:8080", "mbasmanova");

  return Runner::run(initialSeed, std::move(prestoQueryRunner), options);
}
