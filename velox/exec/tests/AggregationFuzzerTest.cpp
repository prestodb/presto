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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <unordered_set>

#include <boost/random/uniform_int_distribution.hpp>
#include "velox/exec/tests/utils/AggregationFuzzerRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/DuckQueryRunner.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
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
        percentile_ = pickPercentile(fuzzer, rng);
      }

      inputs.push_back(BaseVector::createConstant(
          DOUBLE(), percentile_.value(), size, pool));
    } else {
      VELOX_CHECK(percentileType->isArray());
      VELOX_CHECK(percentileType->childAt(0)->isDouble());

      if (percentiles_.empty()) {
        percentiles_.push_back(pickPercentile(fuzzer, rng));
        percentiles_.push_back(pickPercentile(fuzzer, rng));
        percentiles_.push_back(pickPercentile(fuzzer, rng));
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
  double pickPercentile(VectorFuzzer& fuzzer, FuzzerGenerator& rng) {
    // 10% of the times generate random value in [0, 1] range.
    // 90% of the times use one of the common values.
    if (fuzzer.coinToss(0.1)) {
      return boost::random::uniform_01<double>()(rng);
    }

    static const std::vector<double> kPercentiles = {
        0.1, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999, 0.9999};

    const auto index =
        boost::random::uniform_int_distribution<uint32_t>()(rng) %
        kPercentiles.size();

    return kPercentiles[index];
  }

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

// Applies specified SQL transformation to the results before comparing. For
// example, sorts an array before comparing results of array_agg.
//
// Supports 'compare' API.
class TransformResultVerifier : public ResultVerifier {
 public:
  // @param transform fmt::format-compatible SQL expression to use to transform
  // aggregation results before comparison. The string must have a single
  // placeholder for the column name that contains aggregation results. For
  // example, "array_sort({})".
  explicit TransformResultVerifier(const std::string& transform)
      : transform_{transform} {}

  static std::shared_ptr<ResultVerifier> create(const std::string& transform) {
    return std::make_shared<TransformResultVerifier>(transform);
  }

  bool supportsCompare() override {
    return true;
  }

  bool supportsVerify() override {
    return false;
  }

  void initialize(
      const std::vector<RowVectorPtr>& /*input*/,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& /*aggregate*/,
      const std::string& aggregateName) override {
    projections_ = groupingKeys;
    projections_.push_back(
        fmt::format(fmt::runtime(transform_), aggregateName));
  }

  bool compare(const RowVectorPtr& result, const RowVectorPtr& altResult)
      override {
    return assertEqualResults({transform(result)}, {transform(altResult)});
  }

  bool verify(const RowVectorPtr& /*result*/) override {
    VELOX_UNSUPPORTED();
  }

  void reset() override {
    projections_.clear();
  }

 private:
  RowVectorPtr transform(const RowVectorPtr& data) {
    VELOX_CHECK(!projections_.empty());
    auto plan = PlanBuilder().values({data}).project(projections_).planNode();
    return AssertQueryBuilder(plan).copyResults(data->pool());
  }

  const std::string transform_;

  std::vector<std::string> projections_;
};

// Compares results of approx_distinct(x[, e]) with count(distinct x).
// For each group calculates the difference between 2 values and counts number
// of groups where difference is > 2e. If total number of groups is >= 50,
// allows 2 groups > 2e. If number of groups is small (< 50),
// expects all groups to be under 2e.
class ApproxDistinctResultVerifier : public ResultVerifier {
 public:
  bool supportsCompare() override {
    return false;
  }

  bool supportsVerify() override {
    return true;
  }

  // Compute count(distinct x) over 'input'.
  void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    auto plan =
        PlanBuilder()
            .values(input)
            .singleAggregation(groupingKeys, {makeCountDistinctCall(aggregate)})
            .planNode();

    expected_ = AssertQueryBuilder(plan).copyResults(input[0]->pool());
    groupingKeys_ = groupingKeys;
    name_ = aggregateName;
    error_ = extractError(aggregate, input[0]);
  }

  bool compare(
      const RowVectorPtr& /*result*/,
      const RowVectorPtr& /*altResult*/) override {
    VELOX_UNSUPPORTED();
  }

  bool verify(const RowVectorPtr& result) override {
    // Union 'result' with 'expected_', group by on 'groupingKeys_' and produce
    // pairs of actual and expected values per group. We cannot use join because
    // grouping keys may have nulls.
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto expectedSource = PlanBuilder(planNodeIdGenerator)
                              .values({expected_})
                              .appendColumns({"'expected' as label"})
                              .planNode();

    auto actualSource = PlanBuilder(planNodeIdGenerator)
                            .values({result})
                            .appendColumns({"'actual' as label"})
                            .planNode();

    auto mapAgg = fmt::format("map_agg(label, {}) as m", name_);
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .localPartition({}, {expectedSource, actualSource})
                    .singleAggregation(groupingKeys_, {mapAgg})
                    .project({"m['actual'] as a", "m['expected'] as e"})
                    .planNode();
    auto combined = AssertQueryBuilder(plan).copyResults(result->pool());

    auto* actual = combined->childAt(0)->as<SimpleVector<int64_t>>();
    auto* expected = combined->childAt(1)->as<SimpleVector<int64_t>>();

    const auto numGroups = result->size();
    VELOX_CHECK_EQ(numGroups, combined->size());

    std::vector<double> largeGaps;
    for (auto i = 0; i < numGroups; ++i) {
      VELOX_CHECK(!actual->isNullAt(i))
      VELOX_CHECK(!expected->isNullAt(i))

      const auto actualCnt = actual->valueAt(i);
      const auto expectedCnt = expected->valueAt(i);
      if (actualCnt != expectedCnt) {
        if (expectedCnt > 0) {
          const auto gap =
              std::abs(actualCnt - expectedCnt) * 1.0 / expectedCnt;
          if (gap > 2 * error_) {
            largeGaps.push_back(gap);
            LOG(ERROR) << fmt::format(
                "approx_distinct(x, {}) is more than 2 stddev away from "
                "count(distinct x). Difference: {}, approx_distinct: {}, "
                "count(distinct): {}. This is unusual, but doesn't necessarily "
                "indicate a bug.",
                error_,
                gap,
                actualCnt,
                expectedCnt);
          }
        } else {
          LOG(ERROR) << fmt::format(
              "count(distinct x) returned 0, but approx_distinct(x, {}) is {}",
              error_,
              actualCnt);
          return false;
        }
      }
    }

    // We expect large deviations (>2 stddev) in < 5% of values.
    if (numGroups >= 50) {
      return largeGaps.size() <= 3;
    }

    return largeGaps.empty();
  }

  void reset() override {
    expected_.reset();
  }

 private:
  static constexpr double kDefaultError = 0.023;

  static double extractError(
      const core::AggregationNode::Aggregate& aggregate,
      const RowVectorPtr& input) {
    const auto& args = aggregate.call->inputs();

    if (args.size() == 1) {
      return kDefaultError;
    }

    auto field = core::TypedExprs::asFieldAccess(args[1]);
    VELOX_CHECK_NOT_NULL(field);
    auto errorVector =
        input->childAt(field->name())->as<SimpleVector<double>>();
    return errorVector->valueAt(0);
  }

  static std::string makeCountDistinctCall(
      const core::AggregationNode::Aggregate& aggregate) {
    const auto& args = aggregate.call->inputs();
    VELOX_CHECK_GE(args.size(), 1)

    auto inputField = core::TypedExprs::asFieldAccess(args[0]);
    VELOX_CHECK_NOT_NULL(inputField)

    std::string countDistinctCall =
        fmt::format("count(distinct {})", inputField->name());

    if (aggregate.mask != nullptr) {
      countDistinctCall +=
          fmt::format(" filter (where {})", aggregate.mask->name());
    }

    return countDistinctCall;
  }

  RowVectorPtr expected_;
  std::vector<std::string> groupingKeys_;
  std::string name_;
  double error_;
};

// Verifies results of approx_percentile by checking the range of percentiles
// represented by the result value and asserting that the requested percentile
// falls into that range within 'accuracy'.
class ApproxPercentileResultVerifier : public ResultVerifier {
 public:
  bool supportsCompare() override {
    return false;
  }

  bool supportsVerify() override {
    return true;
  }

  // Compute the range of percentiles represented by each of the input values.
  void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    VELOX_CHECK(!input.empty());

    int64_t numInputs = 0;
    for (const auto& v : input) {
      numInputs += v->size();
    }

    const auto& args = aggregate.call->inputs();
    const auto& valueField = fieldName(args[0]);
    std::optional<std::string> weightField;
    if (args.size() >= 3 && args[1]->type()->isBigint()) {
      weightField = fieldName(args[1]);
    }

    groupingKeys_ = groupingKeys;
    name_ = aggregateName;

    percentiles_ = extractPercentiles(input, aggregate);
    VELOX_CHECK(!percentiles_.empty());

    accuracy_ = extractAccuracy(aggregate, input[0]);

    // Compute percentiles for all values.
    allRanges_ =
        computePercentiles(input, valueField, weightField, aggregate.mask);
    VELOX_CHECK_LE(allRanges_->size(), numInputs);
  }

  bool compare(
      const RowVectorPtr& /*result*/,
      const RowVectorPtr& /*altResult*/) override {
    VELOX_UNSUPPORTED();
  }

  bool verify(const RowVectorPtr& result) override {
    // Compute acceptable ranges of percentiles for each value in 'result'.
    auto ranges = getPercentileRanges(result);
    // VELOX_CHECK_EQ(ranges->size(), result->size() * percentiles_.size());

    auto& value = ranges->childAt(name_);
    auto* minPct = ranges->childAt("min_pct")->as<SimpleVector<double>>();
    auto* maxPct = ranges->childAt("max_pct")->as<SimpleVector<double>>();
    auto* pctIndex = ranges->childAt("pct_index")->as<SimpleVector<int64_t>>();

    for (auto i = 0; i < ranges->size(); ++i) {
      if (value->isNullAt(i)) {
        VELOX_CHECK(minPct->isNullAt(i));
        VELOX_CHECK(maxPct->isNullAt(i));
        continue;
      }

      VELOX_CHECK(!minPct->isNullAt(i));
      VELOX_CHECK(!maxPct->isNullAt(i));
      VELOX_CHECK(!pctIndex->isNullAt(i));

      const auto pct = percentiles_[pctIndex->valueAt(i)];

      std::pair<double, double> range{minPct->valueAt(i), maxPct->valueAt(i)};
      if (!checkPercentileGap(pct, range, accuracy_)) {
        return false;
      }
    }

    return true;
  }

  void reset() override {
    allRanges_.reset();
  }

 private:
  static constexpr double kDefaultAccuracy = 0.0133;

  static double extractAccuracy(
      const core::AggregationNode::Aggregate& aggregate,
      const RowVectorPtr& input) {
    const auto& args = aggregate.call->inputs();

    column_index_t accuracyIndex = 2;
    if (args.size() >= 3 && args[1]->type()->isBigint()) {
      // We have a 'w' argument.
      accuracyIndex = 3;
    }

    if (args.size() <= accuracyIndex) {
      return kDefaultAccuracy;
    }

    auto field = core::TypedExprs::asFieldAccess(args[accuracyIndex]);
    VELOX_CHECK_NOT_NULL(field);
    auto accuracyVector =
        input->childAt(field->name())->as<SimpleVector<double>>();
    return accuracyVector->valueAt(0);
  }

  static bool checkPercentileGap(
      double pct,
      const std::pair<double, double>& range,
      double accuracy) {
    double gap = 0.0;
    if (pct < range.first) {
      gap = range.first - pct;
    } else if (pct > range.second) {
      gap = pct - range.second;
    }

    if (gap > accuracy) {
      LOG(ERROR) << "approx_percentile(pct: " << pct
                 << ", accuracy: " << accuracy << ") is more than " << accuracy
                 << " away from acceptable range of [" << range.first << ", "
                 << range.second << "]. Difference: " << gap;
      return false;
    }

    return true;
  }

  static std::vector<std::string> append(
      const std::vector<std::string>& values,
      const std::vector<std::string>& newValues) {
    auto combined = values;
    combined.insert(combined.end(), newValues.begin(), newValues.end());
    return combined;
  }

  // Groups input by 'groupingKeys_'. Within each group, sorts data on
  // 'valueField', duplicates rows according to optional weight, filters out
  // NULLs and rows where mask is not true, then computes ranges of row numbers
  // and turns these into ranges of percentiles.
  //
  // @return A vector of grouping keys, followed by value column named 'name_',
  // followed by min_pct and max_pct columns.
  RowVectorPtr computePercentiles(
      const std::vector<RowVectorPtr>& input,
      const std::string& valueField,
      const std::optional<std::string>& weightField,
      const core::FieldAccessTypedExprPtr& mask) {
    VELOX_CHECK(!input.empty())
    const auto rowType = asRowType(input[0]->type());

    const bool weighted = weightField.has_value();

    std::vector<std::string> projections = groupingKeys_;
    projections.push_back(fmt::format("{} as x", valueField));
    if (weighted) {
      projections.push_back(fmt::format("{} as w", weightField.value()));
    }

    PlanBuilder planBuilder;
    planBuilder.values(input);

    if (mask != nullptr) {
      planBuilder.filter(mask->name());
    }

    planBuilder.project(projections).filter("x IS NOT NULL");

    if (weighted) {
      planBuilder.appendColumns({"sequence(1, w) as repeats"})
          .unnest(append(groupingKeys_, {"x"}), {"repeats"});
    }

    std::string partitionByClause;
    if (!groupingKeys_.empty()) {
      partitionByClause =
          fmt::format("partition by {}", folly::join(", ", groupingKeys_));
    }

    std::vector<std::string> windowCalls = {
        fmt::format(
            "row_number() OVER ({} order by x) as rn", partitionByClause),
        fmt::format(
            "count(1) OVER ({} order by x range between unbounded preceding and unbounded following) "
            "as total",
            partitionByClause),
    };

    planBuilder.window(windowCalls)
        .appendColumns({
            "(rn::double - 1.0) / total::double as lower",
            "rn::double / total::double as upper",
        })
        .singleAggregation(
            append(groupingKeys_, {"x"}),
            {"min(lower) as min_pct", "max(upper) as max_pct"})
        .project(append(
            groupingKeys_,
            {fmt::format("x as {}", name_), "min_pct", "max_pct"}));

    auto plan = planBuilder.planNode();
    return AssertQueryBuilder(plan).copyResults(input[0]->pool());
  }

  static const std::string& fieldName(const core::TypedExprPtr& expression) {
    auto field = core::TypedExprs::asFieldAccess(expression);
    VELOX_CHECK_NOT_NULL(field);
    return field->name();
  }

  // Extract 'percentile' argument.
  static std::vector<double> extractPercentiles(
      const std::vector<RowVectorPtr>& input,
      const core::AggregationNode::Aggregate& aggregate) {
    const auto args = aggregate.call->inputs();
    column_index_t percentileIndex = 1;
    if (args.size() >= 3 && args[1]->type()->isBigint()) {
      percentileIndex = 2;
    }

    const auto& percentileExpr = args[percentileIndex];

    if (auto constantExpr = core::TypedExprs::asConstant(percentileExpr)) {
      if (constantExpr->type()->isDouble()) {
        return {constantExpr->value().value<double>()};
      }

      return toList(constantExpr->valueVector());
    }

    const auto& percentileVector = input[0]->childAt(fieldName(percentileExpr));

    if (percentileVector->type()->isDouble()) {
      VELOX_CHECK(!percentileVector->isNullAt(0));
      return {percentileVector->as<SimpleVector<double>>()->valueAt(0)};
    }

    return toList(percentileVector);
  }

  static std::vector<double> toList(const VectorPtr& vector) {
    VELOX_CHECK(vector->type()->equivalent(*ARRAY(DOUBLE())));

    DecodedVector decoded(*vector);
    auto arrayVector = decoded.base()->as<ArrayVector>();

    VELOX_CHECK(!decoded.isNullAt(0));
    const auto offset = arrayVector->offsetAt(decoded.index(0));
    const auto size = arrayVector->sizeAt(decoded.index(0));

    auto* elementsVector = arrayVector->elements()->as<SimpleVector<double>>();

    std::vector<double> percentiles;
    percentiles.reserve(size);
    for (auto i = 0; i < size; ++i) {
      VELOX_CHECK(!elementsVector->isNullAt(offset + i));
      percentiles.push_back(elementsVector->valueAt(offset + i));
    }
    return percentiles;
  }

  // For each row ([k1, k2,] x) in 'result', lookup min_pct and max_pct in
  // 'allRanges_'. Return a vector of ([k1, k2,] x, min_pct, max_pct) rows.
  RowVectorPtr getPercentileRanges(const RowVectorPtr& result) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodePtr expectedSource;
    core::PlanNodePtr actualSource;
    if (result->childAt(name_)->type()->isArray()) {
      expectedSource =
          PlanBuilder(planNodeIdGenerator)
              .values({allRanges_})
              .appendColumns({fmt::format(
                  "sequence(0, {}) as s", percentiles_.size() - 1)})
              .unnest(
                  append(groupingKeys_, {name_, "min_pct", "max_pct"}),
                  {"s"},
                  "pct_index")
              .project(append(
                  groupingKeys_, {name_, "min_pct", "max_pct", "pct_index"}))
              .planNode();

      actualSource = PlanBuilder(planNodeIdGenerator)
                         .values({result})
                         .unnest(groupingKeys_, {name_}, "pct_index")
                         .project(append(
                             groupingKeys_,
                             {
                                 fmt::format("{}_e as {}", name_, name_),
                                 "null::double as min_pct",
                                 "null::double as max_pct",
                                 "pct_index - 1 as pct_index",
                             }))
                         .planNode();
    } else {
      expectedSource = PlanBuilder(planNodeIdGenerator)
                           .values({allRanges_})
                           .appendColumns({"0 as pct_index"})
                           .planNode();

      actualSource = PlanBuilder(planNodeIdGenerator)
                         .values({result})
                         .appendColumns({
                             "null::double as min_pct",
                             "null::double as max_pct",
                             "0 as pct_index",
                         })
                         .planNode();
    }

    auto plan = PlanBuilder(planNodeIdGenerator)
                    .localPartition({}, {expectedSource, actualSource})
                    .singleAggregation(
                        append(groupingKeys_, {name_, "pct_index"}),
                        {
                            "count(1) as cnt",
                            "arbitrary(min_pct) as min_pct",
                            "arbitrary(max_pct) as max_pct",
                        })
                    .filter({"cnt = 2"})
                    .planNode();
    return AssertQueryBuilder(plan).copyResults(result->pool());
  }

  std::vector<std::string> groupingKeys_;
  std::string name_;
  std::vector<double> percentiles_;
  double accuracy_;
  RowVectorPtr allRanges_;
};

} // namespace
} // namespace facebook::velox::exec::test

int main(int argc, char** argv) {
  facebook::velox::aggregate::prestosql::registerAllAggregateFunctions(
      "", false);
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::window::prestosql::registerAllWindowFunctions();
  facebook::velox::functions::prestosql::registerInternalFunctions();

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;

  auto duckQueryRunner =
      std::make_unique<facebook::velox::exec::test::DuckQueryRunner>();
  duckQueryRunner->disableAggregateFunctions({
      "skewness",
      // DuckDB results on constant inputs are incorrect. Should be NaN,
      // but DuckDB returns some random value.
      "kurtosis",
      "entropy",
  });

  // List of functions that have known bugs that cause crashes or failures.
  static const std::unordered_set<std::string> skipFunctions = {
      // https://github.com/facebookincubator/velox/issues/3493
      "stddev_pop",
      // Lambda functions are not supported yet.
      "reduce_agg",
  };

  using facebook::velox::exec::test::ApproxDistinctResultVerifier;
  using facebook::velox::exec::test::ApproxPercentileResultVerifier;
  using facebook::velox::exec::test::TransformResultVerifier;

  auto makeArrayVerifier = []() {
    return TransformResultVerifier::create("\"$internal$canonicalize\"({})");
  };

  auto makeMapVerifier = []() {
    return TransformResultVerifier::create(
        "\"$internal$canonicalize\"(map_keys({}))");
  };

  // Functions whose results verification should be skipped. These can be
  // order-dependent functions whose results depend on the order of input rows,
  // or functions that return complex-typed results containing floating-point
  // fields. For some functions, the result can be transformed to a value that
  // can be verified. If such transformation exists, it can be specified to be
  // used for results verification. If no transformation is specified, results
  // are not verified.
  static const std::unordered_map<
      std::string,
      std::shared_ptr<facebook::velox::exec::test::ResultVerifier>>
      customVerificationFunctions = {
          // Order-dependent functions.
          {"approx_distinct", std::make_shared<ApproxDistinctResultVerifier>()},
          {"approx_set", nullptr},
          {"approx_percentile",
           std::make_shared<ApproxPercentileResultVerifier>()},
          {"arbitrary", nullptr},
          {"any_value", nullptr},
          {"array_agg", makeArrayVerifier()},
          {"set_agg", makeArrayVerifier()},
          {"set_union", makeArrayVerifier()},
          {"map_agg", makeMapVerifier()},
          {"map_union", makeMapVerifier()},
          {"map_union_sum", makeMapVerifier()},
          {"max_by", nullptr},
          {"min_by", nullptr},
          {"multimap_agg",
           TransformResultVerifier::create(
               "transform_values({}, (k, v) -> \"$internal$canonicalize\"(v))")},
          // Semantically inconsistent functions
          {"skewness", nullptr},
          {"kurtosis", nullptr},
          {"entropy", nullptr},
          // https://github.com/facebookincubator/velox/issues/6330
          {"max_data_size_for_stats", nullptr},
          {"sum_data_size_for_stats", nullptr},
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
  return Runner::run(initialSeed, std::move(duckQueryRunner), options);
}
