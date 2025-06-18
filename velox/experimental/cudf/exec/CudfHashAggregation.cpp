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

#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"

#include "velox/exec/Aggregate.h"
#include "velox/exec/PrefixSort.h"
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"

#include <cudf/binaryop.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/reduction.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/unary.hpp>

namespace {

using namespace facebook::velox;

#define DEFINE_SIMPLE_AGGREGATOR(Name, name, KIND)                            \
  struct Name##Aggregator : cudf_velox::CudfHashAggregation::Aggregator {     \
    Name##Aggregator(                                                         \
        core::AggregationNode::Step step,                                     \
        uint32_t inputIndex,                                                  \
        VectorPtr constant,                                                   \
        bool is_global)                                                       \
        : Aggregator(                                                         \
              step,                                                           \
              cudf::aggregation::KIND,                                        \
              inputIndex,                                                     \
              constant,                                                       \
              is_global) {}                                                   \
                                                                              \
    void addGroupbyRequest(                                                   \
        cudf::table_view const& tbl,                                          \
        std::vector<cudf::groupby::aggregation_request>& requests) override { \
      VELOX_CHECK(                                                            \
          constant == nullptr,                                                \
          #Name "Aggregator does not yet support constant input");            \
      auto& request = requests.emplace_back();                                \
      output_idx = requests.size() - 1;                                       \
      request.values = tbl.column(inputIndex);                                \
      request.aggregations.push_back(                                         \
          cudf::make_##name##_aggregation<cudf::groupby_aggregation>());      \
    }                                                                         \
                                                                              \
    std::unique_ptr<cudf::column> makeOutputColumn(                           \
        std::vector<cudf::groupby::aggregation_result>& results,              \
        rmm::cuda_stream_view stream) override {                              \
      return std::move(results[output_idx].results[0]);                       \
    }                                                                         \
                                                                              \
    std::unique_ptr<cudf::column> doReduce(                                   \
        cudf::table_view const& input,                                        \
        TypePtr const& outputType,                                            \
        rmm::cuda_stream_view stream) override {                              \
      auto const aggRequest =                                                 \
          cudf::make_##name##_aggregation<cudf::reduce_aggregation>();        \
      auto const cudfOutputType =                                             \
          cudf::data_type(cudf_velox::veloxToCudfTypeId(outputType));         \
      auto const resultScalar = cudf::reduce(                                 \
          input.column(inputIndex), *aggRequest, cudfOutputType, stream);     \
      return cudf::make_column_from_scalar(*resultScalar, 1, stream);         \
    }                                                                         \
                                                                              \
   private:                                                                   \
    uint32_t output_idx;                                                      \
  };

DEFINE_SIMPLE_AGGREGATOR(Sum, sum, SUM)
DEFINE_SIMPLE_AGGREGATOR(Min, min, MIN)
DEFINE_SIMPLE_AGGREGATOR(Max, max, MAX)

struct CountAggregator : cudf_velox::CudfHashAggregation::Aggregator {
  CountAggregator(
      core::AggregationNode::Step step,
      uint32_t inputIndex,
      VectorPtr constant,
      bool isGlobal)
      : Aggregator(
            step,
            cudf::aggregation::COUNT_VALID,
            inputIndex,
            constant,
            isGlobal) {}

  void addGroupbyRequest(
      cudf::table_view const& tbl,
      std::vector<cudf::groupby::aggregation_request>& requests) override {
    auto& request = requests.emplace_back();
    outputIdx_ = requests.size() - 1;
    request.values = tbl.column(constant == nullptr ? inputIndex : 0);
    std::unique_ptr<cudf::groupby_aggregation> aggRequest =
        exec::isRawInput(step)
        ? cudf::make_count_aggregation<cudf::groupby_aggregation>(
              constant == nullptr ? cudf::null_policy::EXCLUDE
                                  : cudf::null_policy::INCLUDE)
        : cudf::make_sum_aggregation<cudf::groupby_aggregation>();
    request.aggregations.push_back(std::move(aggRequest));
  }

  std::unique_ptr<cudf::column> doReduce(
      cudf::table_view const& input,
      TypePtr const& outputType,
      rmm::cuda_stream_view stream) override {
    if (exec::isRawInput(step)) {
      // For raw input, implement count using size + null count
      auto inputCol = input.column(constant == nullptr ? inputIndex : 0);

      // count_valid: size - null_count, count_all: just the size
      int64_t count = constant == nullptr
          ? inputCol.size() - inputCol.null_count()
          : inputCol.size();

      auto resultScalar = cudf::numeric_scalar<int64_t>(count);

      return cudf::make_column_from_scalar(resultScalar, 1, stream);
    } else {
      // For non-raw input (intermediate/final), use sum aggregation
      auto const aggRequest =
          cudf::make_sum_aggregation<cudf::reduce_aggregation>();
      auto const cudfOutputType = cudf::data_type(cudf::type_id::INT64);
      auto const resultScalar = cudf::reduce(
          input.column(inputIndex), *aggRequest, cudfOutputType, stream);
      return cudf::make_column_from_scalar(*resultScalar, 1, stream);
    }
    return nullptr;
  }

  std::unique_ptr<cudf::column> makeOutputColumn(
      std::vector<cudf::groupby::aggregation_result>& results,
      rmm::cuda_stream_view stream) override {
    // cudf produces int32 for count(0) but velox expects int64
    auto col = std::move(results[outputIdx_].results[0]);
    if (col->type() == cudf::data_type(cudf::type_id::INT32)) {
      col = cudf::cast(*col, cudf::data_type(cudf::type_id::INT64), stream);
    }
    return col;
  }

 private:
  uint32_t outputIdx_;
};

struct MeanAggregator : cudf_velox::CudfHashAggregation::Aggregator {
  MeanAggregator(
      core::AggregationNode::Step step,
      uint32_t inputIndex,
      VectorPtr constant,
      bool isGlobal)
      : Aggregator(
            step,
            cudf::aggregation::MEAN,
            inputIndex,
            constant,
            isGlobal) {}

  void addGroupbyRequest(
      cudf::table_view const& tbl,
      std::vector<cudf::groupby::aggregation_request>& requests) override {
    switch (step) {
      case core::AggregationNode::Step::kSingle: {
        auto& request = requests.emplace_back();
        meanIdx_ = requests.size() - 1;
        request.values = tbl.column(inputIndex);
        request.aggregations.push_back(
            cudf::make_mean_aggregation<cudf::groupby_aggregation>());
        break;
      }
      case core::AggregationNode::Step::kPartial: {
        auto& request = requests.emplace_back();
        sumIdx_ = requests.size() - 1;
        request.values = tbl.column(inputIndex);
        request.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
        request.aggregations.push_back(
            cudf::make_count_aggregation<cudf::groupby_aggregation>(
                cudf::null_policy::EXCLUDE));
        break;
      }
      case core::AggregationNode::Step::kIntermediate:
      case core::AggregationNode::Step::kFinal: {
        // In intermediate and final aggregation, the previously computed sum
        // and count are in the child columns of the input column.
        auto& request = requests.emplace_back();
        sumIdx_ = requests.size() - 1;
        request.values = tbl.column(inputIndex).child(0);
        request.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());

        auto& request2 = requests.emplace_back();
        countIdx_ = requests.size() - 1;
        request2.values = tbl.column(inputIndex).child(1);
        // The counts are already computed in partial aggregation, so we just
        // need to sum them up again.
        request2.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
        break;
      }
      default:
        // We don't know how to handle kIntermediate step for mean
        VELOX_NYI("Unsupported aggregation step for mean");
    }
  }

  std::unique_ptr<cudf::column> makeOutputColumn(
      std::vector<cudf::groupby::aggregation_result>& results,
      rmm::cuda_stream_view stream) override {
    switch (step) {
      case core::AggregationNode::Step::kSingle:
        return std::move(results[meanIdx_].results[0]);
      case core::AggregationNode::Step::kPartial: {
        auto sum = std::move(results[sumIdx_].results[0]);
        auto count = std::move(results[sumIdx_].results[1]);

        auto const size = sum->size();

        auto countInt64 =
            cudf::cast(*count, cudf::data_type(cudf::type_id::INT64), stream);

        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(sum));
        children.push_back(std::move(countInt64));

        // TODO: Handle nulls. This can happen if all values are null in a
        // group.
        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT),
            size,
            rmm::device_buffer{},
            rmm::device_buffer{},
            0,
            std::move(children));
      }
      case core::AggregationNode::Step::kIntermediate: {
        // The difference between intermediate and partial is in where the
        // sum and count are coming from. In partial, since the input column is
        // the same, the sum and count are in the same agg result. In
        // intermediate, the input columns are different (it's the child
        // columns of the input column) and so the sum and count are in
        // different agg results.
        auto sum = std::move(results[sumIdx_].results[0]);
        auto count = std::move(results[countIdx_].results[0]);

        auto size = sum->size();

        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(sum));
        children.push_back(std::move(count));

        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT),
            size,
            rmm::device_buffer{},
            rmm::device_buffer{},
            0,
            std::move(children));
      }
      case core::AggregationNode::Step::kFinal: {
        auto sum = std::move(results[sumIdx_].results[0]);
        auto count = std::move(results[countIdx_].results[0]);
        auto avg = cudf::binary_operation(
            *sum,
            *count,
            cudf::binary_operator::DIV,
            // TODO: Change the output type to be dependent on the input type
            // like in the cudf groupby implementation.
            cudf::data_type(cudf::type_id::FLOAT64),
            stream);
        return avg;
      }
      default:
        VELOX_NYI("Unsupported aggregation step for mean");
    }
  }

  std::unique_ptr<cudf::column> doReduce(
      cudf::table_view const& input,
      TypePtr const& outputType,
      rmm::cuda_stream_view stream) override {
    switch (step) {
      case core::AggregationNode::Step::kSingle: {
        auto const aggRequest =
            cudf::make_mean_aggregation<cudf::reduce_aggregation>();
        auto const cudfOutputType =
            cudf::data_type(cudf_velox::veloxToCudfTypeId(outputType));
        auto const resultScalar = cudf::reduce(
            input.column(inputIndex), *aggRequest, cudfOutputType, stream);
        return cudf::make_column_from_scalar(*resultScalar, 1, stream);
      }
      case core::AggregationNode::Step::kPartial: {
        VELOX_CHECK(outputType->isRow());
        auto const& rowType = outputType->asRow();
        auto const sumType = rowType.childAt(0);
        auto const countType = rowType.childAt(1);
        auto const cudfSumType =
            cudf::data_type(cudf_velox::veloxToCudfTypeId(sumType));
        auto const cudfCountType =
            cudf::data_type(cudf_velox::veloxToCudfTypeId(countType));

        // sum
        auto const aggRequest =
            cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        auto const sumResultScalar = cudf::reduce(
            input.column(inputIndex), *aggRequest, cudfSumType, stream);
        auto sumCol =
            cudf::make_column_from_scalar(*sumResultScalar, 1, stream);

        // libcudf doesn't have a count agg for reduce. What we want is to
        // count the number of valid rows.
        auto countCol = cudf::make_column_from_scalar(
            cudf::numeric_scalar<int64_t>(
                input.column(inputIndex).size() -
                input.column(inputIndex).null_count()),
            1,
            stream);

        // Assemble into struct as expected by velox.
        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(sumCol));
        children.push_back(std::move(countCol));
        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT),
            1,
            rmm::device_buffer{},
            rmm::device_buffer{},
            0,
            std::move(children));
      }
      case core::AggregationNode::Step::kFinal: {
        // Input column has two children: sum and count
        auto const sumCol = input.column(inputIndex).child(0);
        auto const countCol = input.column(inputIndex).child(1);

        // sum the sums
        auto const sumAggRequest =
            cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        auto const sumResultScalar =
            cudf::reduce(sumCol, *sumAggRequest, sumCol.type(), stream);
        auto sumResultCol =
            cudf::make_column_from_scalar(*sumResultScalar, 1, stream);

        // sum the counts
        auto const countAggRequest =
            cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        auto const countResultScalar =
            cudf::reduce(countCol, *countAggRequest, countCol.type(), stream);

        // divide the sums by the counts
        auto const cudfOutputType =
            cudf::data_type(cudf_velox::veloxToCudfTypeId(outputType));
        return cudf::binary_operation(
            *sumResultCol,
            *countResultScalar,
            cudf::binary_operator::DIV,
            cudfOutputType,
            stream);
      }
      default:
        VELOX_NYI("Unsupported aggregation step for mean");
    }
  }

 private:
  // These indices are used to track where the desired result columns
  // (mean/<sum, count>) are in the output of cudf::groupby::aggregate().
  uint32_t meanIdx_;
  uint32_t sumIdx_;
  uint32_t countIdx_;
};

std::unique_ptr<cudf_velox::CudfHashAggregation::Aggregator> createAggregator(
    core::AggregationNode::Step step,
    std::string const& kind,
    uint32_t inputIndex,
    VectorPtr constant,
    bool isGlobal) {
  if (kind.rfind("sum", 0) == 0) {
    return std::make_unique<SumAggregator>(
        step, inputIndex, constant, isGlobal);
  } else if (kind.rfind("count", 0) == 0) {
    return std::make_unique<CountAggregator>(
        step, inputIndex, constant, isGlobal);
  } else if (kind.rfind("min", 0) == 0) {
    return std::make_unique<MinAggregator>(
        step, inputIndex, constant, isGlobal);
  } else if (kind.rfind("max", 0) == 0) {
    return std::make_unique<MaxAggregator>(
        step, inputIndex, constant, isGlobal);
  } else if (kind.rfind("avg", 0) == 0) {
    return std::make_unique<MeanAggregator>(
        step, inputIndex, constant, isGlobal);
  } else {
    VELOX_NYI("Aggregation not yet supported");
  }
}

static const std::unordered_map<std::string, core::AggregationNode::Step>
    companionStep = {
        {"_partial", core::AggregationNode::Step::kPartial},
        {"_merge", core::AggregationNode::Step::kIntermediate},
        {"_merge_extract", core::AggregationNode::Step::kFinal}};

/// \brief Convert companion function to step for the aggregation function
///
/// Companion functions are functions that are registered in velox along with
/// their main aggregation functions. These are designed to always function
/// with a fixed `step`. This is to allow spark style planNodes where `step` is
/// the property of the aggregation function rather than the planNode.
/// Companion functions allow us to override the planNode's step and use
/// aggregations of different steps in the same planNode
core::AggregationNode::Step getCompanionStep(
    std::string const& kind,
    core::AggregationNode::Step step) {
  for (const auto& [k, v] : companionStep) {
    if (folly::StringPiece(kind).endsWith(k)) {
      step = v;
      break;
    }
  }
  return step;
}

bool hasFinalAggs(
    std::vector<core::AggregationNode::Aggregate> const& aggregates) {
  return std::any_of(aggregates.begin(), aggregates.end(), [](auto const& agg) {
    return folly::StringPiece(agg.call->name()).endsWith("_merge_extract");
  });
}

auto toAggregators(
    core::AggregationNode const& aggregationNode,
    exec::OperatorCtx const& operatorCtx) {
  auto const step = aggregationNode.step();
  bool const isGlobal = aggregationNode.groupingKeys().empty();
  auto const& inputRowSchema = aggregationNode.sources()[0]->outputType();

  std::vector<std::unique_ptr<cudf_velox::CudfHashAggregation::Aggregator>>
      aggregators;
  for (auto const& aggregate : aggregationNode.aggregates()) {
    std::vector<column_index_t> aggInputs;
    std::vector<VectorPtr> aggConstants;
    for (auto const& arg : aggregate.call->inputs()) {
      if (auto const field =
              dynamic_cast<core::FieldAccessTypedExpr const*>(arg.get())) {
        aggInputs.push_back(inputRowSchema->getChildIdx(field->name()));
      } else if (
          auto constant =
              dynamic_cast<const core::ConstantTypedExpr*>(arg.get())) {
        aggInputs.push_back(kConstantChannel);
        aggConstants.push_back(constant->toConstantVector(operatorCtx.pool()));
      } else {
        VELOX_NYI("Constants and lambdas not yet supported");
      }
    }
    // The loop on aggregate.call->inputs() is taken from
    // AggregateInfo.cpp::toAggregateInfo(). It seems to suggest that there can
    // be multiple inputs to an aggregate.
    // We're postponing properly supporting this for now because the currently
    // supported aggregation functions in cudf_velox don't use it.
    VELOX_CHECK(aggInputs.size() == 1);

    if (aggregate.distinct) {
      VELOX_NYI("De-dup before aggregation is not yet supported");
    }

    auto const kind = aggregate.call->name();
    auto const inputIndex = aggInputs[0];
    auto const constant = aggConstants.empty() ? nullptr : aggConstants[0];
    auto const companionStep = getCompanionStep(kind, step);
    aggregators.push_back(
        createAggregator(companionStep, kind, inputIndex, constant, isGlobal));
  }
  return aggregators;
}

auto toIntermediateAggregators(
    core::AggregationNode const& aggregationNode,
    exec::OperatorCtx const& operatorCtx) {
  auto const step = core::AggregationNode::Step::kIntermediate;
  bool const isGlobal = aggregationNode.groupingKeys().empty();
  auto const& inputRowSchema = aggregationNode.outputType();

  std::vector<std::unique_ptr<cudf_velox::CudfHashAggregation::Aggregator>>
      aggregators;
  for (size_t i = 0; i < aggregationNode.aggregates().size(); i++) {
    // Intermediate aggregation has a 1:1 mapping between input and output.
    // We don't need to figure out input from the aggregate function.
    auto const& aggregate = aggregationNode.aggregates()[i];
    auto const inputIndex = aggregationNode.groupingKeys().size() + i;
    auto const kind = aggregate.call->name();
    auto const constant = nullptr;
    aggregators.push_back(
        createAggregator(step, kind, inputIndex, constant, isGlobal));
  }
  return aggregators;
}

} // namespace

namespace facebook::velox::cudf_velox {

CudfHashAggregation::CudfHashAggregation(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    std::shared_ptr<core::AggregationNode const> const& aggregationNode)
    : Operator(
          driverCtx,
          aggregationNode->outputType(),
          operatorId,
          aggregationNode->id(),
          aggregationNode->step() == core::AggregationNode::Step::kPartial
              ? "CudfPartialAggregation"
              : "CudfAggregation",
          aggregationNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      NvtxHelper(
          nvtx3::rgb{34, 139, 34}, // Forest Green
          operatorId,
          fmt::format("[{}]", aggregationNode->id())),
      aggregationNode_(aggregationNode),
      isPartialOutput_(
          exec::isPartialOutput(aggregationNode->step()) &&
          !hasFinalAggs(aggregationNode->aggregates())),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      isDistinct_(!isGlobal_ && aggregationNode->aggregates().empty()),
      maxPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage()) {}

void CudfHashAggregation::initialize() {
  Operator::initialize();

  auto const& inputType = aggregationNode_->sources()[0]->outputType();
  ignoreNullKeys_ = aggregationNode_->ignoreNullKeys();
  setupGroupingKeyChannelProjections(
      groupingKeyInputChannels_, groupingKeyOutputChannels_);

  auto const numGroupingKeys = groupingKeyOutputChannels_.size();

  // Velox CPU does optimizations related to pre-grouped keys. This can be
  // done in cudf by passing sort information to cudf::groupby() constructor.
  // We're postponing this for now.

  numAggregates_ = aggregationNode_->aggregates().size();
  aggregators_ = toAggregators(*aggregationNode_, *operatorCtx_);
  intermediateAggregators_ =
      toIntermediateAggregators(*aggregationNode_, *operatorCtx_);

  // Check that aggregate result type match the output type.
  // TODO: This is output schema validation. In velox CPU, it's done using
  // output types reported by aggregation functions. We can't do that in cudf
  // groupby.

  // TODO: Set identity projections used by HashProbe to pushdown dynamic
  // filters to table scan.

  // TODO: Add support for grouping sets and group ids.

  aggregationNode_.reset();
}

void CudfHashAggregation::setupGroupingKeyChannelProjections(
    std::vector<column_index_t>& groupingKeyInputChannels,
    std::vector<column_index_t>& groupingKeyOutputChannels) const {
  VELOX_CHECK(groupingKeyInputChannels.empty());
  VELOX_CHECK(groupingKeyOutputChannels.empty());

  auto const& inputType = aggregationNode_->sources()[0]->outputType();
  auto const& groupingKeys = aggregationNode_->groupingKeys();
  // The map from the grouping key output channel to the input channel.
  //
  // NOTE: grouping key output order is specified as 'groupingKeys' in
  // 'aggregationNode_'.
  std::vector<exec::IdentityProjection> groupingKeyProjections;
  groupingKeyProjections.reserve(groupingKeys.size());
  for (auto i = 0; i < groupingKeys.size(); ++i) {
    groupingKeyProjections.emplace_back(
        exec::exprToChannel(groupingKeys[i].get(), inputType), i);
  }

  groupingKeyInputChannels.reserve(groupingKeys.size());
  for (auto i = 0; i < groupingKeys.size(); ++i) {
    groupingKeyInputChannels.push_back(groupingKeyProjections[i].inputChannel);
  }

  groupingKeyOutputChannels.resize(groupingKeys.size());

  std::iota(
      groupingKeyOutputChannels.begin(), groupingKeyOutputChannels.end(), 0);
}

void CudfHashAggregation::computeIntermediateGroupbyPartial(CudfVectorPtr tbl) {
  // For every input, we'll do a groupby and compact results with the existing
  // intermediate groupby results.

  auto inputTableStream = tbl->stream();
  auto groupbyOnInput = doGroupByAggregation(
      tbl->release(),
      groupingKeyInputChannels_,
      aggregators_,
      inputTableStream);

  // If we already have partial output, concatenate the new results with it.
  if (partialOutput_) {
    // Create a vector of tables to concatenate
    std::vector<cudf::table_view> tablesToConcat;
    tablesToConcat.push_back(partialOutput_->getTableView());
    tablesToConcat.push_back(groupbyOnInput->getTableView());

    auto partialOutputStream = partialOutput_->stream();
    // We need to join the input table stream on the partial output stream to
    // make sure the intermediate results are available when we do the concat.
    cudf::detail::join_streams(
        std::vector<rmm::cuda_stream_view>{inputTableStream},
        partialOutputStream);

    // Concatenate the tables
    auto concatenatedTable =
        cudf::concatenate(tablesToConcat, partialOutputStream);

    // Now we have to groupby again but this time with intermediate aggregators.
    auto compactedOutput = doGroupByAggregation(
        std::move(concatenatedTable),
        groupingKeyOutputChannels_,
        intermediateAggregators_,
        partialOutputStream);
    partialOutput_ = compactedOutput;
  } else {
    // First time processing, just store the result of the input batch's groupby
    // This means we're storing the stream from the first batch.
    partialOutput_ = groupbyOnInput;
  }
}

void CudfHashAggregation::computeIntermediateDistinctPartial(
    CudfVectorPtr tbl) {
  // For every input, we'll concat with existing distinct results and then do a
  // distinct on the concatenated results.

  auto inputTableStream = tbl->stream();

  if (partialOutput_) {
    // Concatenate the input table with the existing distinct results.
    std::vector<cudf::table_view> tablesToConcat;
    tablesToConcat.push_back(partialOutput_->getTableView());
    tablesToConcat.push_back(tbl->getTableView().select(
        groupingKeyInputChannels_.begin(), groupingKeyInputChannels_.end()));

    auto partialOutputStream = partialOutput_->stream();
    // We need to join the input table stream on the partial output stream to
    // make sure the input table is available when we do the concat.
    cudf::detail::join_streams(
        std::vector<rmm::cuda_stream_view>{inputTableStream},
        partialOutputStream);

    auto concatenatedTable =
        cudf::concatenate(tablesToConcat, partialOutputStream);

    // Do a distinct on the concatenated results.
    auto distinctOutput = getDistinctKeys(
        std::move(concatenatedTable),
        groupingKeyOutputChannels_,
        inputTableStream);
    partialOutput_ = distinctOutput;
  } else {
    // First time processing, just store the result of the input batch's
    // distinct.
    partialOutput_ = getDistinctKeys(
        tbl->release(), groupingKeyInputChannels_, inputTableStream);
  }
}

void CudfHashAggregation::addInput(RowVectorPtr input) {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  if (input->size() == 0) {
    return;
  }
  numInputRows_ += input->size();

  auto cudfInput = std::dynamic_pointer_cast<cudf_velox::CudfVector>(input);
  VELOX_CHECK_NOT_NULL(cudfInput);

  if (isPartialOutput_ && !isGlobal_) {
    if (isDistinct_) {
      // Handle partial distinct aggregation.
      computeIntermediateDistinctPartial(cudfInput);
    } else {
      // Handle partial groupby aggregation.
      computeIntermediateGroupbyPartial(cudfInput);
    }
    return;
  }

  // Handle final aggregation or global cases.
  inputs_.push_back(std::move(cudfInput));
}

CudfVectorPtr CudfHashAggregation::doGroupByAggregation(
    std::unique_ptr<cudf::table> tbl,
    std::vector<column_index_t> const& groupByKeys,
    std::vector<std::unique_ptr<Aggregator>>& aggregators,
    rmm::cuda_stream_view stream) {
  auto groupbyKeyView = tbl->select(groupByKeys.begin(), groupByKeys.end());

  size_t const numGroupingKeys = groupbyKeyView.num_columns();

  // TODO: All other args to groupby are related to sort groupby. We don't
  // support optimizations related to it yet.
  cudf::groupby::groupby groupByOwner(
      groupbyKeyView,
      ignoreNullKeys_ ? cudf::null_policy::EXCLUDE
                      : cudf::null_policy::INCLUDE);

  std::vector<cudf::groupby::aggregation_request> requests;
  for (auto& aggregator : aggregators) {
    aggregator->addGroupbyRequest(tbl->view(), requests);
  }

  auto [groupKeys, results] = groupByOwner.aggregate(requests, stream);
  // flatten the results
  std::vector<std::unique_ptr<cudf::column>> resultColumns;

  // first fill the grouping keys
  auto groupKeysColumns = groupKeys->release();
  resultColumns.insert(
      resultColumns.begin(),
      std::make_move_iterator(groupKeysColumns.begin()),
      std::make_move_iterator(groupKeysColumns.end()));

  // then fill the aggregation results
  for (auto& aggregator : aggregators) {
    resultColumns.push_back(aggregator->makeOutputColumn(results, stream));
  }

  // make a cudf table out of columns
  auto resultTable = std::make_unique<cudf::table>(std::move(resultColumns));

  // velox expects nullptr instead of a table with 0 rows
  if (resultTable->num_rows() == 0) {
    return nullptr;
  }

  auto numRows = resultTable->num_rows();

  return std::make_shared<cudf_velox::CudfVector>(
      pool(), outputType_, numRows, std::move(resultTable), stream);
}

CudfVectorPtr CudfHashAggregation::doGlobalAggregation(
    std::unique_ptr<cudf::table> tbl,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::column>> resultColumns;
  resultColumns.reserve(aggregators_.size());
  for (auto i = 0; i < aggregators_.size(); i++) {
    resultColumns.push_back(aggregators_[i]->doReduce(
        tbl->view(), outputType_->childAt(i), stream));
  }

  return std::make_shared<cudf_velox::CudfVector>(
      pool(),
      outputType_,
      1,
      std::make_unique<cudf::table>(std::move(resultColumns)),
      stream);
}

CudfVectorPtr CudfHashAggregation::getDistinctKeys(
    std::unique_ptr<cudf::table> tbl,
    std::vector<column_index_t> const& groupByKeys,
    rmm::cuda_stream_view stream) {
  auto result = cudf::distinct(
      tbl->view().select(groupByKeys.begin(), groupByKeys.end()),
      {groupingKeyOutputChannels_.begin(), groupingKeyOutputChannels_.end()},
      cudf::duplicate_keep_option::KEEP_FIRST,
      cudf::null_equality::EQUAL,
      cudf::nan_equality::ALL_EQUAL,
      stream);

  auto numRows = result->num_rows();

  return std::make_shared<cudf_velox::CudfVector>(
      pool(), outputType_, numRows, std::move(result), stream);
}

CudfVectorPtr CudfHashAggregation::releaseAndResetPartialOutput() {
  VELOX_DCHECK(!isGlobal_);
  auto numOutputRows = partialOutput_->size();
  const double aggregationPct =
      numOutputRows == 0 ? 0 : (numOutputRows * 1.0) / numInputRows_ * 100;
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat("flushRowCount", RuntimeCounter(numOutputRows));
    lockedStats->addRuntimeStat("flushTimes", RuntimeCounter(1));
    lockedStats->addRuntimeStat(
        "partialAggregationPct", RuntimeCounter(aggregationPct));
  }

  numInputRows_ = 0;
  // We're moving partialOutput_ to the caller because we want it to be null
  // after this call.
  return std::move(partialOutput_);
}

RowVectorPtr CudfHashAggregation::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();

  // Handle partial groupby.
  if (isPartialOutput_ && !isGlobal_) {
    if (partialOutput_ &&
        partialOutput_->estimateFlatSize() >
            maxPartialAggregationMemoryUsage_) {
      // This is basically a flush of the partial output.
      return releaseAndResetPartialOutput();
    }
    if (not noMoreInput_) {
      // Don't produce output if the partial output hasn't reached memory limit
      // and there's more batches to come.
      return nullptr;
    }
    if (!partialOutput_ && finished_) {
      return nullptr;
    }
    return releaseAndResetPartialOutput();
  }

  if (finished_) {
    return nullptr;
  }

  if (!isPartialOutput_ && !noMoreInput_) {
    // Final aggregation has to wait for all batches to arrive so we cannot
    // return any results here.
    return nullptr;
  }

  if (inputs_.empty()) {
    return nullptr;
  }

  auto stream = cudfGlobalStreamPool().get_stream();
  auto tbl = getConcatenatedTable(inputs_, stream);

  // Release input data after synchronizing.
  stream.synchronize();
  inputs_.clear();

  if (noMoreInput_) {
    finished_ = true;
  }

  VELOX_CHECK_NOT_NULL(tbl);

  if (isDistinct_) {
    return getDistinctKeys(std::move(tbl), groupingKeyInputChannels_, stream);
  } else if (isGlobal_) {
    return doGlobalAggregation(std::move(tbl), stream);
  } else {
    return doGroupByAggregation(
        std::move(tbl), groupingKeyInputChannels_, aggregators_, stream);
  }
}

void CudfHashAggregation::noMoreInput() {
  Operator::noMoreInput();
  if (isPartialOutput_ && inputs_.empty()) {
    finished_ = true;
  }
}

bool CudfHashAggregation::isFinished() {
  return finished_;
}

} // namespace facebook::velox::cudf_velox
