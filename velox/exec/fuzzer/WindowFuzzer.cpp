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

#include "velox/exec/fuzzer/WindowFuzzer.h"

#include <boost/random/uniform_int_distribution.hpp>
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/ScopedVarSetter.h"

DEFINE_bool(
    enable_window_reference_verification,
    false,
    "When true, the results of the window aggregation are compared to reference DB results");

namespace facebook::velox::exec::test {

namespace {

bool supportIgnoreNulls(const std::string& name) {
  // Below are all functions that support ignore nulls. Aggregation functions in
  // window operations do not support ignore nulls.
  // https://github.com/prestodb/presto/issues/21304.
  static std::unordered_set<std::string> supportedFunctions{
      "first_value",
      "last_value",
      "nth_value",
      "lead",
      "lag",
  };
  return supportedFunctions.count(name) > 0;
}

bool isKBoundFrame(const core::WindowNode::BoundType& boundType) {
  return (
      boundType == core::WindowNode::BoundType::kPreceding ||
      boundType == core::WindowNode::BoundType::kFollowing);
}

} // namespace

void WindowFuzzer::addWindowFunctionSignatures(
    const WindowFunctionMap& signatureMap) {
  for (const auto& [name, entry] : signatureMap) {
    ++functionsStats.numFunctions;
    bool hasSupportedSignature = false;
    for (auto& signature : entry.signatures) {
      hasSupportedSignature |= addSignature(name, signature);
    }
    if (hasSupportedSignature) {
      ++functionsStats.numSupportedFunctions;
    }
  }
}

std::string WindowFuzzer::generateKRowsFrameBound(
    std::vector<std::string>& argNames,
    std::vector<TypePtr>& argTypes,
    const std::string& colName) {
  // Generating column bounded frames 50% of the time and constant bounded the
  // rest of the times. The column bounded frames are of type INTEGER and only
  // have positive values as Window functions reject negative values for them.
  if (vectorFuzzer_.coinToss(0.5)) {
    argTypes.push_back(INTEGER());
    argNames.push_back(colName);
    return colName;
  }

  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
  constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
  // For frames with kPreceding, kFollowing bounds, pick a valid k, in the
  // range of 1 to 10, 70% of times. Test for random k values remaining times.
  int64_t minKValue, maxKValue;
  if (vectorFuzzer_.coinToss(0.7)) {
    minKValue = 1;
    maxKValue = 10;
  } else {
    minKValue = kMin;
    maxKValue = kMax;
  }
  const auto kValue = boost::random::uniform_int_distribution<int64_t>(
      minKValue, maxKValue)(rng_);
  return std::to_string(kValue);
}

WindowFuzzer::FrameMetadata WindowFuzzer::generateFrameClause(
    std::vector<std::string>& argNames,
    std::vector<TypePtr>& argTypes,
    const std::vector<std::string>& kBoundColumnNames) {
  FrameMetadata frameMetadata;
  // Randomly select if ROWS or RANGE frame
  frameMetadata.windowType = vectorFuzzer_.coinToss(0.5)
      ? core::WindowNode::WindowType::kRows
      : core::WindowNode::WindowType::kRange;
  const std::vector<core::WindowNode::BoundType> commonBoundTypes = {
      core::WindowNode::BoundType::kPreceding,
      core::WindowNode::BoundType::kCurrentRow,
      core::WindowNode::BoundType::kFollowing};
  std::vector<core::WindowNode::BoundType> startBoundOptions = {
      core::WindowNode::BoundType::kUnboundedPreceding};
  startBoundOptions.insert(
      startBoundOptions.end(),
      commonBoundTypes.begin(),
      commonBoundTypes.end());
  auto endBoundOptions = commonBoundTypes;
  endBoundOptions.emplace_back(
      core::WindowNode::BoundType::kUnboundedFollowing);

  // End bound option should not be greater than start bound option as this
  // would result in an invalid frame.
  auto startBoundIndex = boost::random::uniform_int_distribution<uint32_t>(
      0, startBoundOptions.size() - 1)(rng_);
  frameMetadata.startBoundType = startBoundOptions[startBoundIndex];
  auto endBoundMinIdx = std::max(0, static_cast<int>(startBoundIndex) - 1);
  auto endBoundIndex = boost::random::uniform_int_distribution<uint32_t>(
      endBoundMinIdx, endBoundOptions.size() - 1)(rng_);
  frameMetadata.endBoundType = endBoundOptions[endBoundIndex];

  // Generate the frame bounds for kRows frames. The frame bounds for kRange
  // frames can be generated only after we get the sorting keys, as the frame
  // bound value depends on the ORDER-BY column value.
  if (frameMetadata.windowType == core::WindowNode::WindowType::kRows) {
    if (isKBoundFrame(frameMetadata.startBoundType)) {
      frameMetadata.startBoundString =
          generateKRowsFrameBound(argNames, argTypes, kBoundColumnNames[0]);
    }
    if (isKBoundFrame(frameMetadata.endBoundType)) {
      frameMetadata.endBoundString =
          generateKRowsFrameBound(argNames, argTypes, kBoundColumnNames[1]);
    }
  }

  return frameMetadata;
}

std::string WindowFuzzer::frameClauseString(
    const FrameMetadata& frameMetadata,
    const std::vector<std::string>& kRangeOffsetColumns) {
  auto frameType = [&](const core::WindowNode::BoundType boundType,
                       bool isStartBound) -> std::string {
    const auto boundTypeString = core::WindowNode::boundTypeName(boundType);
    switch (boundType) {
      case core::WindowNode::BoundType::kUnboundedPreceding:
      case core::WindowNode::BoundType::kCurrentRow:
      case core::WindowNode::BoundType::kUnboundedFollowing:
        return boundTypeString;
      case core::WindowNode::BoundType::kPreceding:
      case core::WindowNode::BoundType::kFollowing: {
        std::string frameBound;
        if (kRangeOffsetColumns.empty()) {
          frameBound = isStartBound ? frameMetadata.startBoundString
                                    : frameMetadata.endBoundString;
        } else {
          frameBound =
              isStartBound ? kRangeOffsetColumns[0] : kRangeOffsetColumns[1];
        }
        return fmt::format("{} {}", frameBound, boundTypeString);
      }
      default:
        VELOX_UNREACHABLE("Unknown option for frame clause generation");
        return "";
    }
  };

  return fmt::format(
      " {} BETWEEN {} AND {}",
      core::WindowNode::windowTypeName(frameMetadata.windowType),
      frameType(frameMetadata.startBoundType, true),
      frameType(frameMetadata.endBoundType, false));
}

template <typename T>
const T WindowFuzzer::genOffsetAtIdx(
    const T& orderByValue,
    const T& frameBound,
    const core::WindowNode::BoundType& frameBoundType,
    const core::SortOrder& sortOrder) {
  constexpr bool isIntegerType =
      (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
       std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
       std::is_same_v<T, int128_t>);
  auto isPreceding = [&](core::WindowNode::BoundType boundType) {
    return boundType == core::WindowNode::BoundType::kPreceding;
  };

  if ((isPreceding(frameBoundType) && sortOrder.isAscending()) ||
      (!isPreceding(frameBoundType) && !sortOrder.isAscending())) {
    if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
      return orderByValue - frameBound;
    } else if constexpr (isIntegerType) {
      return checkedMinus<T>(orderByValue, frameBound);
    }
  }

  if ((!isPreceding(frameBoundType) && sortOrder.isAscending()) ||
      (isPreceding(frameBoundType) && !sortOrder.isAscending())) {
    if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
      return orderByValue + frameBound;
    } else if constexpr (isIntegerType) {
      return checkedPlus<T>(orderByValue, frameBound);
    }
  }

  VELOX_UNREACHABLE(
      "Offset cannot be generated: orderBy key type: {}, sortOrder ascending {}, frameBoundType {}",
      CppToType<T>::name,
      sortOrder.toString(),
      core::WindowNode::boundTypeName(frameBoundType));
  return T{};
}

std::string WindowFuzzer::generateOrderByClause(
    const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders) {
  std::stringstream frame;
  frame << " order by ";
  for (auto i = 0; i < sortingKeysAndOrders.size(); ++i) {
    if (i != 0) {
      frame << ", ";
    }
    frame << sortingKeysAndOrders[i].key_ << " "
          << sortingKeysAndOrders[i].sortOrder_.toString();
  }
  return frame.str();
}

std::string WindowFuzzer::getFrame(
    const std::vector<std::string>& partitionKeys,
    const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
    const std::string& frameClause) {
  std::stringstream frame;
  VELOX_CHECK(!partitionKeys.empty());
  frame << "partition by " << folly::join(", ", partitionKeys);
  if (!sortingKeysAndOrders.empty()) {
    frame << generateOrderByClause(sortingKeysAndOrders);
  }
  frame << frameClause;
  return frame.str();
}

std::vector<SortingKeyAndOrder> WindowFuzzer::generateSortingKeysAndOrders(
    const std::string& prefix,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types,
    bool isKRangeFrame) {
  auto keys = generateSortingKeys(prefix, names, types, isKRangeFrame);
  std::vector<SortingKeyAndOrder> results;
  for (auto i = 0; i < keys.size(); ++i) {
    auto asc = vectorFuzzer_.coinToss(0.5);
    auto nullsFirst = vectorFuzzer_.coinToss(0.5);
    results.emplace_back(keys[i], core::SortOrder(asc, nullsFirst));
  }
  return results;
}

template <typename T>
VectorPtr WindowFuzzer::buildKRangeColumn(
    const VectorPtr& frameBound,
    const VectorPtr& orderByCol,
    const core::WindowNode::BoundType& frameBoundType,
    const core::SortOrder& sortOrder,
    bool isColumnBound,
    bool isOffsetColumn) {
  auto type = CppToType<T>::create();
  const auto size = vectorFuzzer_.getOptions().vectorSize;
  const SelectivityVector allRows(size);
  DecodedVector decodedVector(*orderByCol, allRows);
  BufferPtr values = AlignedBuffer::allocate<T>(size, pool_.get());
  BufferPtr nulls = allocateNulls(size, pool_.get());
  auto rawValues = values->asMutableRange<T>();
  auto* rawNulls = nulls->asMutable<uint64_t>();

  for (auto j = 0; j < size; j++) {
    if (decodedVector.isNullAt(j)) {
      bits::setNull(rawNulls, j, true);
    } else {
      const auto orderByValueAtIdx = decodedVector.valueAt<T>(j);
      if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
        if (!std::isfinite(orderByValueAtIdx) ||
            std::isnan(orderByValueAtIdx)) {
          rawValues[j] = orderByValueAtIdx;
          continue;
        }
      }

      const T frameBoundValue = isColumnBound
          ? frameBound->as<FlatVector<T>>()->valueAt(j)
          : frameBound->as<ConstantVector<T>>()->valueAt(0);
      if (isOffsetColumn) {
        rawValues[j] = genOffsetAtIdx<T>(
            orderByValueAtIdx, frameBoundValue, frameBoundType, sortOrder);
      } else {
        rawValues[j] = frameBoundValue;
      }
    }
  }

  return std::make_shared<FlatVector<T>>(
      pool_.get(), type, nulls, size, values, std::vector<BufferPtr>{});
}

template <typename T>
std::string WindowFuzzer::addKRangeOffsetColumnToInputImpl(
    std::vector<RowVectorPtr>& input,
    const core::WindowNode::BoundType& frameBoundType,
    const SortingKeyAndOrder& orderByKey,
    const std::string& columnName,
    const std::string& offsetColumnName) {
  // Use columns as frame bound 50% of time.
  bool isColumnBound = vectorFuzzer_.coinToss(0.5);
  const auto type = CppToType<T>::create();
  VectorPtr constantFrameBound =
      isColumnBound ? nullptr : vectorFuzzer_.fuzzConstant(type, 1);
  VectorPtr columnFrameBound;
  // Generate frame bound (constant/column) without nulls.
  ScopedVarSetter nullRatioHolder(
      &vectorFuzzer_.getMutableOptions().nullRatio, 0.0);
  ScopedVarSetter dataSpecHolder(
      &vectorFuzzer_.getMutableOptions().dataSpec, {false, false});
  velox::test::VectorMaker vectorMaker{pool_.get()};

  for (auto i = 0; i < FLAGS_num_batches; i++) {
    const auto orderByCol = input[i]->childAt(orderByKey.key_);
    auto names = input[i]->type()->asRow().names();
    auto children = input[i]->children();
    if (isColumnBound) {
      VectorPtr fuzzFrameBound = vectorFuzzer_.fuzzFlat(type);
      names.push_back(columnName);
      columnFrameBound = buildKRangeColumn<T>(
          fuzzFrameBound,
          orderByCol,
          frameBoundType,
          orderByKey.sortOrder_,
          isColumnBound,
          false);
      children.push_back(columnFrameBound);
    }

    names.push_back(offsetColumnName);
    VectorPtr frameBound =
        isColumnBound ? columnFrameBound : constantFrameBound;
    const VectorPtr offsetColumn = buildKRangeColumn<T>(
        frameBound,
        orderByCol,
        frameBoundType,
        orderByKey.sortOrder_,
        isColumnBound,
        true);
    children.push_back(offsetColumn);
    input[i] = vectorMaker.rowVector(names, children);
  }

  return isColumnBound
      ? columnName
      : constantFrameBound->as<ConstantVector<T>>()->toString(0);
}

template <TypeKind TKind>
void WindowFuzzer::addKRangeOffsetColumnToInput(
    std::vector<RowVectorPtr>& input,
    FrameMetadata& frameMetadata,
    SortingKeyAndOrder& orderByKey,
    const std::vector<std::string>& columnNames,
    const std::vector<std::string>& offsetColumnNames) {
  using TCpp = typename TypeTraits<TKind>::NativeType;

  if (isKBoundFrame(frameMetadata.startBoundType)) {
    frameMetadata.startBoundString = addKRangeOffsetColumnToInputImpl<TCpp>(
        input,
        frameMetadata.startBoundType,
        orderByKey,
        columnNames[0],
        offsetColumnNames[0]);
  }
  if (isKBoundFrame(frameMetadata.endBoundType)) {
    frameMetadata.endBoundString = addKRangeOffsetColumnToInputImpl<TCpp>(
        input,
        frameMetadata.endBoundType,
        orderByKey,
        columnNames[1],
        offsetColumnNames[1]);
  }
}

void WindowFuzzer::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");

  auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  auto vectorOptions = vectorFuzzer_.getOptions();
  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";

    auto signatureWithStats = pickSignature();
    signatureWithStats.second.numRuns++;
    if (functionDataSpec_.count(signatureWithStats.first.name) > 0) {
      vectorOptions.dataSpec =
          functionDataSpec_.at(signatureWithStats.first.name);
    } else {
      vectorOptions.dataSpec = {true, true};
    }
    vectorFuzzer_.setOptions(vectorOptions);

    const auto signature = signatureWithStats.first;
    stats_.functionNames.insert(signature.name);

    const bool customVerification =
        customVerificationFunctions_.count(signature.name) != 0;
    std::shared_ptr<ResultVerifier> customVerifier = nullptr;
    if (customVerification) {
      customVerifier = customVerificationFunctions_.at(signature.name);
    }
    const bool requireSortedInput =
        orderDependentFunctions_.count(signature.name) != 0;

    std::vector<TypePtr> argTypes = signature.args;
    std::vector<std::string> argNames = makeNames(argTypes.size());

    const bool ignoreNulls =
        supportIgnoreNulls(signature.name) && vectorFuzzer_.coinToss(0.5);
    const auto call =
        makeFunctionCall(signature.name, argNames, false, false, ignoreNulls);

    // Columns used as k-PRECEDING/FOLLOWING frame bounds have fixed names: k0
    // and k1, for when a column is used as frame start and frame end bound
    // respectively.
    const std::vector<std::string> kBoundColumns = {"k0", "k1"};
    auto frameMetadata = generateFrameClause(argNames, argTypes, kBoundColumns);
    const auto windowType = frameMetadata.windowType;
    const auto startBoundType = frameMetadata.startBoundType;
    const auto endBoundType = frameMetadata.endBoundType;
    bool isKRangeFrame = windowType == core::WindowNode::WindowType::kRange &&
        (isKBoundFrame(startBoundType) || isKBoundFrame(endBoundType));

    auto useRowNumberKey =
        requireSortedInput || windowType == core::WindowNode::WindowType::kRows;

    const auto partitionKeys = generateSortingKeys("p", argNames, argTypes);

    std::vector<SortingKeyAndOrder> sortingKeysAndOrders;
    TypeKind orderByTypeKind;
    if (useRowNumberKey) {
      // If the function is order-dependent or uses "rows" frame, sort all
      // input rows by row_number additionally.
      sortingKeysAndOrders.emplace_back("row_number", core::kAscNullsLast);
      orderByTypeKind = TypeKind::INTEGER;
      ++stats_.numSortedInputs;
    } else if (isKRangeFrame) {
      // kRange frames need only one order by key. This would be row_number for
      // functions that are order dependent.
      sortingKeysAndOrders =
          generateSortingKeysAndOrders("s", argNames, argTypes, true);
      orderByTypeKind = argTypes.back()->kind();
    } else if (vectorFuzzer_.coinToss(0.5)) {
      // 50% chance without order-by clause.
      sortingKeysAndOrders =
          generateSortingKeysAndOrders("s", argNames, argTypes);
    }

    auto input = generateInputDataWithRowNumber(
        argNames, argTypes, partitionKeys, signature);
    // Offset column names used for k-RANGE frame bounds have fixed names: off0
    // and off1, representing the precomputed offset columns used as frame start
    // and frame end bound respectively.
    const std::vector<std::string> kRangeOffsetColumns = {"off0", "off1"};
    if (isKRangeFrame) {
      // Catch possible type overflow errors when generating offset columns.
      try {
        VELOX_USER_CHECK(
            sortingKeysAndOrders.size() == 1,
            "Window with k PRECEDING/FOLLOWING frame bounds should have a single ORDER-BY key");
        auto orderByKey = sortingKeysAndOrders[0];
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            addKRangeOffsetColumnToInput,
            orderByTypeKind,
            input,
            frameMetadata,
            orderByKey,
            kBoundColumns,
            kRangeOffsetColumns);
      } catch (VeloxUserError& e) {
        VLOG(2) << fmt::format(
            "This iteration is not valid due to exception from addKRangeOffsetColumnsToInput: {}",
            e.message());
        continue;
      } catch (VeloxRuntimeError& e) {
        throw e;
      }
    }

    logVectors(input);

    // For kRange frames with constant k, velox expects the frame bounds to be
    // columns containing precomputed offset values. Presto frame clause uses
    // constant k values.
    const auto prestoFrameClause = frameClauseString(frameMetadata);
    const auto frameClause = isKRangeFrame
        ? frameClauseString(frameMetadata, kRangeOffsetColumns)
        : prestoFrameClause;
    bool failed = verifyWindow(
        partitionKeys,
        sortingKeysAndOrders,
        frameClause,
        call,
        input,
        customVerification,
        customVerifier,
        FLAGS_enable_window_reference_verification,
        prestoFrameClause);
    if (failed) {
      signatureWithStats.second.numFailed++;
    }

    LOG(INFO) << "==============================> Done with iteration "
              << iteration;

    if (persistAndRunOnce_) {
      LOG(WARNING)
          << "Iteration succeeded with --persist_and_run_once flag enabled "
             "(expecting crash failure)";
      exit(0);
    }

    reSeed();
    ++iteration;
  }

  stats_.print(iteration);
  printSignatureStats();
}

void WindowFuzzer::go(const std::string& /*planPath*/) {
  // TODO: allow running window fuzzer with saved plans and splits.
  VELOX_NYI();
}

void WindowFuzzer::testAlternativePlans(
    const std::vector<std::string>& partitionKeys,
    const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
    const std::string& frame,
    const std::string& functionCall,
    const std::vector<RowVectorPtr>& input,
    bool customVerification,
    const std::shared_ptr<ResultVerifier>& customVerifier,
    const velox::fuzzer::ResultOrError& expected) {
  std::vector<AggregationFuzzerBase::PlanWithSplits> plans;

  std::vector<std::string> allKeys;
  for (const auto& key : partitionKeys) {
    allKeys.emplace_back(key + " NULLS FIRST");
  }
  for (const auto& keyAndOrder : sortingKeysAndOrders) {
    allKeys.emplace_back(fmt::format(
        "{} {}", keyAndOrder.key_, keyAndOrder.sortOrder_.toString()));
  }

  // Streaming window from values.
  if (!allKeys.empty()) {
    plans.push_back(
        {PlanBuilder()
             .values(input)
             .orderBy(allKeys, false)
             .streamingWindow(
                 {fmt::format("{} over ({})", functionCall, frame)})
             .planNode(),
         {}});
  }

  // With TableScan.
  auto directory = exec::test::TempDirectoryPath::create();
  const auto inputRowType = asRowType(input[0]->type());
  if (isTableScanSupported(inputRowType)) {
    auto splits = makeSplits(input, directory->getPath(), writerPool_);

// There is a known issue where LocalPartition will send DictionaryVectors
// with the same underlying base Vector to multiple threads.  This triggers
// TSAN to report data races, particularly if that base Vector is from the
// TableScan and reused.  Don't run these tests when TSAN is enabled to avoid
// the false negatives.
#ifndef TSAN_BUILD
    plans.push_back(
        {PlanBuilder()
             .tableScan(inputRowType)
             .localPartition(partitionKeys)
             .window({fmt::format("{} over ({})", functionCall, frame)})
             .planNode(),
         splits});
#endif

    if (!allKeys.empty()) {
      plans.push_back(
          {PlanBuilder()
               .tableScan(inputRowType)
               .orderBy(allKeys, false)
               .streamingWindow(
                   {fmt::format("{} over ({})", functionCall, frame)})
               .planNode(),
           splits});
    }
  }

  for (const auto& plan : plans) {
    testPlan(
        plan, false, false, customVerification, {customVerifier}, expected);
  }
}

namespace {
void initializeVerifier(
    const core::PlanNodePtr& plan,
    const std::shared_ptr<ResultVerifier>& customVerifier,
    const std::vector<RowVectorPtr>& input,
    const std::vector<std::string>& partitionKeys,
    const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
    const std::string& frame) {
  const auto& windowNode =
      std::dynamic_pointer_cast<const core::WindowNode>(plan);
  customVerifier->initializeWindow(
      input,
      partitionKeys,
      sortingKeysAndOrders,
      windowNode->windowFunctions()[0],
      frame,
      "w0");
}
} // namespace

bool WindowFuzzer::verifyWindow(
    const std::vector<std::string>& partitionKeys,
    const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
    const std::string& frameClause,
    const std::string& functionCall,
    const std::vector<RowVectorPtr>& input,
    bool customVerification,
    const std::shared_ptr<ResultVerifier>& customVerifier,
    bool enableWindowVerification,
    const std::string& prestoFrameClause) {
  SCOPE_EXIT {
    if (customVerifier) {
      customVerifier->reset();
    }
  };

  core::PlanNodeId windowNodeId;
  auto frame = getFrame(partitionKeys, sortingKeysAndOrders, frameClause);
  auto plan = PlanBuilder()
                  .values(input)
                  .window({fmt::format("{} over ({})", functionCall, frame)})
                  .capturePlanNodeId(windowNodeId)
                  .planNode();

  if (persistAndRunOnce_) {
    persistReproInfo({{plan, {}}}, reproPersistPath_);
  }

  velox::fuzzer::ResultOrError resultOrError;
  try {
    resultOrError = execute(plan);
    if (resultOrError.exceptionPtr) {
      ++stats_.numFailed;
    }

    if (!customVerification) {
      if (resultOrError.result && enableWindowVerification) {
        auto prestoQueryRunner =
            dynamic_cast<PrestoQueryRunner*>(referenceQueryRunner_.get());
        bool isPrestoQueryRunner = (prestoQueryRunner != nullptr);
        if (isPrestoQueryRunner) {
          prestoQueryRunner->queryRunnerContext()
              ->windowFrames_[windowNodeId]
              .push_back(prestoFrameClause);
        }
        auto referenceResult =
            computeReferenceResults(plan, input, referenceQueryRunner_.get());
        if (isPrestoQueryRunner) {
          prestoQueryRunner->queryRunnerContext()->windowFrames_.clear();
        }
        stats_.updateReferenceQueryStats(referenceResult.second);
        if (auto expectedResult = referenceResult.first) {
          ++stats_.numVerified;
          stats_.verifiedFunctionNames.insert(
              retrieveWindowFunctionName(plan)[0]);
          VELOX_CHECK(
              assertEqualResults(
                  expectedResult.value(),
                  plan->outputType(),
                  {resultOrError.result}),
              "Velox and reference DB results don't match");
          LOG(INFO) << "Verified results against reference DB";
        }
      }
    } else {
      LOG(INFO) << "Verification through custom verifier";
      ++stats_.numVerificationSkipped;

      if (customVerifier && resultOrError.result) {
        VELOX_CHECK(
            customVerifier->supportsVerify(),
            "Window fuzzer only uses custom verify() methods.");
        initializeVerifier(
            plan,
            customVerifier,
            input,
            partitionKeys,
            sortingKeysAndOrders,
            frame);
        customVerifier->verify(resultOrError.result);
      }
    }

    testAlternativePlans(
        partitionKeys,
        sortingKeysAndOrders,
        frame,
        functionCall,
        input,
        customVerification,
        customVerifier,
        resultOrError);

    return resultOrError.exceptionPtr != nullptr;
  } catch (...) {
    if (!reproPersistPath_.empty()) {
      persistReproInfo({{plan, {}}}, reproPersistPath_);
    }
    throw;
  }
}

void windowFuzzer(
    AggregateFunctionSignatureMap aggregationSignatureMap,
    WindowFunctionMap windowSignatureMap,
    size_t seed,
    const std::unordered_map<std::string, std::shared_ptr<ResultVerifier>>&
        customVerificationFunctions,
    const std::unordered_map<std::string, std::shared_ptr<InputGenerator>>&
        customInputGenerators,
    const std::unordered_set<std::string>& orderDependentFunctions,
    const std::unordered_map<std::string, DataSpec>& functionDataSpec,
    VectorFuzzer::Options::TimestampPrecision timestampPrecision,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    const std::unordered_map<std::string, std::string>& hiveConfigs,
    bool orderableGroupKeys,
    const std::optional<std::string>& planPath,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
  auto windowFuzzer = WindowFuzzer(
      std::move(aggregationSignatureMap),
      std::move(windowSignatureMap),
      seed,
      customVerificationFunctions,
      customInputGenerators,
      orderDependentFunctions,
      functionDataSpec,
      timestampPrecision,
      queryConfigs,
      hiveConfigs,
      orderableGroupKeys,
      std::move(referenceQueryRunner));
  planPath.has_value() ? windowFuzzer.go(planPath.value()) : windowFuzzer.go();
}

void WindowFuzzer::Stats::print(size_t numIterations) const {
  AggregationFuzzerBase::Stats::print(numIterations);
  LOG(INFO) << "Total functions verified in reference DB: "
            << verifiedFunctionNames.size();
}

} // namespace facebook::velox::exec::test
