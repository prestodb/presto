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
#include "velox/exec/tests/JoinFuzzer.h"
#include <boost/random/uniform_int_distribution.hpp>
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 5, "The number of generated vectors.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_bool(enable_spill, false, "Whether to test plans with spilling enabled");

namespace facebook::velox::exec::test {

namespace {

class JoinFuzzer {
 public:
  explicit JoinFuzzer(size_t initialSeed);

  void go();

 private:
  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
    opts.nullRatio = FLAGS_null_ratio;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  /// Randomly pick a join type to test.
  core::JoinType pickJoinType();

  void verify(core::JoinType joinType);

  /// Returns a list of up to 5 randomly generated join key types.
  std::vector<TypePtr> generateJoinKeyTypes();

  /// Returns randomly generated probe input with upto 3 additional payload
  /// columns.
  std::vector<RowVectorPtr> generateProbeInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes);

  /// Same as generateProbeInput() but copies over 10% of the input in the probe
  /// columns to ensure some matches during joining. Also generates an empty
  /// input with a 10% chance.
  std::vector<RowVectorPtr> generateBuildInput(
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys);

  void shuffleJoinKeys(
      std::vector<std::string>& probeKeys,
      std::vector<std::string>& buildKeys);

  RowVectorPtr execute(const core::PlanNodePtr& plan, bool injectSpill);

  std::optional<MaterializedRowMultiset> computeDuckDbResult(
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      const core::PlanNodePtr& plan);

  int32_t randInt(int32_t min, int32_t max) {
    return boost::random::uniform_int_distribution<int32_t>(min, max)(rng_);
  }

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> pool_{memory::getDefaultMemoryPool()};
  VectorFuzzer vectorFuzzer_;
};

JoinFuzzer::JoinFuzzer(size_t initialSeed)
    : vectorFuzzer_{getFuzzerOptions(), pool_.get()} {
  seed(initialSeed);
}

template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

core::JoinType JoinFuzzer::pickJoinType() {
  // Right joins are tested by flipping sides of the left joins.
  static std::vector<core::JoinType> kJoinTypes = {
      core::JoinType::kInner,
      core::JoinType::kLeft,
      core::JoinType::kFull,
      core::JoinType::kLeftSemiFilter,
      core::JoinType::kLeftSemiProject,
      core::JoinType::kAnti,
  };

  size_t idx = randInt(0, kJoinTypes.size() - 1);
  return kJoinTypes[idx];
}

std::vector<TypePtr> JoinFuzzer::generateJoinKeyTypes() {
  auto numKeys = randInt(1, 5);
  std::vector<TypePtr> types;
  types.reserve(numKeys);
  for (auto i = 0; i < numKeys; ++i) {
    // Pick random scalar type.
    types.push_back(vectorFuzzer_.randType(0 /*maxDepth*/));
  }
  return types;
}

std::vector<RowVectorPtr> JoinFuzzer::generateProbeInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes) {
  std::vector<std::string> names = keyNames;
  std::vector<TypePtr> types = keyTypes;

  // Add up to 3 payload columns.
  auto numPayload = randInt(0, 3);
  for (auto i = 0; i < numPayload; ++i) {
    names.push_back(fmt::format("tp{}", i + keyNames.size()));
    types.push_back(vectorFuzzer_.randType(2 /*maxDepth*/));
  }

  auto inputType = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> input;
  for (auto i = 0; i < FLAGS_num_batches; ++i) {
    input.push_back(vectorFuzzer_.fuzzInputRow(inputType));
  }
  return input;
}

std::vector<RowVectorPtr> JoinFuzzer::generateBuildInput(
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys) {
  std::vector<std::string> names = buildKeys;
  std::vector<TypePtr> types;
  for (const auto& key : probeKeys) {
    types.push_back(asRowType(probeInput[0]->type())->findChild(key));
  }

  // Add up to 3 payload columns.
  auto numPayload = randInt(0, 3);
  for (auto i = 0; i < numPayload; ++i) {
    names.push_back(fmt::format("bp{}", i + buildKeys.size()));
    types.push_back(vectorFuzzer_.randType(2 /*maxDepth*/));
  }

  auto rowType = ROW(std::move(names), std::move(types));

  // 1 in 10 times use empty build.
  // TODO Use non-empty build with no matches sometimes.
  if (vectorFuzzer_.coinToss(0.1)) {
    return {BaseVector::create<RowVector>(rowType, 0, pool_.get())};
  }

  // TODO Remove the assumption that probeKeys are the first columns in
  // probeInput.

  // To ensure there are some matches, sample with replacement 10% of probe join
  // keys and use these as build keys.
  // TODO Add a few random rows as well.
  std::vector<RowVectorPtr> input;
  for (const auto& probe : probeInput) {
    auto numRows = 1 + probe->size() / 10;
    auto build = BaseVector::create<RowVector>(rowType, numRows, probe->pool());

    // Pick probe side rows to copy.
    std::vector<vector_size_t> rowNumbers(numRows);
    for (auto i = 0; i < numRows; ++i) {
      rowNumbers[i] = randInt(0, probe->size() - 1);
    }

    SelectivityVector rows(numRows);
    for (auto i = 0; i < probeKeys.size(); ++i) {
      build->childAt(i)->resize(numRows);
      build->childAt(i)->copy(probe->childAt(i).get(), rows, rowNumbers.data());
    }

    for (auto i = 0; i < numPayload; ++i) {
      auto column = i + probeKeys.size();
      build->childAt(column) =
          vectorFuzzer_.fuzz(rowType->childAt(column), numRows);
    }

    input.push_back(build);
  }

  return input;
}

std::vector<RowVectorPtr> flatten(const std::vector<RowVectorPtr>& vectors) {
  std::vector<RowVectorPtr> flatVectors;
  for (const auto& vector : vectors) {
    auto flat = BaseVector::create<RowVector>(
        vector->type(), vector->size(), vector->pool());
    flat->copy(vector.get(), 0, 0, vector->size());
    flatVectors.push_back(flat);
  }

  return flatVectors;
}

RowVectorPtr JoinFuzzer::execute(
    const core::PlanNodePtr& plan,
    bool injectSpill) {
  LOG(INFO) << "Executing query plan: " << std::endl
            << plan->toString(true, true);

  AssertQueryBuilder builder(plan);
  std::shared_ptr<TempDirectoryPath> spillDirectory;
  if (injectSpill) {
    spillDirectory = exec::test::TempDirectoryPath::create();
    builder.config(core::QueryConfig::kSpillEnabled, "true")
        .config(core::QueryConfig::kAggregationSpillEnabled, "true")
        .config(core::QueryConfig::kTestingSpillPct, "100")
        .spillDirectory(spillDirectory->path);
  }

  auto result = builder.maxDrivers(2).copyResults(pool_.get());
  LOG(INFO) << "Results: " << result->toString();
  if (VLOG_IS_ON(1)) {
    VLOG(1) << std::endl << result->toString(0, result->size());
  }
  return result;
}

std::optional<core::JoinType> tryFlipJoinType(core::JoinType joinType) {
  switch (joinType) {
    case core::JoinType::kInner:
      return joinType;
    case core::JoinType::kLeft:
      return core::JoinType::kRight;
    case core::JoinType::kRight:
      return core::JoinType::kLeft;
    case core::JoinType::kFull:
      return joinType;
    case core::JoinType::kLeftSemiFilter:
      return core::JoinType::kRightSemiFilter;
    case core::JoinType::kLeftSemiProject:
      return core::JoinType::kRightSemiProject;
    case core::JoinType::kRightSemiFilter:
      return core::JoinType::kLeftSemiFilter;
    case core::JoinType::kRightSemiProject:
      return core::JoinType::kLeftSemiProject;
    default:
      return std::nullopt;
  }
}

core::PlanNodePtr tryFlipJoinSides(const core::HashJoinNode& joinNode) {
  auto flippedJoinType = tryFlipJoinType(joinNode.joinType());
  if (!flippedJoinType.has_value()) {
    return nullptr;
  }

  return std::make_shared<core::HashJoinNode>(
      joinNode.id(),
      flippedJoinType.value(),
      joinNode.isNullAware(),
      joinNode.rightKeys(),
      joinNode.leftKeys(),
      joinNode.filter(),
      joinNode.sources()[1],
      joinNode.sources()[0],
      joinNode.outputType());
}

bool containsTypeKind(const TypePtr& type, const TypeKind& search) {
  if (type->kind() == search) {
    return true;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (containsTypeKind(type->childAt(i), search)) {
      return true;
    }
  }

  return false;
}

std::optional<MaterializedRowMultiset> JoinFuzzer::computeDuckDbResult(
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const core::PlanNodePtr& plan) {
  // Skip queries that use Timestamp and Varbinary type.
  // DuckDB doesn't support nanosecond precision for timestamps.
  // TODO Investigate mismatches reported when comparing Varbinary.
  if (containsTypeKind(probeInput[0]->type(), TypeKind::TIMESTAMP) ||
      containsTypeKind(probeInput[0]->type(), TypeKind::VARBINARY)) {
    return std::nullopt;
  }

  if (containsTypeKind(buildInput[0]->type(), TypeKind::TIMESTAMP) ||
      containsTypeKind(buildInput[0]->type(), TypeKind::VARBINARY)) {
    return std::nullopt;
  }

  DuckDbQueryRunner queryRunner;
  queryRunner.createTable("t", probeInput);
  queryRunner.createTable("u", buildInput);

  auto joinNode = dynamic_cast<const core::HashJoinNode*>(plan.get());
  VELOX_CHECK_NOT_NULL(joinNode);

  auto joinKeysToSql = [](auto keys) {
    std::stringstream out;
    for (auto i = 0; i < keys.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << keys[i]->name();
    }
    return out.str();
  };

  auto equiClausesToSql = [](auto joinNode) {
    std::stringstream out;
    for (auto i = 0; i < joinNode->leftKeys().size(); ++i) {
      if (i > 0) {
        out << " AND ";
      }
      out << joinNode->leftKeys()[i]->name() << " = "
          << joinNode->rightKeys()[i]->name();
    }
    return out.str();
  };

  const auto& outputNames = plan->outputType()->names();

  std::stringstream sql;
  if (joinNode->isLeftSemiProjectJoin()) {
    sql << "SELECT "
        << folly::join(", ", outputNames.begin(), --outputNames.end());
  } else {
    sql << "SELECT " << folly::join(", ", outputNames);
  }

  switch (joinNode->joinType()) {
    case core::JoinType::kInner:
      sql << " FROM t INNER JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kLeft:
      sql << " FROM t LEFT JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kFull:
      sql << " FROM t FULL OUTER JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kLeftSemiFilter:
      if (joinNode->leftKeys().size() > 1) {
        return std::nullopt;
      }
      sql << " FROM t WHERE " << joinKeysToSql(joinNode->leftKeys())
          << " IN (SELECT " << joinKeysToSql(joinNode->rightKeys())
          << " FROM u)";
      break;
    case core::JoinType::kLeftSemiProject:
      if (joinNode->leftKeys().size() > 1) {
        return std::nullopt;
      }
      sql << ", " << joinKeysToSql(joinNode->leftKeys()) << " IN (SELECT "
          << joinKeysToSql(joinNode->rightKeys()) << " FROM u) FROM t";
      break;
    case core::JoinType::kAnti:
      if (joinNode->isNullAware()) {
        if (joinNode->leftKeys().size() > 1) {
          return std::nullopt;
        }
        sql << " FROM t WHERE " << joinKeysToSql(joinNode->leftKeys())
            << " NOT IN (SELECT " << joinKeysToSql(joinNode->rightKeys())
            << " FROM u)";
      } else {
        sql << " FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE "
            << equiClausesToSql(joinNode) << ")";
      }
      break;
    default:
      VELOX_UNREACHABLE();
  }

  return queryRunner.execute(sql.str(), plan->outputType());
}

std::vector<std::string> fieldNames(
    const std::vector<core::FieldAccessTypedExprPtr>& fields) {
  std::vector<std::string> names;
  names.reserve(fields.size());
  for (const auto& field : fields) {
    names.push_back(field->name());
  }
  return names;
}

core::PlanNodePtr makeDefaultPlan(
    core::JoinType joinType,
    bool nullAware,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const std::vector<std::string>& output) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .values(probeInput)
      .hashJoin(
          probeKeys,
          buildKeys,
          PlanBuilder(planNodeIdGenerator).values(buildInput).planNode(),
          "" /*filter*/,
          output,
          joinType,
          nullAware)
      .planNode();
}

std::vector<core::PlanNodePtr> makeSources(
    const std::vector<RowVectorPtr>& input,
    std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator) {
  auto numSources = std::min<size_t>(4, input.size());
  std::vector<std::vector<RowVectorPtr>> sourceInputs(numSources);
  for (auto i = 0; i < input.size(); ++i) {
    sourceInputs[i % numSources].push_back(input[i]);
  }

  std::vector<core::PlanNodePtr> sourceNodes;
  for (const auto& sourceInput : sourceInputs) {
    sourceNodes.push_back(
        PlanBuilder(planNodeIdGenerator).values(sourceInput).planNode());
  }

  return sourceNodes;
}

void makeAlternativePlans(
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    std::vector<core::PlanNodePtr>& plans) {
  auto joinNode = std::dynamic_pointer_cast<const core::HashJoinNode>(plan);
  VELOX_CHECK_NOT_NULL(joinNode);

  // Flip join sides.
  if (auto flippedPlan = tryFlipJoinSides(*joinNode)) {
    plans.push_back(flippedPlan);
  }

  // Parallelize probe and build sides.
  auto probeKeys = fieldNames(joinNode->leftKeys());
  auto buildKeys = fieldNames(joinNode->rightKeys());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plans.push_back(PlanBuilder(planNodeIdGenerator)
                      .localPartitionRoundRobin(
                          makeSources(probeInput, planNodeIdGenerator))
                      .hashJoin(
                          probeKeys,
                          buildKeys,
                          PlanBuilder(planNodeIdGenerator)
                              .localPartitionRoundRobin(
                                  makeSources(buildInput, planNodeIdGenerator))
                              .planNode(),
                          "" /*filter*/,
                          joinNode->outputType()->names(),
                          joinNode->joinType(),
                          joinNode->isNullAware())
                      .planNode());

  // Use OrderBy + MergeJoin (if join type is inner or left).
  if (joinNode->isInnerJoin() || joinNode->isLeftJoin()) {
    planNodeIdGenerator->reset();
    plans.push_back(PlanBuilder(planNodeIdGenerator)
                        .values(probeInput)
                        .orderBy(probeKeys, false)
                        .mergeJoin(
                            probeKeys,
                            buildKeys,
                            PlanBuilder(planNodeIdGenerator)
                                .values(buildInput)
                                .orderBy(buildKeys, false)
                                .planNode(),
                            "" /*filter*/,
                            asRowType(joinNode->outputType())->names(),
                            joinNode->joinType())
                        .planNode());
  }
}

std::vector<std::string> makeNames(const std::string& prefix, size_t n) {
  std::vector<std::string> names;
  names.reserve(n);
  for (auto i = 0; i < n; ++i) {
    names.push_back(fmt::format("{}{}", prefix, i));
  }
  return names;
}

void JoinFuzzer::shuffleJoinKeys(
    std::vector<std::string>& probeKeys,
    std::vector<std::string>& buildKeys) {
  auto numKeys = probeKeys.size();
  if (numKeys == 1) {
    return;
  }

  std::vector<column_index_t> columnIndices(numKeys);
  std::iota(columnIndices.begin(), columnIndices.end(), 0);
  std::shuffle(columnIndices.begin(), columnIndices.end(), rng_);

  auto copyProbeKeys = probeKeys;
  auto copyBuildKeys = buildKeys;

  for (auto i = 0; i < numKeys; ++i) {
    probeKeys[i] = copyProbeKeys[columnIndices[i]];
    buildKeys[i] = copyBuildKeys[columnIndices[i]];
  }
}

RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
  std::vector<std::string> names = a->names();
  std::vector<TypePtr> types = a->children();

  for (auto i = 0; i < b->size(); ++i) {
    names.push_back(b->nameOf(i));
    types.push_back(b->childAt(i));
  }

  return ROW(std::move(names), std::move(types));
}

void JoinFuzzer::verify(core::JoinType joinType) {
  // Pick number and types of join keys.
  std::vector<TypePtr> keyTypes = generateJoinKeyTypes();
  std::vector<std::string> probeKeys = makeNames("t", keyTypes.size());
  std::vector<std::string> buildKeys = makeNames("u", keyTypes.size());

  auto probeInput = generateProbeInput(probeKeys, keyTypes);
  auto buildInput = generateBuildInput(probeInput, probeKeys, buildKeys);

  // Flatten inputs.
  auto flatProbeInput = flatten(probeInput);
  auto flatBuildInput = flatten(buildInput);

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Probe input: " << probeInput[0]->toString();
    for (const auto& v : flatProbeInput) {
      VLOG(1) << std::endl << v->toString(0, v->size());
    }

    VLOG(1) << "Build input: " << buildInput[0]->toString();
    for (const auto& v : flatBuildInput) {
      VLOG(1) << std::endl << v->toString(0, v->size());
    }
  }

  auto output =
      (core::isLeftSemiProjectJoin(joinType) ||
       core::isLeftSemiFilterJoin(joinType) || core::isAntiJoin(joinType))
      ? asRowType(probeInput[0]->type())->names()
      : concat(
            asRowType(probeInput[0]->type()), asRowType(buildInput[0]->type()))
            ->names();

  // Shuffle output columns.
  std::shuffle(output.begin(), output.end(), rng_);

  // Remove some output columns.
  auto numOutput = randInt(1, output.size());
  output.resize(numOutput);

  if (core::isLeftSemiProjectJoin(joinType) ||
      core::isRightSemiProjectJoin(joinType)) {
    output.push_back("match");
  }

  shuffleJoinKeys(probeKeys, buildKeys);

  bool nullAware = vectorFuzzer_.coinToss(0.5);
  auto plan = makeDefaultPlan(
      joinType,
      nullAware,
      probeKeys,
      buildKeys,
      probeInput,
      buildInput,
      output);

  auto expected = execute(plan, false /*injectSpill*/);

  // Verify results against DuckDB.
  if (auto duckDbResult = computeDuckDbResult(probeInput, buildInput, plan)) {
    VELOX_CHECK(
        assertEqualResults(duckDbResult.value(), {expected}),
        "Velox and DuckDB results don't match");
  }

  std::vector<core::PlanNodePtr> altPlans;
  altPlans.push_back(makeDefaultPlan(
      joinType,
      nullAware,
      probeKeys,
      buildKeys,
      flatProbeInput,
      flatBuildInput,
      output));
  makeAlternativePlans(plan, probeInput, buildInput, altPlans);
  makeAlternativePlans(plan, flatProbeInput, flatBuildInput, altPlans);

  for (auto i = 0; i < altPlans.size(); ++i) {
    LOG(INFO) << "Testing plan #" << i;
    auto actual = execute(altPlans[i], false /*injectSpill*/);
    VELOX_CHECK(
        assertEqualResults({expected}, {actual}),
        "Logically equivalent plans produced different results");

    if (FLAGS_enable_spill) {
      // Spilling for right semi project doesn't work yet.
      if (auto hashJoin = std::dynamic_pointer_cast<const core::HashJoinNode>(
              altPlans[i])) {
        if (hashJoin->isRightSemiProjectJoin()) {
          continue;
        }
      }

      LOG(INFO) << "Testing plan #" << i << " with spilling";
      actual = execute(altPlans[i], true /*injectSpill*/);
      VELOX_CHECK(
          assertEqualResults({expected}, {actual}),
          "Logically equivalent plans produced different results");
    }
  }
}

void JoinFuzzer::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.")

  auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";

    // Pick join type.
    auto joinType = pickJoinType();

    verify(joinType);

    LOG(INFO) << "==============================> Done with iteration "
              << iteration;

    reSeed();
    ++iteration;
  }
}

} // namespace

void joinFuzzer(size_t seed) {
  JoinFuzzer(seed).go();
}
} // namespace facebook::velox::exec::test
