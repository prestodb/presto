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
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/VectorMaker.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::test;
using connector::hive::HivePartitionFunction;

namespace {

constexpr std::array<TypeKind, 10> kSupportedTypes{
    TypeKind::BOOLEAN,
    TypeKind::TINYINT,
    TypeKind::SMALLINT,
    TypeKind::INTEGER,
    TypeKind::BIGINT,
    TypeKind::REAL,
    TypeKind::DOUBLE,
    TypeKind::VARCHAR,
    TypeKind::TIMESTAMP,
    TypeKind::DATE};

class HivePartitionFunctionBenchmark
    : public functions::test::FunctionBenchmarkBase {
 public:
  explicit HivePartitionFunctionBenchmark(size_t vectorSize)
      : FunctionBenchmarkBase() {
    // Prepare input data
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize;
    opts.stringLength = 20;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);
    VectorMaker vm{pool_.get()};
    for (auto typeKind : kSupportedTypes) {
      auto flatVector = fuzzer.fuzzFlat(createScalarType(typeKind));
      rowVectors_[typeKind] = vm.rowVector({flatVector});
    }

    // Prepare HivePartitionFunction
    fewBucketsFunction_ = createHivePartitionFunction(20);
    manyBucketsFunction_ = createHivePartitionFunction(100);

    partitions_.resize(vectorSize);
  }

  template <TypeKind KIND>
  void runFew() {
    run<KIND>(fewBucketsFunction_.get());
  }

  template <TypeKind KIND>
  void runMany() {
    run<KIND>(manyBucketsFunction_.get());
  }

 private:
  std::unique_ptr<HivePartitionFunction> createHivePartitionFunction(
      size_t bucketCount) {
    std::vector<int> bucketToPartition(bucketCount);
    std::iota(bucketToPartition.begin(), bucketToPartition.end(), 0);
    std::vector<ChannelIndex> keyChannels;
    keyChannels.emplace_back(0);
    return std::make_unique<HivePartitionFunction>(
        bucketCount, bucketToPartition, keyChannels);
  }

  template <TypeKind KIND>
  void run(HivePartitionFunction* function) {
    if (rowVectors_.find(KIND) == rowVectors_.end()) {
      throw std::runtime_error(
          fmt::format("Unsupported type {}.", mapTypeKindToName(KIND)));
    }
    function->partition(*rowVectors_[KIND], partitions_);
  }

  std::unordered_map<TypeKind, RowVectorPtr> rowVectors_;
  std::unique_ptr<HivePartitionFunction> fewBucketsFunction_;
  std::unique_ptr<HivePartitionFunction> manyBucketsFunction_;
  std::vector<uint32_t> partitions_;
};

std::unique_ptr<HivePartitionFunctionBenchmark> benchmarkFew;
std::unique_ptr<HivePartitionFunctionBenchmark> benchmarkMany;

BENCHMARK(booleanFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::BOOLEAN>();
}

BENCHMARK_RELATIVE(booleanFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::BOOLEAN>();
}

BENCHMARK(booleanManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::BOOLEAN>();
}

BENCHMARK_RELATIVE(booleanManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::BOOLEAN>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(tinyintFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::TINYINT>();
}

BENCHMARK_RELATIVE(tinyintFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::TINYINT>();
}

BENCHMARK(tinyintManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::TINYINT>();
}

BENCHMARK_RELATIVE(tinyintManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::TINYINT>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(smallintFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::SMALLINT>();
}

BENCHMARK_RELATIVE(smallintFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::SMALLINT>();
}

BENCHMARK(smallintManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::SMALLINT>();
}

BENCHMARK_RELATIVE(smallintManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::SMALLINT>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(integerFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::INTEGER>();
}

BENCHMARK_RELATIVE(integerFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::INTEGER>();
}

BENCHMARK(integerManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::INTEGER>();
}

BENCHMARK_RELATIVE(integerManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::INTEGER>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(bigintFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::BIGINT>();
}

BENCHMARK_RELATIVE(bigintFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::BIGINT>();
}

BENCHMARK(bigintManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::BIGINT>();
}

BENCHMARK_RELATIVE(bigintManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::BIGINT>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(realFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::REAL>();
}

BENCHMARK_RELATIVE(realFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::REAL>();
}

BENCHMARK(realManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::REAL>();
}

BENCHMARK_RELATIVE(realManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::REAL>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(doubleFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::DOUBLE>();
}

BENCHMARK_RELATIVE(doubleFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::DOUBLE>();
}

BENCHMARK(doubleManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::DOUBLE>();
}

BENCHMARK_RELATIVE(doubleManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::DOUBLE>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(varcharFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::VARCHAR>();
}

BENCHMARK_RELATIVE(varcharFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::VARCHAR>();
}

BENCHMARK(varcharManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::VARCHAR>();
}

BENCHMARK_RELATIVE(varcharManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::VARCHAR>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(timestampFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::TIMESTAMP>();
}

BENCHMARK_RELATIVE(timestampFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::TIMESTAMP>();
}

BENCHMARK(timestampManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::TIMESTAMP>();
}

BENCHMARK_RELATIVE(timestampManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::TIMESTAMP>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(dateFewRowsFewBuckets) {
  benchmarkFew->runFew<TypeKind::DATE>();
}

BENCHMARK_RELATIVE(dateFewRowsManyBuckets) {
  benchmarkFew->runMany<TypeKind::DATE>();
}

BENCHMARK(dateManyRowsFewBuckets) {
  benchmarkMany->runFew<TypeKind::DATE>();
}

BENCHMARK_RELATIVE(dateManyRowsManyBuckets) {
  benchmarkMany->runMany<TypeKind::DATE>();
}

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmarkFew = std::make_unique<HivePartitionFunctionBenchmark>(1'000);
  benchmarkMany = std::make_unique<HivePartitionFunctionBenchmark>(10'000);

  folly::runBenchmarks();

  benchmarkFew.reset();
  benchmarkMany.reset();

  return 0;
}
