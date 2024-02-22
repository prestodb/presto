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

#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <random>

DEFINE_int32(seed, 0, "Random seed");

namespace facebook::velox {
namespace {

MapVectorPtr generateBase(
    memory::MemoryPool* pool,
    std::default_random_engine& gen) {
  constexpr int kSize = 1000;
  constexpr int kMapSize = 10;
  test::VectorMaker maker(pool);
  std::uniform_real_distribution valueDist;
  return maker.mapVector<int64_t, double>(
      kSize,
      [&](auto /*i*/) { return kMapSize; },
      [](auto /*i*/, auto j) { return j; },
      [&](auto /*i*/, auto /*j*/) { return valueDist(gen); });
}

MapVectorPtr generateUpdate(
    memory::MemoryPool* pool,
    std::default_random_engine& gen,
    const MapVector& base) {
  constexpr int kMapSize = 5;
  auto offsets = allocateIndices(base.size(), pool);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  rawOffsets[0] = 0;
  for (int i = 1; i < base.size(); ++i) {
    rawOffsets[i] = rawOffsets[i - 1] + kMapSize;
  }
  auto sizes = allocateIndices(base.size(), pool);
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  std::fill(rawSizes, rawSizes + base.size(), kMapSize);
  int64_t keyCandidates[10];
  std::iota(std::begin(keyCandidates), std::end(keyCandidates), 0);
  auto keys = BaseVector::create<FlatVector<int64_t>>(
      BIGINT(), kMapSize * base.size(), pool);
  for (int i = 0; i < base.size(); ++i) {
    std::shuffle(std::begin(keyCandidates), std::end(keyCandidates), gen);
    for (int j = 0; j < kMapSize; ++j) {
      keys->set(j + i * kMapSize, keyCandidates[j]);
    }
  }
  test::VectorMaker maker(pool);
  std::uniform_real_distribution valueDist;
  auto values = maker.flatVector<double>(
      kMapSize * base.size(), [&](auto /*i*/) { return valueDist(gen); });
  return std::make_shared<MapVector>(
      pool,
      base.type(),
      nullptr,
      base.size(),
      std::move(offsets),
      std::move(sizes),
      std::move(keys),
      std::move(values));
}

} // namespace
} // namespace facebook::velox

int main(int argc, char* argv[]) {
  using namespace facebook::velox;
  folly::Init follyInit(&argc, &argv);
  if (gflags::GetCommandLineFlagInfoOrDie("seed").is_default) {
    FLAGS_seed = std::random_device{}();
    LOG(INFO) << "Use generated random seed " << FLAGS_seed;
  }

  memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();
  std::default_random_engine gen(FLAGS_seed);
  auto base = generateBase(pool.get(), gen);
  auto update = generateUpdate(pool.get(), gen, *base);

  folly::addBenchmark(__FILE__, "int64 keys", [&] {
    auto result = base->update({update});
    folly::doNotOptimizeAway(result);
    return base->size();
  });
  folly::runBenchmarks();

  return 0;
}
