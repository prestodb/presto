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

#include "velox/exec/tests/TableEvolutionFuzzer.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "velox/parse/TypeResolver.h"

DEFINE_uint32(seed, 0, "");
DEFINE_int32(duration_sec, 30, "");
DEFINE_int32(column_count, 5, "");
DEFINE_int32(evolution_count, 5, "");

namespace facebook::velox::exec::test {

namespace {

void registerFactories(folly::Executor* ioExecutor) {
  filesystems::registerLocalFileSystem();
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              TableEvolutionFuzzer::connectorId(),
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()),
              ioExecutor);
  connector::registerConnector(hiveConnector);
  dwio::common::registerFileSinks();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();
}

TEST(TableEvolutionFuzzerTest, run) {
  auto pool = memory::memoryManager()->addLeafPool("TableEvolutionFuzzer");
  exec::test::TableEvolutionFuzzer::Config config;
  config.pool = pool.get();
  config.columnCount = FLAGS_column_count;
  config.evolutionCount = FLAGS_evolution_count;
  config.formats = TableEvolutionFuzzer::parseFileFormats("dwrf");
  LOG(INFO) << "Running TableEvolutionFuzzer with seed " << FLAGS_seed;
  exec::test::TableEvolutionFuzzer fuzzer(config);
  fuzzer.setSeed(FLAGS_seed);
  const auto startTime = std::chrono::system_clock::now();
  const auto deadline = startTime + std::chrono::seconds(FLAGS_duration_sec);
  for (int i = 0; std::chrono::system_clock::now() < deadline; ++i) {
    LOG(INFO) << "Starting iteration " << i << ", seed=" << fuzzer.seed();
    fuzzer.run();
    fuzzer.reSeed();
  }
}

} // namespace

} // namespace facebook::velox::exec::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv);
  if (gflags::GetCommandLineFlagInfoOrDie("seed").is_default) {
    FLAGS_seed = std::random_device{}();
    LOG(INFO) << "Use generated random seed " << FLAGS_seed;
  }
  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});
  auto ioExecutor = folly::getGlobalIOExecutor();
  facebook::velox::exec::test::registerFactories(ioExecutor.get());
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::parse::registerTypeResolver();
  return RUN_ALL_TESTS();
}
