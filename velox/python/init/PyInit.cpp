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

#include "velox/python/init/PyInit.h"

#include <folly/debugging/symbolizer/SignalHandler.h>

#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::py {

folly::once_flag registerOnceFlag;

void registerAllResourcesOnce() {
  velox::filesystems::registerLocalFileSystem();

  // Register file readers and writers.
  velox::dwrf::registerDwrfWriterFactory();
  velox::dwrf::registerDwrfReaderFactory();

  velox::dwio::common::LocalFileSink::registerFactory();

  velox::parse::registerTypeResolver();

  velox::core::PlanNode::registerSerDe();
  velox::Type::registerSerDe();
  velox::common::Filter::registerSerDe();
  velox::connector::hive::HiveConnector::registerSerDe();
  velox::core::ITypedExpr::registerSerDe();

  // Register functions.
  // TODO: We should move this to a separate module so that clients could
  // register only when needed.
  velox::functions::prestosql::registerAllScalarFunctions();
  velox::aggregate::prestosql::registerAllAggregateFunctions();
}

void registerAllResources() {
  folly::call_once(registerOnceFlag, registerAllResourcesOnce);
}

void initializeVeloxMemory() {
  if (not velox::memory::MemoryManager::testInstance()) {
    // Enable full Velox stack trace when exceptions are thrown.
    FLAGS_velox_exception_user_stacktrace_enabled = true;
    velox::memory::initializeMemoryManager(
        velox::memory::MemoryManager::Options{});

    // Installs folly's signal handler to print a stack trace on crashes.
    folly::symbolizer::installFatalSignalHandler(
        folly::symbolizer::kAllFatalSignals);
  }
}

} // namespace facebook::velox::py
