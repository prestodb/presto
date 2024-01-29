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
#include <glog/logging.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/remote/server/RemoteFunctionService.h"

/// This file generates a binary only meant for testing. It instantiates a
/// remote function server able to serve all Presto functions, and is used
/// to ensure that the client instrumentation of remote function execution works
/// as expected.
///
/// It currently listens on a local unix domain socket controleed by the flag
/// below.

DEFINE_string(
    uds_path,
    "/tmp/remote.socket",
    "Unix domain socket used by the thrift server.");

DEFINE_string(
    function_prefix,
    "json.test_schema.",
    "Prefix to be added to the functions being registered");

using namespace ::facebook::velox;
using ::apache::thrift::ThriftServer;

int main(int argc, char* argv[]) {
  folly::Init init{&argc, &argv, false};
  FLAGS_logtostderr = true;

  // Always registers all Presto functions and make them available under a
  // certain prefix/namespace.
  LOG(INFO) << "Registering Presto functions";
  functions::prestosql::registerAllScalarFunctions(FLAGS_function_prefix);

  folly::SocketAddress location{
      folly::SocketAddress::makeFromPath(FLAGS_uds_path)};

  LOG(INFO) << "Initializing thrift server";
  auto handler = std::make_shared<functions::RemoteFunctionServiceHandler>();
  auto server = std::make_shared<ThriftServer>();
  server->setInterface(handler);
  server->setAddress(location);
  server->serve();

  LOG(INFO) << "Shutting down.";
  return 0;
}
