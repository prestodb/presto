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

namespace cpp2 facebook.velox.functions.remote
namespace java.swift com.facebook.spark.remotefunctionserver.api

cpp_include "folly/io/IOBuf.h"

include "thrift/annotation/cpp.thrift"

@cpp.Type{name = "folly::IOBuf"}
typedef binary IOBuf

/// The format used to serialize buffers/payloads.
enum PageFormat {
  PRESTO_PAGE = 1,
  SPARK_UNSAFE_ROW = 2,
}

/// Identifies the remote function being called.
struct RemoteFunctionHandle {
  /// The function name
  1: string name;

  /// The function return and argument types. The types are serialized using
  /// Velox's type serialization format.
  2: string returnType;
  3: list<string> argumentTypes;
}

/// A page of data to be sent to, or got as a return from a remote function
/// execution.
struct RemoteFunctionPage {
  /// The serialization format used to encode the payload.
  1: PageFormat pageFormat;

  /// The actual data.
  2: IOBuf payload;

  /// The number of logical rows in this page.
  3: i64 rowCount;
}

/// The parameters passed to the remote thrift call.
struct RemoteFunctionRequest {
  /// Function handle to identify te function being called.
  1: RemoteFunctionHandle remoteFunctionHandle;

  /// The page containing serialized input parameters.
  2: RemoteFunctionPage inputs;

  /// Whether the function is supposed to throw an exception if errors are
  /// found, or capture exceptions and return back to the user. This is used
  /// to implement special forms (if the function is inside a try() construct,
  /// for example).
  ///
  /// TODO: the format to return serialized exceptions back to the client needs
  /// to be defined and implemented.
  3: bool throwOnError;
}

/// Statistics that may be returned from server to client.
struct RemoteFunctionStats {
  1: map<string, string> stats;
}

/// Structured returned from server to client. Contains the serialized output
/// page and optional statistics.
struct RemoteFunctionResponse {
  1: RemoteFunctionPage result;
  2: optional RemoteFunctionStats remoteFunctionStats;
}

/// Service definition.
service RemoteFunctionService {
  RemoteFunctionResponse invokeFunction(1: RemoteFunctionRequest request);
}
