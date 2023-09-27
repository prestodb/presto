/*
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
#pragma once

#include <folly/SocketAddress.h>

namespace facebook::presto {

/// Register remote functions from json files found at `path`.
///
/// If `path` is a file, open it and parse the json signature using
/// JsonSignatureParser. If `path` is a directory, recursively visit all
/// directories and read/register all json files in the directory tree.
///
/// `location` specifies where the remote function should forward connections
/// to.
///
/// `prefix`, if not empty, it is added to the names of functions registered
/// using '.' as a separator, e.g., 'prefix.functionName'.
///
/// Returns the number of signatures registered.
size_t registerRemoteFunctions(
    const std::string& inputPath,
    const folly::SocketAddress& location,
    const std::string& prefix = "");

} // namespace facebook::presto
