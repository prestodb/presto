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

#include <folly/io/IOBuf.h>
#include <proxygen/httpserver/ResponseHandler.h>
#include <proxygen/lib/http/HTTPHeaders.h>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "velox/common/memory/MemoryPool.h"

namespace facebook::presto::expression {

/// Optimizes RowExpressions received in a http request and returns a http
/// response containing the result of expression optimization.
/// @param httpHeaders Headers from the http request, contains the timezone
/// from Presto coordinator and the expression optimizer level.
/// @param inputRowExpressions List of RowExpressions to be optimized.
/// @param downstream Returns the result of expression optimization as a http
/// response. If expression optimizer level is `EVALUATED` and the evaluation of
/// any expression from the input fails, the http response contains the error
/// message encountered during evaluation with a 500 response code. Otherwise,
/// the http response contains the list of optimized RowExpressions, serialized
/// as an array of JSON objects, with 200 response code.
/// @param driverExecutor Driver CPU executor.
/// @param pool Memory pool.
void optimizeExpressions(
    const proxygen::HTTPHeaders& httpHeaders,
    const nlohmann::json::array_t& inputRowExpressions,
    proxygen::ResponseHandler* downstream,
    folly::Executor* driverExecutor,
    velox::memory::MemoryPool* pool);
} // namespace facebook::presto::expression
