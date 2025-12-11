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

#include <cstdint>

namespace facebook::presto::http {

// HTTP status codes.
constexpr uint16_t kHttpOk = 200;
constexpr uint16_t kHttpAccepted = 202;
constexpr uint16_t kHttpNoContent = 204;
constexpr uint16_t kHttpMultipleChoices = 300;
constexpr uint16_t kHttpBadRequest = 400;
constexpr uint16_t kHttpUnauthorized = 401;
constexpr uint16_t kHttpNotFound = 404;
constexpr uint16_t kHttpUnprocessableContent = 422;
constexpr uint16_t kHttpInternalServerError = 500;

constexpr char kMimeTypeApplicationJson[] = "application/json";
constexpr char kMimeTypeApplicationThrift[] = "application/x-thrift+binary";
constexpr char kMimeTypeTextPlain[] = "text/plain";
constexpr char kShuttingDown[] = "\"SHUTTING_DOWN\"";
constexpr char kPrestoInternalBearer[] = "X-Presto-Internal-Bearer";

} // namespace facebook::presto::http
