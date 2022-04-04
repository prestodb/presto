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
namespace facebook::presto::http {

const int kHttpOk = 200;
const int kHttpAccepted = 202;
const int kHttpNoContent = 204;
const int kHttpNotFound = 404;
const int kHttpInternalServerError = 500;

const char kMimeTypeApplicationJson[] = "application/json";
const char kMimeTypeApplicationThrift[] = "application/x-thrift+binary";
} // namespace facebook::presto::http
