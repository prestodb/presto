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

#include "velox/functions/prestosql/json/SIMDJsonUtil.h"

#include "velox/common/base/VeloxException.h"

namespace facebook::velox {

void simdjsonErrorsToExceptions(
    std::exception_ptr exceptions[simdjson::NUM_ERROR_CODES]) {
  for (int i = 1; i < simdjson::NUM_ERROR_CODES; ++i) {
    simdjson::simdjson_error e(static_cast<simdjson::error_code>(i));
    exceptions[i] = toVeloxException(std::make_exception_ptr(e));
  }
}

simdjson::simdjson_result<simdjson::ondemand::document> simdjsonParse(
    const simdjson::padded_string_view& json) {
  thread_local simdjson::ondemand::parser parser;
  return parser.iterate(json);
}

} // namespace facebook::velox
