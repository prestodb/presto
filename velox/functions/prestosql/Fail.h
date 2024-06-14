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
#pragma once

#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"
#include "velox/functions/prestosql/json/SIMDJsonWrapper.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions {

template <typename TExec>
struct FailFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<UnknownValue>& /*result*/, const arg_type<Varchar>& message) {
    if (threadSkipErrorDetails()) {
      return Status::UserError();
    }

    return Status::UserError("{}", message);
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<UnknownValue>& result,
      const arg_type<int32_t>& /*errorCode*/,
      const arg_type<Varchar>& message) {
    return call(result, message);
  }
};

template <typename TExec>
struct FailFromJsonFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<UnknownValue>& /*result*/, const arg_type<Json>& json) {
    if (threadSkipErrorDetails()) {
      return Status::UserError();
    }

    // Extract 'message' field from 'json'.
    std::string_view message;
    if (extractMessage(json, message) != simdjson::SUCCESS) {
      return Status::UserError(
          "Cannot extract 'message' from the JSON: {}", json);
    }

    return Status::UserError("{}", message);
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<UnknownValue>& result,
      const arg_type<int32_t>& /*errorCode*/,
      const arg_type<Json>& json) {
    return call(result, json);
  }

 private:
  static simdjson::error_code extractMessage(
      const StringView& json,
      std::string_view& message) {
    simdjson::padded_string paddedJson(json.data(), json.size());

    SIMDJSON_ASSIGN_OR_RAISE(auto jsonDoc, simdjsonParse(paddedJson));

    SIMDJSON_ASSIGN_OR_RAISE(message, jsonDoc["message"].get_string());

    return simdjson::SUCCESS;
  }
};

} // namespace facebook::velox::functions
