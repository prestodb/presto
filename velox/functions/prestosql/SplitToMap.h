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

#include "folly/container/F14Set.h"
#include "velox/common/base/Status.h"
#include "velox/functions/Udf.h"

namespace facebook::velox::functions {

template <typename TExecCtx>
struct SplitToMapFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  Status call(
      out_type<Map<Varchar, Varchar>>& out,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& entryDelimiter,
      const arg_type<Varchar>& keyValueDelimiter) {
    return callImpl(
        out,
        toStringView(input),
        toStringView(entryDelimiter),
        toStringView(keyValueDelimiter),
        OnDuplicateKey::kFail);
  }

  Status call(
      out_type<Map<Varchar, Varchar>>& out,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& entryDelimiter,
      const arg_type<Varchar>& keyValueDelimiter,
      const arg_type<bool>& keepFirstOrLast) {
    return callImpl(
        out,
        toStringView(input),
        toStringView(entryDelimiter),
        toStringView(keyValueDelimiter),
        keepFirstOrLast ? OnDuplicateKey::kKeepFirst
                        : OnDuplicateKey::kKeepLast);
  }

 private:
  static std::string_view toStringView(const arg_type<Varchar>& input) {
    return std::string_view(input.data(), input.size());
  }

  enum class OnDuplicateKey {
    // Raise an error if there are duplicate keys.
    kFail,

    // Keep the first value for each key.
    kKeepFirst,

    // Keep the last value for each key.
    kKeepLast,
  };

  Status callImpl(
      out_type<Map<Varchar, Varchar>>& out,
      std::string_view input,
      std::string_view entryDelimiter,
      std::string_view keyValueDelimiter,
      OnDuplicateKey onDuplicateKey) const {
    VELOX_RETURN_IF(
        entryDelimiter.empty(), Status::UserError("entryDelimiter is empty"));
    VELOX_RETURN_IF(
        keyValueDelimiter.empty(),
        Status::UserError("keyValueDelimiter is empty"));
    VELOX_RETURN_IF(
        entryDelimiter == keyValueDelimiter,
        Status::UserError(
            "entryDelimiter and keyValueDelimiter must not be the same: {}",
            entryDelimiter));

    if (input.empty()) {
      return Status::OK();
    }

    size_t pos = 0;

    folly::F14FastMap<std::string_view, std::string_view> keyValuePairs;

    auto nextEntryPos = input.find(entryDelimiter, pos);
    while (nextEntryPos != std::string::npos) {
      VELOX_RETURN_NOT_OK(processEntry(
          std::string_view(input.data() + pos, nextEntryPos - pos),
          keyValueDelimiter,
          onDuplicateKey,
          keyValuePairs));

      pos = nextEntryPos + entryDelimiter.size();
      nextEntryPos = input.find(entryDelimiter, pos);
    }

    // Entry delimiter can be the last character in the input. In this case
    // there is no last entry to process.
    if (pos < input.size()) {
      VELOX_RETURN_NOT_OK(processEntry(
          std::string_view(input.data() + pos, input.size() - pos),
          keyValueDelimiter,
          onDuplicateKey,
          keyValuePairs));
    }

    out.reserve(keyValuePairs.size());
    for (auto& [key, value] : keyValuePairs) {
      auto [keyWriter, valueWriter] = out.add_item();
      keyWriter.setNoCopy(StringView(key));
      valueWriter.setNoCopy(StringView(value));
    }

    return Status::OK();
  }

  Status processEntry(
      std::string_view entry,
      std::string_view keyValueDelimiter,
      OnDuplicateKey onDuplicateKey,
      folly::F14FastMap<std::string_view, std::string_view>& keyValuePairs)
      const {
    const auto delimiterPos = entry.find(keyValueDelimiter, 0);

    VELOX_RETURN_IF(
        delimiterPos == std::string::npos,
        Status::UserError(
            "No delimiter found. Key-value delimiter must appear exactly once in each entry. Bad input: '{}'",
            entry));
    VELOX_RETURN_IF(
        entry.find(
            keyValueDelimiter, delimiterPos + keyValueDelimiter.size()) !=
            std::string::npos,
        Status::UserError(
            "More than one delimiter found. Key-value delimiter must appear exactly once in each entry. Bad input: '{}'",
            entry));

    const auto key = std::string_view(entry.data(), delimiterPos);
    const auto value = std::string_view(
        entry.data() + delimiterPos + keyValueDelimiter.size(),
        entry.size() - delimiterPos - keyValueDelimiter.size());

    switch (onDuplicateKey) {
      case OnDuplicateKey::kFail: {
        if (!keyValuePairs.emplace(key, value).second) {
          return Status::UserError("Duplicate keys ({}) are not allowed.", key);
        }
        break;
      }
      case OnDuplicateKey::kKeepFirst:
        keyValuePairs.try_emplace(key, value);
        break;
      case OnDuplicateKey::kKeepLast:
        keyValuePairs[key] = value;
        break;
    }

    return Status::OK();
  }
};

/// Analyzes split_to_map(string, entryDelim, keyDelim, lambda) call
/// to determine whether 'lambda' indicates keeping first or last value
/// for each key. If so, rewrites the call using $internal$split_to_map.
///
/// For example,
///
/// Rewrites
///     split_to_map(s, ed, kd, (k, v1, v2) -> v1
/// into
///     $internal$split_to_map(s, ed, kd, true) -- keep first
///
/// and
///     split_to_map(s, ed, kd, (k, v1, v2) -> v2
/// into
///     $internal$split_to_map(s, ed, kd, false) -- keep last
///
/// Returns new expression or nullptr if rewrite is not possible.
core::TypedExprPtr rewriteSplitToMapCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr);

} // namespace facebook::velox::functions
