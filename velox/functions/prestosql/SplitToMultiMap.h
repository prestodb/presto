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

#include "velox/functions/Udf.h"

namespace facebook::velox::functions {

template <typename TExecCtx>
struct SplitToMultiMapFunction {
  using RestMapType =
      folly::F14FastMap<std::string_view, std::vector<std::string_view>>;
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  void call(
      out_type<Map<Varchar, Array<Varchar>>>& out,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& entryDelimiter,
      const arg_type<Varchar>& keyValueDelimiter) {
    VELOX_USER_CHECK(!entryDelimiter.empty(), "entryDelimiter is empty");
    VELOX_USER_CHECK(!keyValueDelimiter.empty(), "keyValueDelimiter is empty");
    VELOX_USER_CHECK_NE(
        entryDelimiter,
        keyValueDelimiter,
        "entryDelimiter and keyValueDelimiter must not be the same");

    if (input.empty()) {
      return;
    }

    callImpl(
        out,
        toStringView(input),
        toStringView(entryDelimiter),
        toStringView(keyValueDelimiter));
  }

  // Overloaded call function to handle UnknownValue cases
  template <typename TEntryDelimiter, typename TKeyValueDelimiter>
  void call(
      out_type<Map<Varchar, Array<Varchar>>>& out,
      const arg_type<Varchar>& input,
      const TEntryDelimiter& entryDelimiter,
      const TKeyValueDelimiter& keyValueDelimiter) {
    static_assert(
        std::is_same_v<TEntryDelimiter, arg_type<UnknownValue>> ||
        std::is_same_v<TKeyValueDelimiter, arg_type<UnknownValue>>);
    return;
  }

 private:
  static std::string_view toStringView(const arg_type<Varchar>& input) {
    return std::string_view(input.data(), input.size());
  }

  void callImpl(
      out_type<Map<Varchar, Array<Varchar>>>& out,
      std::string_view input,
      std::string_view entryDelimiter,
      std::string_view keyValueDelimiter) const {
    size_t pos = 0;

    RestMapType restMap;

    while (pos < input.size()) {
      const auto nextEntryPos = input.find(entryDelimiter, pos);

      // Process the current entry
      processEntry(
          restMap,
          // If no more delimiters are found, take the rest of the string as the
          // next entry
          nextEntryPos == std::string::npos
              ? std::string_view(input.data() + pos, input.size() - pos)
              : std::string_view(input.data() + pos, nextEntryPos - pos),
          keyValueDelimiter);

      if (nextEntryPos == std::string::npos) {
        break;
      }
      // Move to the next entry
      pos = nextEntryPos + entryDelimiter.size();
    }

    // Populate the output map
    out.reserve(restMap.size());
    for (const auto& [key, values] : restMap) {
      auto [keyWriter, valueWriter] = out.add_item();
      keyWriter.copy_from(key);
      valueWriter.copy_from(values);
    }
  }

  void processEntry(
      RestMapType& restMap,
      std::string_view entry,
      std::string_view keyValueDelimiter) const {
    const auto delimiterPos = entry.find(keyValueDelimiter);

    VELOX_USER_CHECK_NE(
        delimiterPos,
        std::string::npos,
        "Key-value delimiter must appear exactly once in each entry. Bad input: '{}'",
        entry);

    const auto key = std::string_view(entry.data(), delimiterPos);

    const auto value = std::string_view(
        entry.data() + delimiterPos + keyValueDelimiter.size(),
        entry.size() - delimiterPos - keyValueDelimiter.size());

    // Validate that the value does not contain the delimiter
    VELOX_USER_CHECK_EQ(
        value.find(keyValueDelimiter),
        std::string::npos,
        "Key-value delimiter must appear exactly once in each entry. Bad input: '{}'",
        entry);

    auto [it, _] = restMap.try_emplace(key);
    it->second.push_back(value);
  }
};

} // namespace facebook::velox::functions
