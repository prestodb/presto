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

namespace facebook::velox::functions {
template <typename TExec>
struct Re2RegexpSplit {
  Re2RegexpSplit() : cache_(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*string*/,
      const arg_type<Varchar>* /*pattern*/) {
    cache_.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  static constexpr int32_t reuse_strings_from_arg = 0;

  void call(
      out_type<Array<Varchar>>& out,
      const arg_type<Varchar>& string,
      const arg_type<Varchar>& pattern) {
    auto* re = cache_.findOrCompile(pattern);

    const auto re2String = re2::StringPiece(string.data(), string.size());

    size_t pos = 0;
    size_t lastEnd = 0;
    const char* start = string.data();

    re2::StringPiece subMatches[1];
    while (re->Match(
        re2String,
        pos,
        string.size(),
        RE2::Anchor::UNANCHORED,
        subMatches,
        1)) {
      const auto fullMatch = subMatches[0];
      const auto offset = fullMatch.data() - start;
      const auto size = fullMatch.size();

      out.add_item().setNoCopy(
          StringView(string.data() + lastEnd, offset - lastEnd));

      lastEnd = offset + size;
      if (UNLIKELY(size == 0)) {
        pos = lastEnd + 1;
      } else {
        pos = lastEnd;
      }
    }

    if (LIKELY(pos <= string.size())) {
      out.add_item().setNoCopy(
          StringView(string.data() + pos, string.size() - pos));
    } else {
      static const StringView kEmptyString(nullptr, 0);
      out.add_item().setNoCopy(kEmptyString);
    }
  }

 private:
  detail::ReCache cache_;
};
} // namespace facebook::velox::functions
