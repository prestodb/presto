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

#include <folly/container/F14Map.h>
#include <libstemmer.h> // @manual

#include "velox/functions/Udf.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {

namespace detail {
// Wrap the sbstemmer library and use its sb_stemmer_stem
// to get word stem.
class Stemmer {
 public:
  Stemmer(sb_stemmer* stemmer) : sbStemmer_(stemmer) {
    VELOX_CHECK_NOT_NULL(stemmer);
  }

  ~Stemmer() {
    sb_stemmer_delete(sbStemmer_);
  }

  // Get the word stem or NULL if out of memory.
  const char* stem(const std::string& input) {
    return (const char*)(sb_stemmer_stem(
        sbStemmer_,
        reinterpret_cast<unsigned char const*>(input.c_str()),
        input.length()));
  }

 private:
  sb_stemmer* sbStemmer_;
};
} // namespace detail

/// word_stem function
/// word_stem(word) -> varchar
///     return the stem of the word in the English language
/// word_stem(word, lang) -> varchar
///     return the stem of the word in the specificed language
///
/// Use the snowball stemmer library to calculate the stem.
/// https://snowballstem.org
/// The website provides Java implementation which is used in Presto as well
/// as C implementation. Therefore, both Presto and Prestimissio
/// would have the same word stem results.
template <typename TExec>
struct WordStemFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    return doCall<false>(result, input);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    return doCall<true>(result, input);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& lang) {
    return doCall<false>(result, input, lang);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& lang) {
    return doCall<true>(result, input, lang);
  }

  template <bool isAscii>
  FOLLY_ALWAYS_INLINE void doCall(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const std::string& lang = "en") {
    auto* stemmer = getStemmer(lang);
    VELOX_USER_CHECK_NOT_NULL(
        stemmer, "Unsupported stemmer language: {}", lang);

    std::string lowerOutput;
    stringImpl::lower<isAscii>(lowerOutput, input);
    auto* stem = stemmer->stem(lowerOutput);
    VELOX_CHECK_NOT_NULL(
        stem, "Stemmer library returned a NULL (out-of-memory)")
    result = stem;
  }

 private:
  folly::F14FastMap<std::string, std::unique_ptr<detail::Stemmer>> stemmers_;

  // Get a detail::Stemmer from the the map using the lang as the key or create
  // a new one if it doesn't exist. Return nullptr if the specified lang is not
  // supported.
  detail::Stemmer* getStemmer(const std::string& lang) {
    if (auto found = stemmers_.find(lang); found != stemmers_.end()) {
      return found->second.get();
    }
    // Only support ASCII and UTF-8.
    if (auto sbStemmer = sb_stemmer_new(lang.c_str(), "UTF_8")) {
      auto* stemmer = new detail::Stemmer(sbStemmer);
      stemmers_[lang] = std::unique_ptr<detail::Stemmer>(stemmer);
      return stemmer;
    }
    return nullptr;
  }
};
} // namespace facebook::velox::functions
