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

#include "velox/vector/fuzzer/Utils.h"

namespace facebook::velox {

bool coinToss(FuzzerGenerator& rng, double threshold) {
  static std::uniform_real_distribution<> dist(0.0, 1.0);
  return dist(rng) < threshold;
}

Timestamp randTimestamp(
    FuzzerGenerator& rng,
    FuzzerTimestampPrecision timestampPrecision) {
  // Generate timestamps only in the valid range to avoid datetime functions,
  // such as try_cast(varchar as timestamp), throwing VeloxRuntimeError in
  // fuzzers.
  constexpr int64_t min = -2'140'671'600;
  constexpr int64_t max = 2'140'671'600;
  constexpr int64_t microInSecond = 1'000'000;
  constexpr int64_t millisInSecond = 1'000;
  // DWRF requires nano to be in a certain range. Hardcode the value here to
  // avoid the dependency on DWRF.
  constexpr int64_t MAX_NANOS = 1'000'000'000;

  switch (timestampPrecision) {
    case FuzzerTimestampPrecision::kNanoSeconds:
      return Timestamp(
          rand<int64_t>(rng, min, max), (rand<int64_t>(rng) % MAX_NANOS));
    case FuzzerTimestampPrecision::kMicroSeconds:
      return Timestamp::fromMicros(
          rand<int64_t>(rng, min, max) * microInSecond +
          rand<int64_t>(rng, -microInSecond, microInSecond));
    case FuzzerTimestampPrecision::kMilliSeconds:
      return Timestamp::fromMillis(
          rand<int64_t>(rng, min, max) * millisInSecond +
          rand<int64_t>(rng, -millisInSecond, millisInSecond));
    case FuzzerTimestampPrecision::kSeconds:
      return Timestamp(rand<int64_t>(rng, min, max), 0);
  }
  return {}; // no-op.
}

int32_t randDate(FuzzerGenerator& rng) {
  constexpr int64_t min = -24'450;
  constexpr int64_t max = 24'450;
  return rand<int32_t>(rng, min, max);
}

/// Unicode character ranges. Ensure the vector indexes match the UTF8CharList
/// enum values.
///
/// Source: https://jrgraphix.net/research/unicode_blocks.php
static const std::vector<std::vector<std::pair<char16_t, char16_t>>>
    kUTFChatSets{
        // UTF8CharList::ASCII
        {
            {33, 127}, // All ASCII printable chars.
        },
        // UTF8CharList::UNICODE_CASE_SENSITIVE
        {
            {u'\u0020', u'\u007F'}, // Basic Latin.
            {u'\u0400', u'\u04FF'}, // Cyrillic.
        },
        // UTF8CharList::EXTENDED_UNICODE
        {
            {u'\u03F0', u'\u03FF'}, // Greek.
            {u'\u0100', u'\u017F'}, // Latin Extended A.
            {u'\u0600', u'\u06FF'}, // Arabic.
            {u'\u0900', u'\u097F'}, // Devanagari.
            {u'\u0600', u'\u06FF'}, // Hebrew.
            {u'\u3040', u'\u309F'}, // Hiragana.
            {u'\u2000', u'\u206F'}, // Punctuation.
            {u'\u2070', u'\u209F'}, // Sub/Super Script.
            {u'\u20A0', u'\u20CF'}, // Currency.
        },
        // UTF8CharList::MATHEMATICAL_SYMBOLS
        {
            {u'\u2200', u'\u22FF'}, // Math Operators.
            {u'\u2150', u'\u218F'}, // Number Forms.
            {u'\u25A0', u'\u25FF'}, // Geometric Shapes.
            {u'\u27C0', u'\u27EF'}, // Math Symbols.
            {u'\u2A00', u'\u2AFF'}, // Supplemental.
        },
    };

FOLLY_ALWAYS_INLINE char16_t getRandomChar(
    FuzzerGenerator& rng,
    const std::vector<std::pair<char16_t, char16_t>>& charSet) {
  const auto& chars = charSet.size() == 1
      ? charSet.front()
      : charSet[rand<uint32_t>(rng) % charSet.size()];
  auto size = chars.second - chars.first;
  auto inc = (rand<uint32_t>(rng) % size);
  char16_t res = chars.first + inc;
  return res;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
std::string randString(
    FuzzerGenerator& rng,
    size_t length,
    const std::vector<UTF8CharList>& encodings,
    std::string& buf,
    std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t>& converter) {
  buf.clear();
  std::u16string wbuf;
  wbuf.resize(length);

  for (size_t i = 0; i < length; ++i) {
    // First choose a random encoding from the list of input acceptable
    // encodings.
    VELOX_CHECK_GE(encodings.size(), 1);
    const auto& encoding = (encodings.size() == 1)
        ? encodings.front()
        : encodings[rand<uint32_t>(rng) % encodings.size()];

    wbuf[i] = getRandomChar(rng, kUTFChatSets[encoding]);
  }
  buf.append(converter.to_bytes(wbuf));
  return buf;
}
#pragma GCC diagnostic pop

namespace generator_spec_utils {

vector_size_t getRandomIndex(FuzzerGenerator& rng, vector_size_t maxIndex) {
  std::uniform_int_distribution<vector_size_t> indexGenerator(
      0, maxIndex); // generates index in [0, maxIndex]
  return indexGenerator(rng);
}

BufferPtr generateNullsBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t vectorSize,
    double nullProbability) {
  NullsBuilder builder{vectorSize, pool};
  for (size_t i = 0; i < vectorSize; ++i) {
    if (coinToss(rng, nullProbability)) {
      builder.setNull(i);
    }
  }
  return builder.build();
}

BufferPtr generateIndicesBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t bufferSize,
    vector_size_t baseVectorSize) {
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(bufferSize, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  auto indicesGenerator =
      std::uniform_int_distribution<vector_size_t>(0, baseVectorSize - 1);

  for (size_t i = 0; i < bufferSize; ++i) {
    rawIndices[i] = indicesGenerator(rng);
  }
  return indices;
}

} // namespace generator_spec_utils

} // namespace facebook::velox
