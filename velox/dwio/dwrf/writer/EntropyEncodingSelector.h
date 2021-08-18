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

#include <folly/Random.h>

#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/dwrf/writer/StringDictionaryEncoder.h"

namespace facebook::velox::dwrf {

class EntropyEncodingSelector {
 public:
  explicit EntropyEncodingSelector(
      memory::MemoryPool& pool,
      const float dictionaryKeySizeThreshold,
      const float entropyKeySizeThreshold,
      const size_t entropyMinSamples,
      const float entropyDictSampleFraction,
      const size_t entropyThreshold)
      : pool_{pool},
        dictionaryKeySizeThreshold_{dictionaryKeySizeThreshold},
        entropyKeySizeThreshold_{entropyKeySizeThreshold},
        entropyMinSamples_{entropyMinSamples},
        entropyDictSampleFraction_{entropyDictSampleFraction},
        entropyThreshold_{entropyThreshold} {
    DWIO_ENSURE_GE(1.0f, dictionaryKeySizeThreshold_);
    DWIO_ENSURE_LE(0.0f, dictionaryKeySizeThreshold_);
    DWIO_ENSURE_GE(1.0f, entropyKeySizeThreshold_);
    DWIO_ENSURE_LE(0.0f, entropyKeySizeThreshold_);
    DWIO_ENSURE_GE(1.0f, entropyDictSampleFraction_);
    DWIO_ENSURE_LE(0.0f, entropyDictSampleFraction_);
  }

  // NOTE: some inclusiveness of inequality in this method is flipped to grant
  // more control. This is particularly useful for testing, but could cause
  // disparity with bbio in terms of raw file output. Need to keep this in mind
  // in integration testing.
  bool useDictionary(
      const StringDictionaryEncoder& dictEncoder,
      uint64_t valueCount) const {
    DWIO_ENSURE(valueCount, "No rows provided to encoding selector!");
    // The fraction of non-null values in this column that are repeats of values
    // in the dictionary
    float repeatedValuesFraction =
        static_cast<float>(valueCount - dictEncoder.size()) / valueCount;

    // dictionaryKeySizeThreshold is the fraction of keys that are distinct
    // beyond which dictionary encoding is turned off so 1 -
    // dictionaryKeySizeThreshold is the number of repeated values below which
    // dictionary encoding should be turned off
    if (repeatedValuesFraction < 1.0 - dictionaryKeySizeThreshold_) {
      return false;
    }

    // If the number of repeated values is small enough, consider using the
    // entropy heuristic If the number of repeated values is high, even in the
    // presence of low entropy, dictionary encoding can provide benefits beyond
    // just zlib
    return repeatedValuesFraction < entropyKeySizeThreshold_
        ? useDictionaryEncodingEntropyHeuristic(dictEncoder)
        : true;
  }

 private:
  bool useDictionaryEncodingEntropyHeuristic(
      const StringDictionaryEncoder& dictEncoder) const {
    std::unordered_set<char> charSet;
    // Perform sampling if required.
    if (dictEncoder.size() > entropyMinSamples_) {
      auto samples = getSampleIndicesForEntropy(dictEncoder);
      size_t sampleSize = samples.size();
      for (size_t i = 0; i != sampleSize; ++i) {
        if (isEntropyThresholdExceeded(dictEncoder, charSet, samples[i])) {
          return true;
        }
      }
    } else {
      size_t dictSize = dictEncoder.size();
      for (size_t i = 0; i != dictSize; ++i) {
        if (isEntropyThresholdExceeded(dictEncoder, charSet, i)) {
          return true;
        }
      }
    }
    return false;
  }

  dwio::common::DataBuffer<uint32_t> getSampleIndicesForEntropy(
      const StringDictionaryEncoder& dictEncoder) const {
    dwio::common::DataBuffer<uint32_t> indices{pool_, dictEncoder.size()};
    size_t numSamples = std::max(
        entropyMinSamples_,
        (size_t)(entropyDictSampleFraction_ * dictEncoder.size()));
    dwio::common::DataBuffer<uint32_t> samples{pool_, numSamples};
    size_t dictSize = dictEncoder.size();

    // The goal of this loop is to select numSamples number of distinct indices
    // of dictionary
    //
    // The loop works as follows, start with an array of zeros
    // On each iteration pick a random number in the range of 0 to size of the
    // dictionary minus one minus the number of previous iterations, thus the
    // effective size of the array is decreased by one with each iteration (not
    // actually but logically) Look at the value of the array at that random
    // index, if it is 0, the sample index is the random index because we've
    // never looked at this position before, if it's nonzero, that value is the
    // sample index Then take the value at the end of the logical size of the
    // array (size of the dictionary minus one minus the number of iterations)
    // if it's 0 put that index into the array at the random index, otherwise
    // put the nonzero value Thus, by reducing the logical size of the array and
    // moving the value at the end of the array we are removing indexes we have
    // previously visited and making sure we do not lose any indexes
    for (size_t i = 0; i != numSamples; ++i) {
      size_t index = folly::Random::rand32(dictSize - i);
      if (indices[index] == 0) {
        samples[i] = index;
      } else {
        samples[i] = indices[index];
      }
      indices[index] = indices[dictSize - i - 1] == 0
          ? dictSize - i - 1
          : indices[dictSize - i - 1];
    }
    return samples;
  }

  bool isEntropyThresholdExceeded(
      const StringDictionaryEncoder& dictEncoder,
      std::unordered_set<char>& charSet,
      size_t index) const {
    auto key = dictEncoder.getKey(index);
    size_t size = key.size();
    for (size_t i = 0; i != size; ++i) {
      charSet.insert(key[i]);
    }
    return charSet.size() > entropyThreshold_;
  }

  memory::MemoryPool& pool_;
  const float dictionaryKeySizeThreshold_;
  const float entropyKeySizeThreshold_;
  const size_t entropyMinSamples_;
  const float entropyDictSampleFraction_;
  const size_t entropyThreshold_;
};

} // namespace facebook::velox::dwrf
