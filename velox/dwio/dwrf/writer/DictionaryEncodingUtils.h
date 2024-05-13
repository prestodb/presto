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

#include "velox/dwio/dwrf/utils/BufferedWriter.h"
#include "velox/dwio/dwrf/writer/IntegerDictionaryEncoder.h"
#include "velox/dwio/dwrf/writer/StringDictionaryEncoder.h"

namespace facebook::velox::dwrf {

class DictionaryEncodingUtils {
 public:
  static bool shouldWriteKey(
      const StringDictionaryEncoder& dictEncoder,
      size_t index) {
    return dictEncoder.getCount(index) > 1;
  }

  static bool frequencyOrdering(
      const StringDictionaryEncoder& dictEncoder,
      size_t lhs,
      size_t rhs) {
    return dictEncoder.getCount(lhs) > dictEncoder.getCount(rhs);
  }

  // Sort the keys numerically, returns the number of elements in sorted
  // dictionary. The number of elements in sorted dictionary can be different
  // if we are using stride dictionary optimization.
  // Populates a lookup table that maps original indices to
  // sorted indices. In other words,
  //         lookupTable[index] = sortedIndex
  // NOTE: Dropped infrequent keys would be mapped to a separate dictionary
  // per stride. The method additionally populates a data buffer indicating the
  // size of the per stride dictionary for infrequent keys.
  template <typename DF, typename LF>
  static uint32_t getSortedIndexLookupTable(
      const StringDictionaryEncoder& dictEncoder,
      memory::MemoryPool& pool,
      bool sort,
      std::function<bool(const StringDictionaryEncoder&, size_t, size_t)>
          compare,
      bool dropInfrequentKeys,
      dwio::common::DataBuffer<uint32_t>& lookupTable,
      dwio::common::DataBuffer<bool>& inDict,
      dwio::common::DataBuffer<uint32_t>& strideDictSizes,
      DF dataFn,
      LF lengthFn) {
    auto numKeys = dictEncoder.size();
    dwio::common::DataBuffer<uint32_t> sortedIndex{pool};

    if (sort) {
      sortedIndex.reserve(numKeys);
      for (uint32_t i = 0; i != numKeys; ++i) {
        sortedIndex[i] = i;
      }
      std::sort(
          sortedIndex.data(),
          sortedIndex.data() + numKeys,
          [&](size_t lhs, size_t rhs) {
            return compare(dictEncoder, lhs, rhs);
          });
    }

    inDict.reserve(numKeys);
    lookupTable.reserve(numKeys);
    uint32_t newIndex = 0;
    auto dictLengthWriter =
        createBufferedWriter<uint32_t>(pool, 64 * 1024, lengthFn);
    auto errorGuard =
        folly::makeGuard([&dictLengthWriter]() { dictLengthWriter.abort(); });
    for (uint32_t i = 0; i != numKeys; ++i) {
      auto origIndex = (sort ? sortedIndex[i] : i);
      if (!dropInfrequentKeys || shouldWriteKey(dictEncoder, origIndex)) {
        lookupTable[origIndex] = newIndex++;
        inDict[origIndex] = true;
        auto key = dictEncoder.getKey(origIndex);
        dataFn(key.data(), key.size());
        dictLengthWriter.add(key.size());
      } else {
        lookupTable[origIndex] =
            strideDictSizes[dictEncoder.getStride(origIndex)]++;
        inDict[origIndex] = false;
      }
    }
    errorGuard.dismiss();
    dictLengthWriter.close();

    return newIndex;
  }
};

} // namespace facebook::velox::dwrf
