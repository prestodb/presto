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

#include "velox/serializers/PrestoSerializerSerializationUtils.h"

#include "velox/vector/BiasVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::serializer::presto::detail {
namespace {
template <TypeKind kind>
void serializeFlatVectorRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = vector->as<FlatVector<T>>();
  auto* rawValues = flatVector->rawValues();
  if (!flatVector->mayHaveNulls()) {
    for (auto& range : ranges) {
      stream->appendNonNull(range.size);
      stream->append<T>(folly::Range(&rawValues[range.begin], range.size));
    }
  } else {
    int32_t firstNonNull = -1;
    int32_t lastNonNull = -1;
    for (int32_t i = 0; i < ranges.size(); ++i) {
      const int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (flatVector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        if (std::is_same_v<T, StringView>) {
          // Bunching consecutive non-nulls into one append does not work with
          // strings because the lengths will then get out of order with the
          // zero lengths produced by nulls.
          stream->appendOne(rawValues[offset]);
        } else if (firstNonNull == -1) {
          firstNonNull = offset;
          lastNonNull = offset;
        } else if (offset == lastNonNull + 1) {
          lastNonNull = offset;
        } else {
          stream->append<T>(folly::Range(
              &rawValues[firstNonNull], 1 + lastNonNull - firstNonNull));
          firstNonNull = offset;
          lastNonNull = offset;
        }
      }
    }
    if (firstNonNull != -1 && !std::is_same_v<T, StringView>) {
      stream->append<T>(folly::Range(
          &rawValues[firstNonNull], 1 + lastNonNull - firstNonNull));
    }
  }
}

template <>
void serializeFlatVectorRanges<TypeKind::BOOLEAN>(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto flatVector = vector->as<FlatVector<bool>>();
  if (!vector->mayHaveNulls()) {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      stream->appendNonNull(ranges[i].size);
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        stream->appendOne<uint8_t>(flatVector->valueAtFast(offset) ? 1 : 0);
      }
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (vector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        stream->appendOne<uint8_t>(flatVector->valueAtFast(offset) ? 1 : 0);
      }
    }
  }
}

template <>
void serializeFlatVectorRanges<TypeKind::OPAQUE>(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  using T = typename TypeTraits<TypeKind::OPAQUE>::NativeType;
  auto* flatVector = vector->as<FlatVector<T>>();
  VELOX_CHECK_NOT_NULL(flatVector, "Should cast to FlatVector properly");

  auto opaqueType =
      std::dynamic_pointer_cast<const OpaqueType>(flatVector->type());
  auto serialization = opaqueType->getSerializeFunc();
  auto* rawValues = flatVector->rawValues();

  // Do not handle the case !flatVector->mayHaveNulls() in a special way like
  // we do for say TypeKind::VARCHAR, because we need to traverse each element
  // anyway to serialize them.
  for (int32_t i = 0; i < ranges.size(); ++i) {
    const int32_t end = ranges[i].begin + ranges[i].size;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (flatVector->isNullAt(offset)) {
        stream->appendNull();
        continue;
      }
      stream->appendNonNull();
      auto serialized = serialization(rawValues[offset]);
      const auto view = StringView(serialized);
      stream->appendOne(view);
    }
  }
}

void serializeWrappedRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  std::vector<IndexRange> newRanges;
  const bool mayHaveNulls = vector->mayHaveNulls();
  const VectorPtr& wrapped = BaseVector::wrappedVectorShared(vector);
  for (int32_t i = 0; i < ranges.size(); ++i) {
    const auto end = ranges[i].begin + ranges[i].size;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (mayHaveNulls && vector->isNullAt(offset)) {
        // The wrapper added a null.
        if (!newRanges.empty()) {
          serializeColumn(wrapped, newRanges, stream, scratch);
          newRanges.clear();
        }
        stream->appendNull();
        continue;
      }
      const auto innerIndex = vector->wrappedIndex(offset);
      newRanges.push_back(IndexRange{innerIndex, 1});
    }
  }
  if (!newRanges.empty()) {
    serializeColumn(wrapped, newRanges, stream, scratch);
  }
}

template <TypeKind kind>
void serializeConstantVectorRangesImpl(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto constVector = vector->as<ConstantVector<T>>();
  if (constVector->valueVector() != nullptr) {
    serializeWrappedRanges(vector, ranges, stream, scratch);
    return;
  }

  const int32_t count = rangesTotalSize(ranges);
  if (vector->isNullAt(0)) {
    for (int32_t i = 0; i < count; ++i) {
      stream->appendNull();
    }
    return;
  }

  if constexpr (std::is_same_v<T, std::shared_ptr<void>>) {
    auto opaqueType =
        std::dynamic_pointer_cast<const OpaqueType>(constVector->type());
    auto serialization = opaqueType->getSerializeFunc();
    T valueOpaque = constVector->valueAtFast(0);

    std::string serialized = serialization(valueOpaque);
    const auto value = StringView(serialized);
    for (int32_t i = 0; i < count; ++i) {
      stream->appendNonNull();
      stream->appendOne(value);
    }
  } else {
    T value = constVector->valueAtFast(0);
    for (int32_t i = 0; i < count; ++i) {
      stream->appendNonNull();
      stream->appendOne(value);
    }
  }
}

template <TypeKind Kind>
void serializeConstantVectorRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  if (stream->isConstantStream()) {
    for (const auto& range : ranges) {
      stream->appendNonNull(range.size);
    }

    std::vector<IndexRange> newRanges;
    newRanges.push_back({0, 1});
    serializeConstantVectorRangesImpl<Kind>(
        vector, newRanges, stream->childAt(0), scratch);
  } else {
    serializeConstantVectorRangesImpl<Kind>(vector, ranges, stream, scratch);
  }
}

template <TypeKind Kind>
void serializeDictionaryVectorRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  // Check if the stream was set up for dictionary (we had to know the
  // encoding type when creating VectorStream for that).
  if (!stream->isDictionaryStream()) {
    serializeWrappedRanges(vector, ranges, stream, scratch);
    return;
  }

  auto numRows = rangesTotalSize(ranges);

  // Cannot serialize dictionary as PrestoPage dictionary if it has nulls.
  if (vector->nulls()) {
    stream->flattenStream(vector, numRows);
    serializeWrappedRanges(vector, ranges, stream, scratch);
    return;
  }

  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto dictionaryVector = vector->as<DictionaryVector<T>>();

  ScratchPtr<vector_size_t, 64> selectedIndicesHolder(scratch);
  auto* mutableSelectedIndices =
      selectedIndicesHolder.get(dictionaryVector->valueVector()->size());
  auto numUsed = computeSelectedIndices(
      dictionaryVector, ranges, scratch, mutableSelectedIndices);

  if (!stream->preserveEncodings()) {
    // If the values are fixed width and we aren't getting enough reuse to
    // justify the dictionary, flatten it. For variable width types, rather than
    // iterate over them computing their size, we simply assume we'll get a
    // benefit.
    if constexpr (TypeTraits<Kind>::isFixedWidth) {
      // This calculation admittdely ignores some constants, but if they really
      // make a difference, they're small so there's not much difference either
      // way.
      if (numUsed * vector->type()->cppSizeInBytes() +
              numRows * sizeof(int32_t) >=
          numRows * vector->type()->cppSizeInBytes()) {
        stream->flattenStream(vector, numRows);
        serializeWrappedRanges(vector, ranges, stream, scratch);
        return;
      }
    }

    // If every element is unique the dictionary isn't giving us any benefit,
    // flatten it.
    if (numUsed == numRows) {
      stream->flattenStream(vector, numRows);
      serializeWrappedRanges(vector, ranges, stream, scratch);
      return;
    }
  }

  // Serialize the used elements from the Dictionary.
  serializeColumn(
      dictionaryVector->valueVector(),
      folly::Range<const vector_size_t*>(mutableSelectedIndices, numUsed),
      stream->childAt(0),
      scratch);

  // Create a mapping from the original indices to the indices in the shrunk
  // Dictionary of just used values.
  ScratchPtr<vector_size_t, 64> updatedIndicesHolder(scratch);
  auto* updatedIndices =
      updatedIndicesHolder.get(dictionaryVector->valueVector()->size());
  vector_size_t curIndex = 0;
  for (vector_size_t i = 0; i < numUsed; ++i) {
    updatedIndices[mutableSelectedIndices[i]] = curIndex++;
  }

  // Write out the indices, translating them using the above mapping.
  stream->appendNonNull(numRows);
  auto* indices = dictionaryVector->indices()->template as<vector_size_t>();
  for (const auto& range : ranges) {
    for (auto i = 0; i < range.size; ++i) {
      stream->appendOne(updatedIndices[indices[range.begin + i]]);
    }
  }
}

template <typename T>
void serializeBiasVectorRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto biasVector = vector->as<BiasVector<T>>();
  if (!vector->mayHaveNulls()) {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      stream->appendNonNull(ranges[i].size);
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        stream->appendOne(biasVector->valueAtFast(offset));
      }
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (biasVector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        stream->appendOne(biasVector->valueAtFast(offset));
      }
    }
  }
}

void serializeIPPrefixRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto wrappedVector = BaseVector::wrappedVectorShared(vector);
  auto rowVector = wrappedVector->asUnchecked<RowVector>();
  auto ip = rowVector->childAt(0)->asUnchecked<FlatVector<int128_t>>();
  auto prefix = rowVector->childAt(1)->asUnchecked<FlatVector<int8_t>>();

  for (int32_t i = 0; i < ranges.size(); ++i) {
    auto begin = ranges[i].begin;
    auto end = begin + ranges[i].size;
    for (auto offset = begin; offset < end; ++offset) {
      if (vector->isNullAt(offset)) {
        stream->appendNull();
        continue;
      }
      stream->appendNonNull();
      stream->appendLength(ipaddress::kIPPrefixBytes);
      stream->appendOne(
          toJavaIPPrefixType(ip->valueAt(offset), prefix->valueAt(offset)));
    }
  }
}

void serializeRowVectorRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  if (isIPPrefixType(vector->type())) {
    return serializeIPPrefixRanges(vector, ranges, stream);
  }
  auto rowVector = vector->as<RowVector>();
  std::vector<IndexRange> childRanges;
  for (int32_t i = 0; i < ranges.size(); ++i) {
    auto begin = ranges[i].begin;
    auto end = begin + ranges[i].size;
    for (auto offset = begin; offset < end; ++offset) {
      if (rowVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        stream->appendLength(1);
        childRanges.push_back(IndexRange{offset, 1});
      }
    }
  }
  for (int32_t i = 0; i < rowVector->childrenSize(); ++i) {
    serializeColumn(
        rowVector->childAt(i), childRanges, stream->childAt(i), scratch);
  }
}

void serializeArrayVectorRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  auto arrayVector = vector->as<ArrayVector>();
  auto rawSizes = arrayVector->rawSizes();
  auto rawOffsets = arrayVector->rawOffsets();
  std::vector<IndexRange> childRanges;
  childRanges.reserve(ranges.size());
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    for (int32_t offset = begin; offset < end; ++offset) {
      if (arrayVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        auto size = rawSizes[offset];
        stream->appendLength(size);
        if (size > 0) {
          childRanges.emplace_back<IndexRange>({rawOffsets[offset], size});
        }
      }
    }
  }
  serializeColumn(
      arrayVector->elements(), childRanges, stream->childAt(0), scratch);
}

void serializeMapVectorRanges(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  auto mapVector = vector->as<MapVector>();
  auto rawSizes = mapVector->rawSizes();
  auto rawOffsets = mapVector->rawOffsets();
  std::vector<IndexRange> childRanges;
  childRanges.reserve(ranges.size());
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    for (int32_t offset = begin; offset < end; ++offset) {
      if (mapVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        auto size = rawSizes[offset];
        stream->appendLength(size);
        if (size > 0) {
          childRanges.emplace_back<IndexRange>({rawOffsets[offset], size});
        }
      }
    }
  }
  serializeColumn(
      mapVector->mapKeys(), childRanges, stream->childAt(0), scratch);
  serializeColumn(
      mapVector->mapValues(), childRanges, stream->childAt(1), scratch);
}

void appendTimestamps(
    const uint64_t* nulls,
    folly::Range<const vector_size_t*> rows,
    const Timestamp* timestamps,
    VectorStream* stream,
    Scratch& scratch) {
  if (nulls == nullptr) {
    stream->appendNonNull(rows.size());
    for (auto i = 0; i < rows.size(); ++i) {
      stream->appendOne(timestamps[rows[i]]);
    }
    return;
  }

  ScratchPtr<vector_size_t, 64> nonNullHolder(scratch);
  auto* nonNullRows = nonNullHolder.get(rows.size());
  const auto numNonNull =
      simd::indicesOfSetBits(nulls, 0, rows.size(), nonNullRows);
  stream->appendNulls(nulls, 0, rows.size(), numNonNull);
  for (auto i = 0; i < numNonNull; ++i) {
    stream->appendOne(timestamps[rows[nonNullRows[i]]]);
  }
}

void appendStrings(
    const uint64_t* nulls,
    folly::Range<const vector_size_t*> rows,
    const StringView* views,
    VectorStream* stream,
    Scratch& scratch) {
  if (nulls == nullptr) {
    stream->appendLengths(nullptr, rows, rows.size(), [&](auto row) {
      return views[row].size();
    });
    for (auto i = 0; i < rows.size(); ++i) {
      const auto& view = views[rows[i]];
      stream->values().appendStringView(
          std::string_view(view.data(), view.size()));
    }
    return;
  }

  ScratchPtr<vector_size_t, 64> nonNullHolder(scratch);
  auto* nonNull = nonNullHolder.get(rows.size());
  const auto numNonNull =
      simd::indicesOfSetBits(nulls, 0, rows.size(), nonNull);
  stream->appendLengths(
      nulls, rows, numNonNull, [&](auto row) { return views[row].size(); });
  for (auto i = 0; i < numNonNull; ++i) {
    auto& view = views[rows[nonNull[i]]];
    stream->values().appendStringView(
        std::string_view(view.data(), view.size()));
  }
}

void serializeIPPrefix(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream) {
  auto wrappedVector = BaseVector::wrappedVectorShared(vector);
  if (!vector->mayHaveNulls()) {
    // No nulls, and because ipprefix are fixed size, just append the fixed
    // lengths of 17 bytes
    stream->appendLengths(nullptr, rows, rows.size(), [&](auto /*row*/) {
      return ipaddress::kIPPrefixBytes;
    });

    auto rowVector = wrappedVector->asChecked<RowVector>();
    auto ip = rowVector->childAt(0)->asChecked<FlatVector<int128_t>>();
    auto prefix = rowVector->childAt(1)->asChecked<FlatVector<int8_t>>();
    for (auto i = 0; i < rows.size(); ++i) {
      // Append the first 16 bytes in reverse order because Java always stores
      // the ipaddress porition as big endian whereas Velox stores it as little
      auto javaIPPrefix =
          toJavaIPPrefixType(ip->valueAt(rows[i]), prefix->valueAt(rows[i]));
      stream->values().appendStringView(std::string_view(
          (const char*)javaIPPrefix.data(), javaIPPrefix.size()));
    }
    return;
  }

  auto rowVector = wrappedVector->asChecked<RowVector>();
  auto ip = rowVector->childAt(0)->asChecked<FlatVector<int128_t>>();
  auto prefix = rowVector->childAt(1)->asChecked<FlatVector<int8_t>>();
  for (auto i = 0; i < rows.size(); ++i) {
    if (vector->isNullAt(rows[i])) {
      stream->appendNull();
      continue;
    }
    stream->appendNonNull();
    stream->appendLength(ipaddress::kIPPrefixBytes);
    stream->appendOne(
        toJavaIPPrefixType(ip->valueAt(rows[i]), prefix->valueAt(rows[i])));
  }
}

template <typename T, typename Conv = folly::Identity>
void copyWords(
    uint8_t* destination,
    const int32_t* indices,
    int32_t numIndices,
    const T* values,
    Conv&& conv = {}) {
  for (auto i = 0; i < numIndices; ++i) {
    folly::storeUnaligned(
        destination + i * sizeof(T), conv(values[indices[i]]));
  }
}

template <typename T, typename Conv = folly::Identity>
void copyWordsWithRows(
    uint8_t* destination,
    const int32_t* rows,
    const int32_t* indices,
    int32_t numIndices,
    const T* values,
    Conv&& conv = {}) {
  if (!indices) {
    copyWords(destination, rows, numIndices, values, std::forward<Conv>(conv));
    return;
  }
  for (auto i = 0; i < numIndices; ++i) {
    folly::storeUnaligned(
        destination + i * sizeof(T), conv(values[rows[indices[i]]]));
  }
}

template <typename T>
void appendNonNull(
    VectorStream* stream,
    const uint64_t* nulls,
    folly::Range<const vector_size_t*> rows,
    const T* values,
    Scratch& scratch) {
  auto numRows = rows.size();
  ScratchPtr<int32_t, 64> nonNullHolder(scratch);
  const int32_t* nonNullIndices;
  int32_t numNonNull;
  if (LIKELY(numRows <= 8)) {
    // Short batches need extra optimization. The set bits are prematerialized.
    uint8_t nullsByte = *reinterpret_cast<const uint8_t*>(nulls);
    numNonNull = __builtin_popcount(nullsByte);
    nonNullIndices =
        numNonNull == numRows ? nullptr : simd::byteSetBits(nullsByte);
  } else {
    auto mutableIndices = nonNullHolder.get(numRows);
    // Convert null flags to indices. This is much faster than checking bits one
    // by one, several bits per clock specially if mostly null or non-null. Even
    // worst case of half nulls is more than one row per clock.
    numNonNull = simd::indicesOfSetBits(nulls, 0, numRows, mutableIndices);
    nonNullIndices = numNonNull == numRows ? nullptr : mutableIndices;
  }
  stream->appendNulls(nulls, 0, rows.size(), numNonNull);
  ByteOutputStream& out = stream->values();

  if constexpr (sizeof(T) == 8) {
    AppendWindow<int64_t> window(out, scratch);
    auto* output = window.get(numNonNull);
    copyWordsWithRows(
        output,
        rows.data(),
        nonNullIndices,
        numNonNull,
        reinterpret_cast<const int64_t*>(values));
  } else if constexpr (sizeof(T) == 4) {
    AppendWindow<int32_t> window(out, scratch);
    auto* output = window.get(numNonNull);
    copyWordsWithRows(
        output,
        rows.data(),
        nonNullIndices,
        numNonNull,
        reinterpret_cast<const int32_t*>(values));
  } else {
    AppendWindow<T> window(out, scratch);
    auto* output = window.get(numNonNull);
    if (stream->isLongDecimal()) {
      copyWordsWithRows(
          output,
          rows.data(),
          nonNullIndices,
          numNonNull,
          values,
          toJavaDecimalValue);
    } else if (stream->isUuid()) {
      copyWordsWithRows(
          output,
          rows.data(),
          nonNullIndices,
          numNonNull,
          values,
          toJavaUuidValue);
    } else if (stream->isIpAddress()) {
      copyWordsWithRows(
          output,
          rows.data(),
          nonNullIndices,
          numNonNull,
          values,
          reverseIpAddressByteOrder);
    } else {
      copyWordsWithRows(
          output, rows.data(), nonNullIndices, numNonNull, values);
    }
  }
}

template <TypeKind kind>
void serializeFlatVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = vector->asUnchecked<FlatVector<T>>();
  auto* rawValues = flatVector->rawValues();
  if (!flatVector->mayHaveNulls()) {
    if constexpr (std::is_same_v<T, Timestamp>) {
      appendTimestamps(nullptr, rows, rawValues, stream, scratch);
    } else if constexpr (std::is_same_v<T, StringView>) {
      appendStrings(nullptr, rows, rawValues, stream, scratch);
    } else {
      stream->appendNonNull(rows.size());
      AppendWindow<T> window(stream->values(), scratch);
      auto* output = window.get(rows.size());
      if (stream->isLongDecimal()) {
        copyWords(
            output, rows.data(), rows.size(), rawValues, toJavaDecimalValue);
      } else if (stream->isUuid()) {
        copyWords(output, rows.data(), rows.size(), rawValues, toJavaUuidValue);
      } else if (stream->isIpAddress()) {
        copyWords(
            output,
            rows.data(),
            rows.size(),
            rawValues,
            reverseIpAddressByteOrder);
      } else {
        copyWords(output, rows.data(), rows.size(), rawValues);
      }
    }
    return;
  }

  ScratchPtr<uint64_t, 4> nullsHolder(scratch);
  uint64_t* nulls = nullsHolder.get(bits::nwords(rows.size()));
  simd::gatherBits(vector->rawNulls(), rows, nulls);
  if constexpr (std::is_same_v<T, Timestamp>) {
    appendTimestamps(nulls, rows, rawValues, stream, scratch);
  } else if constexpr (std::is_same_v<T, StringView>) {
    appendStrings(nulls, rows, rawValues, stream, scratch);
  } else {
    appendNonNull(stream, nulls, rows, rawValues, scratch);
  }
}

uint64_t bitsToBytesMap[256];

uint64_t bitsToBytes(uint8_t byte) {
  return bitsToBytesMap[byte];
}

template <>
void serializeFlatVector<TypeKind::BOOLEAN>(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto* flatVector = vector->as<FlatVector<bool>>();
  auto* rawValues = flatVector->rawValues<uint64_t>();
  ScratchPtr<uint64_t, 4> bitsHolder(scratch);
  uint64_t* valueBits;
  int32_t numValueBits;
  if (!flatVector->mayHaveNulls()) {
    stream->appendNonNull(rows.size());
    valueBits = bitsHolder.get(bits::nwords(rows.size()));
    simd::gatherBits(rawValues, rows, valueBits);
    numValueBits = rows.size();
  } else {
    uint64_t* nulls = bitsHolder.get(bits::nwords(rows.size()));
    simd::gatherBits(vector->rawNulls(), rows, nulls);
    ScratchPtr<vector_size_t, 64> nonNullsHolder(scratch);
    auto* nonNulls = nonNullsHolder.get(rows.size());
    numValueBits = simd::indicesOfSetBits(nulls, 0, rows.size(), nonNulls);
    stream->appendNulls(nulls, 0, rows.size(), numValueBits);
    valueBits = nulls;
    simd::transpose(
        rows.data(),
        folly::Range<const vector_size_t*>(nonNulls, numValueBits),
        nonNulls);
    simd::gatherBits(
        rawValues,
        folly::Range<const vector_size_t*>(nonNulls, numValueBits),
        valueBits);
  }

  // 'valueBits' contains the non-null bools to be appended to the
  // stream. The wire format has a byte for each bit. Every full byte
  // is appended as a word. The partial byte is translated to a word
  // and its low bytes are appended.
  AppendWindow<uint8_t> window(stream->values(), scratch);
  uint8_t* output = window.get(numValueBits);
  const auto numBytes = bits::nbytes(numValueBits);
  for (auto i = 0; i < numBytes; ++i) {
    uint64_t word = bitsToBytes(reinterpret_cast<uint8_t*>(valueBits)[i]);
    auto* target = output + i * 8;
    if (i < numBytes - 1) {
      folly::storeUnaligned(target, word);
    } else {
      memcpy(target, &word, numValueBits - i * 8);
    }
  }
}

template <>
void serializeFlatVector<TypeKind::UNKNOWN>(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  VELOX_CHECK_NOT_NULL(vector->rawNulls());
  for (auto i = 0; i < rows.size(); ++i) {
    VELOX_DCHECK(vector->isNullAt(rows[i]));
    stream->appendNull();
  }
}

template <>
void serializeFlatVector<TypeKind::OPAQUE>(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  VELOX_UNSUPPORTED();
}

void serializeWrapped(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  ScratchPtr<vector_size_t, 1> innerRowsHolder(scratch);
  const int32_t numRows = rows.size();
  int32_t numInner = 0;
  auto* innerRows = innerRowsHolder.get(numRows);
  bool mayHaveNulls = vector->mayHaveNulls();
  const VectorPtr* wrapped;
  if (vector->encoding() == VectorEncoding::Simple::DICTIONARY &&
      !mayHaveNulls) {
    // Dictionary with no nulls.
    auto* indices = vector->wrapInfo()->as<vector_size_t>();
    wrapped = &vector->valueVector();
    simd::transpose(indices, rows, innerRows);
    numInner = numRows;
  } else {
    wrapped = &BaseVector::wrappedVectorShared(vector);
    for (int32_t i = 0; i < rows.size(); ++i) {
      if (mayHaveNulls && vector->isNullAt(rows[i])) {
        // The wrapper added a null.
        if (numInner > 0) {
          serializeColumn(
              *wrapped,
              folly::Range<const vector_size_t*>(innerRows, numInner),
              stream,
              scratch);
          numInner = 0;
        }
        stream->appendNull();
        continue;
      }
      innerRows[numInner++] = vector->wrappedIndex(rows[i]);
    }
  }

  if (numInner > 0) {
    serializeColumn(
        *wrapped,
        folly::Range<const vector_size_t*>(innerRows, numInner),
        stream,
        scratch);
  }
}

template <TypeKind kind>
void serializeConstantVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  if (isIPPrefixType(vector->type())) {
    return serializeIPPrefix(vector, rows, stream);
  }

  using T = typename KindToFlatVector<kind>::WrapperType;
  auto constVector = vector->as<ConstantVector<T>>();
  if (constVector->valueVector()) {
    serializeWrapped(vector, rows, stream, scratch);
    return;
  }
  const auto numRows = rows.size();
  if (vector->isNullAt(0)) {
    for (int32_t i = 0; i < numRows; ++i) {
      stream->appendNull();
    }
    return;
  }

  T value = constVector->valueAtFast(0);
  for (int32_t i = 0; i < numRows; ++i) {
    stream->appendNonNull();
    stream->appendOne(value);
  }
}

void serializeRowVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  if (isIPPrefixType(vector->type())) {
    return serializeIPPrefix(vector, rows, stream);
  }
  auto rowVector = vector->as<RowVector>();
  ScratchPtr<uint64_t, 4> nullsHolder(scratch);
  ScratchPtr<vector_size_t, 64> innerRowsHolder(scratch);
  auto innerRows = rows.data();
  auto numInnerRows = rows.size();
  if (auto rawNulls = vector->rawNulls()) {
    auto nulls = nullsHolder.get(bits::nwords(rows.size()));
    simd::gatherBits(rawNulls, rows, nulls);
    auto* mutableInnerRows = innerRowsHolder.get(rows.size());
    numInnerRows =
        simd::indicesOfSetBits(nulls, 0, rows.size(), mutableInnerRows);
    stream->appendLengths(nulls, rows, numInnerRows, [](int32_t) { return 1; });
    simd::transpose(
        rows.data(),
        folly::Range<const vector_size_t*>(mutableInnerRows, numInnerRows),
        mutableInnerRows);
    innerRows = mutableInnerRows;
  } else {
    stream->appendLengths(
        nullptr, rows, rows.size(), [](int32_t) { return 1; });
  }
  for (int32_t i = 0; i < rowVector->childrenSize(); ++i) {
    serializeColumn(
        rowVector->childAt(i),
        folly::Range<const vector_size_t*>(innerRows, numInnerRows),
        stream->childAt(i),
        scratch);
  }
}

void serializeArrayVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto arrayVector = vector->as<ArrayVector>();

  ScratchPtr<IndexRange> rangesHolder(scratch);
  int32_t numRanges = rowsToRanges(
      rows,
      arrayVector->rawNulls(),
      arrayVector->rawOffsets(),
      arrayVector->rawSizes(),
      nullptr,
      rangesHolder,
      nullptr,
      stream,
      scratch);
  if (numRanges == 0) {
    return;
  }
  serializeColumn(
      arrayVector->elements(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(0),
      scratch);
}

void serializeMapVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto mapVector = vector->as<MapVector>();

  ScratchPtr<IndexRange> rangesHolder(scratch);
  int32_t numRanges = rowsToRanges(
      rows,
      mapVector->rawNulls(),
      mapVector->rawOffsets(),
      mapVector->rawSizes(),
      nullptr,
      rangesHolder,
      nullptr,
      stream,
      scratch);
  if (numRanges == 0) {
    return;
  }
  serializeColumn(
      mapVector->mapKeys(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(0),
      scratch);
  serializeColumn(
      mapVector->mapValues(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(1),
      scratch);
}
} // namespace

void initBitsToMapOnce() {
  static folly::once_flag initOnceFlag;
  folly::call_once(initOnceFlag, [&]() {
    auto toByte = [](int32_t number, int32_t bit) {
      return static_cast<uint64_t>(bits::isBitSet(&number, bit)) << (bit * 8);
    };
    for (auto i = 0; i < 256; ++i) {
      bitsToBytesMap[i] = toByte(i, 0) | toByte(i, 1) | toByte(i, 2) |
          toByte(i, 3) | toByte(i, 4) | toByte(i, 5) | toByte(i, 6) |
          toByte(i, 7);
    }
  });
}

std::string_view typeToEncodingName(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return kByteArray;
    case TypeKind::TINYINT:
      return kByteArray;
    case TypeKind::SMALLINT:
      return kShortArray;
    case TypeKind::INTEGER:
      return kIntArray;
    case TypeKind::BIGINT:
      return kLongArray;
    case TypeKind::HUGEINT:
      return kInt128Array;
    case TypeKind::REAL:
      return kIntArray;
    case TypeKind::DOUBLE:
      return kLongArray;
    case TypeKind::VARCHAR:
      return kVariableWidth;
    case TypeKind::VARBINARY:
      return kVariableWidth;
    case TypeKind::TIMESTAMP:
      return kLongArray;
    case TypeKind::ARRAY:
      return kArray;
    case TypeKind::MAP:
      return kMap;
    case TypeKind::ROW:
      if (isIPPrefixType(type)) {
        return kVariableWidth;
      }
      return kRow;
    case TypeKind::UNKNOWN:
      return kByteArray;
    case TypeKind::OPAQUE:
      return kVariableWidth;

    default:
      VELOX_FAIL("Unknown type kind: {}", static_cast<int>(type->kind()));
  }
}

void serializeColumn(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          serializeFlatVectorRanges,
          vector->typeKind(),
          vector,
          ranges,
          stream);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeConstantVectorRanges,
          vector->typeKind(),
          vector,
          ranges,
          stream,
          scratch);
      break;
    case VectorEncoding::Simple::DICTIONARY:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeDictionaryVectorRanges,
          vector->typeKind(),
          vector,
          ranges,
          stream,
          scratch);
      break;
    case VectorEncoding::Simple::BIASED:
      switch (vector->typeKind()) {
        case TypeKind::SMALLINT:
          serializeBiasVectorRanges<int16_t>(vector, ranges, stream);
          break;
        case TypeKind::INTEGER:
          serializeBiasVectorRanges<int32_t>(vector, ranges, stream);
          break;
        case TypeKind::BIGINT:
          serializeBiasVectorRanges<int64_t>(vector, ranges, stream);
          break;
        default:
          VELOX_FAIL(
              "Invalid biased vector type {}",
              static_cast<int>(vector->encoding()));
      }
      break;
    case VectorEncoding::Simple::ROW:
      serializeRowVectorRanges(vector, ranges, stream, scratch);
      break;
    case VectorEncoding::Simple::ARRAY:
      serializeArrayVectorRanges(vector, ranges, stream, scratch);
      break;
    case VectorEncoding::Simple::MAP:
      serializeMapVectorRanges(vector, ranges, stream, scratch);
      break;
    case VectorEncoding::Simple::LAZY:
      serializeColumn(
          BaseVector::loadedVectorShared(vector), ranges, stream, scratch);
      break;
    default:
      serializeWrappedRanges(vector, ranges, stream, scratch);
  }
}

void serializeColumn(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          serializeFlatVector,
          vector->typeKind(),
          vector,
          rows,
          stream,
          scratch);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeConstantVector,
          vector->typeKind(),
          vector,
          rows,
          stream,
          scratch);
      break;
    case VectorEncoding::Simple::BIASED:
      VELOX_UNSUPPORTED();
    case VectorEncoding::Simple::ROW:
      serializeRowVector(vector, rows, stream, scratch);
      break;
    case VectorEncoding::Simple::ARRAY:
      serializeArrayVector(vector, rows, stream, scratch);
      break;
    case VectorEncoding::Simple::MAP:
      serializeMapVector(vector, rows, stream, scratch);
      break;
    case VectorEncoding::Simple::LAZY:
      serializeColumn(
          BaseVector::loadedVectorShared(vector), rows, stream, scratch);
      break;
    default:
      serializeWrapped(vector, rows, stream, scratch);
  }
}

int32_t rowsToRanges(
    folly::Range<const vector_size_t*> rows,
    const uint64_t* rawNulls,
    const vector_size_t* offsets,
    const vector_size_t* sizes,
    vector_size_t** sizesPtr,
    ScratchPtr<IndexRange>& rangesHolder,
    ScratchPtr<vector_size_t*>* sizesHolder,
    VectorStream* stream,
    Scratch& scratch) {
  auto numRows = rows.size();
  auto* innerRows = rows.data();
  auto* nonNullRows = innerRows;
  int32_t numInner = rows.size();
  ScratchPtr<vector_size_t, 64> nonNullHolder(scratch);
  ScratchPtr<vector_size_t, 64> innerRowsHolder(scratch);
  if (rawNulls) {
    ScratchPtr<uint64_t, 4> nullsHolder(scratch);
    auto* nulls = nullsHolder.get(bits::nwords(rows.size()));
    simd::gatherBits(rawNulls, rows, nulls);
    auto* mutableNonNullRows = nonNullHolder.get(numRows);
    auto* mutableInnerRows = innerRowsHolder.get(numRows);
    numInner = simd::indicesOfSetBits(nulls, 0, numRows, mutableNonNullRows);
    if (stream) {
      stream->appendLengths(
          nulls, rows, numInner, [&](auto row) { return sizes[row]; });
    }
    simd::transpose(
        rows.data(),
        folly::Range<const vector_size_t*>(mutableNonNullRows, numInner),
        mutableInnerRows);
    nonNullRows = mutableNonNullRows;
    innerRows = mutableInnerRows;
  } else if (stream) {
    stream->appendNonNull(rows.size());
    for (auto i = 0; i < rows.size(); ++i) {
      stream->appendLength(sizes[rows[i]]);
    }
  }
  vector_size_t** sizesOut = nullptr;
  if (sizesPtr) {
    sizesOut = sizesHolder->get(numInner);
  }
  auto ranges = rangesHolder.get(numInner);
  int32_t fill = 0;
  for (auto i = 0; i < numInner; ++i) {
    // Add the size of the length.
    if (sizesPtr) {
      *sizesPtr[rawNulls ? nonNullRows[i] : i] += sizeof(int32_t);
    }
    if (sizes[innerRows[i]] == 0) {
      continue;
    }
    if (sizesOut) {
      sizesOut[fill] = sizesPtr[rawNulls ? nonNullRows[i] : i];
    }
    ranges[fill].begin = offsets[innerRows[i]];
    ranges[fill].size = sizes[innerRows[i]];
    ++fill;
  }
  return fill;
}
} // namespace facebook::velox::serializer::presto::detail
