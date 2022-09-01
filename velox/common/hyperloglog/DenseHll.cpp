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
#include "velox/common/hyperloglog/DenseHll.h"

#include <exception>
#include <sstream>
#include "velox/common/base/IOUtils.h"
#include "velox/common/hyperloglog/BiasCorrection.h"
#include "velox/common/hyperloglog/HllUtils.h"

namespace facebook::velox::common::hll {
namespace {
const int kBitsPerBucket = 4;
const int8_t kMaxDelta = (1 << kBitsPerBucket) - 1;
const int8_t kBucketMask = (1 << kBitsPerBucket) - 1;
constexpr double kLinearCountingMinEmptyBuckets = 0.4;

/// Buckets are stored in a byte array. Each byte stored 2 buckets, 4 bits each.
/// Even buckets are stored in the first 4 bits of the byte. Odd buckets are
/// stored in the last 4 bits of the byte. This function returns the offset in
/// the byte for a given bucket, e.g. 0 for even and 4 for odd buckets.
int8_t shiftForBucket(int32_t index) {
  // ((1 - bucket) % 2) * kBitsPerBucket
  return ((~index) & 1) << 2;
}

/// Returns the value of alpha constant. See "Practical considerations" section
/// in https://en.wikipedia.org/wiki/HyperLogLog
double alpha(int32_t indexBitLength) {
  switch (indexBitLength) {
    case 4:
      return 0.673;
    case 5:
      return 0.697;
    case 6:
      return 0.709;
    default:
      return (0.7213 / (1 + 1.079 / (1 << indexBitLength)));
  }
}

/// Performs binary search for value 'rawEstimate' in a sorted list
/// 'estimateCurve'. Returns the position of the value if found. Otherwise,
/// returns a negated insert position.
int search(double rawEstimate, const std::vector<double>& estimateCurve) {
  uint32_t low = 0;
  uint32_t high = estimateCurve.size() - 1;

  while (low <= high) {
    int middle = (low + high) >> 1;

    double middleValue = estimateCurve[middle];

    if (rawEstimate > middleValue) {
      low = middle + 1;
    } else if (rawEstimate < middleValue) {
      high = middle - 1;
    } else {
      return middle;
    }
  }

  return -(low + 1);
}

int8_t getOverflowImpl(
    int32_t index,
    int32_t overflows,
    const uint16_t* overflowBuckets,
    const int8_t* overflowValues) {
  for (int i = 0; i < overflows; i++) {
    if (overflowBuckets[i] == index) {
      return overflowValues[i];
    }
  }
  return 0;
}

double correctBias(double rawEstimate, int8_t indexBitLength) {
  const auto& estimates = BiasCorrection::kRawEstimates[indexBitLength - 4];
  if (rawEstimate < estimates[0] ||
      rawEstimate > estimates[estimates.size() - 1]) {
    return rawEstimate;
  }

  const auto& biases = BiasCorrection::kBias[indexBitLength - 4];

  int position = search(rawEstimate, estimates);

  double bias;
  if (position >= 0) {
    bias = biases[position];
  } else {
    // interpolate
    int insertionPoint = -(position + 1);

    double x0 = estimates[insertionPoint - 1];
    double y0 = biases[insertionPoint - 1];
    double x1 = estimates[insertionPoint];
    double y1 = biases[insertionPoint];

    bias = ((((rawEstimate - x0) * (y1 - y0)) / (x1 - x0)) + y0);
  }

  return rawEstimate - bias;
}
} // namespace

DenseHll::DenseHll(int8_t indexBitLength, HashStringAllocator* allocator)
    : deltas_{StlAllocator<int8_t>(allocator)},
      overflowBuckets_{StlAllocator<uint16_t>(allocator)},
      overflowValues_{StlAllocator<int8_t>(allocator)} {
  initialize(indexBitLength);
}

void DenseHll::initialize(int8_t indexBitLength) {
  VELOX_CHECK_GE(indexBitLength, 4, "indexBitLength must be in [4, 16] range");
  VELOX_CHECK_LE(indexBitLength, 16, "indexBitLength must be in [4, 16] range");

  indexBitLength_ = indexBitLength;

  auto numBuckets = 1 << indexBitLength;
  baselineCount_ = numBuckets;
  deltas_.resize(numBuckets * kBitsPerBucket / 8);
}

void DenseHll::insertHash(uint64_t hash) {
  auto index = computeIndex(hash, indexBitLength_);
  auto value = computeValue(hash, indexBitLength_);
  insert(index, value);
}

void DenseHll::insert(int32_t index, int8_t value) {
  auto delta = value - baseline_;
  auto oldDelta = getDelta(index);

  if (delta <= oldDelta ||
      (oldDelta == kMaxDelta && (delta <= oldDelta + getOverflow(index)))) {
    // The old bucket value is (baseline + oldDelta) + possibly an overflow, so
    // it's guaranteed to be >= the new value.
    return;
  }

  if (delta > kMaxDelta) {
    int8_t overflow = (int8_t)(delta - kMaxDelta);

    int overflowEntry = findOverflowEntry(index);
    if (overflowEntry != -1) {
      overflowValues_[overflowEntry] = overflow;
    } else {
      addOverflow(index, overflow);
    }

    delta = kMaxDelta;
  }

  setDelta(index, delta);

  if (oldDelta == 0) {
    --baselineCount_;
    adjustBaselineIfNeeded();
  }
}

namespace {

struct DenseHllView {
  int8_t indexBitLength;
  int8_t baseline;
  const int8_t* deltas;
  int16_t overflows;
  const uint16_t* overflowBuckets;
  const int8_t* overflowValues;

  int8_t getDelta(int32_t index) const {
    int slot = index >> 1;
    return (deltas[slot] >> shiftForBucket(index)) & kBucketMask;
  }

  int8_t getValue(int32_t index) const {
    auto delta = getDelta(index);

    if (delta == kMaxDelta) {
      delta +=
          getOverflowImpl(index, overflows, overflowBuckets, overflowValues);
    }

    return baseline + delta;
  }
};

int64_t cardinalityImpl(const DenseHllView& hll) {
  auto numBuckets = 1 << hll.indexBitLength;

  int32_t baselineCount = 0;
  for (int i = 0; i < numBuckets; i++) {
    if (hll.getDelta(i) == 0) {
      baselineCount++;
    }
  }

  // If baseline is zero, then baselineCount is the number of buckets with value
  // 0.
  if ((hll.baseline == 0) &&
      (baselineCount > (kLinearCountingMinEmptyBuckets * numBuckets))) {
    return std::round(linearCounting(baselineCount, numBuckets));
  }

  double sum = 0;
  for (int i = 0; i < numBuckets; i++) {
    int value = hll.getValue(i);
    sum += 1.0 / (1L << value);
  }

  double estimate = (alpha(hll.indexBitLength) * numBuckets * numBuckets) / sum;
  estimate = correctBias(estimate, hll.indexBitLength);

  return std::round(estimate);
}

DenseHllView deserialize(const char* serialized) {
  common::InputByteStream stream(serialized);

  auto version = stream.read<int8_t>();
  VELOX_CHECK_EQ(kPrestoDenseV2, version);

  auto indexBitLength = stream.read<int8_t>();
  auto baseline = stream.read<int8_t>();

  auto numBuckets = 1 << indexBitLength;
  // next numBuckets / 2 bytes are deltas
  const int8_t* deltas = stream.read<int8_t>(numBuckets / 2);

  auto overflows = stream.read<int16_t>();

  const uint16_t* overflowBuckets =
      overflows ? stream.read<uint16_t>(overflows) : nullptr;
  const int8_t* overflowValues =
      overflows ? stream.read<int8_t>(overflows) : nullptr;

  return DenseHllView{
      indexBitLength,
      baseline,
      deltas,
      overflows,
      overflowBuckets,
      overflowValues};
}
} // namespace

int64_t DenseHll::cardinality() const {
  DenseHllView hll{
      indexBitLength_,
      baseline_,
      deltas_.data(),
      overflows_,
      overflowBuckets_.data(),
      overflowValues_.data()};
  return cardinalityImpl(hll);
}

// static
int64_t DenseHll::cardinality(const char* serialized) {
  auto hll = deserialize(serialized);
  return cardinalityImpl(hll);
}

int8_t DenseHll::getDelta(int32_t index) const {
  int slot = index >> 1;
  return (deltas_[slot] >> shiftForBucket(index)) & kBucketMask;
}

void DenseHll::setDelta(int32_t index, int8_t value) {
  int slot = index >> 1;

  // Clear the old value.
  int8_t clearMask = (int8_t)(kBucketMask << shiftForBucket(index));
  deltas_[slot] &= ~clearMask;

  // Set the new value.
  int8_t setMask = (int8_t)(value << shiftForBucket(index));
  deltas_[slot] |= setMask;
}

int8_t DenseHll::getOverflow(int32_t index) const {
  return getOverflowImpl(
      index, overflows_, overflowBuckets_.data(), overflowValues_.data());
}

int DenseHll::findOverflowEntry(int32_t index) const {
  for (auto i = 0; i < overflows_; i++) {
    if (overflowBuckets_[i] == index) {
      return i;
    }
  }
  return -1;
}

void DenseHll::adjustBaselineIfNeeded() {
  auto numBuckets = 1 << indexBitLength_;

  while (baselineCount_ == 0) {
    baseline_++;

    for (int bucket = 0; bucket < numBuckets; ++bucket) {
      int delta = getDelta(bucket);

      bool hasOverflow = false;
      if (delta == kMaxDelta) {
        // scan overflows
        for (int i = 0; i < overflows_; i++) {
          if (overflowBuckets_[i] == bucket) {
            hasOverflow = true;
            overflowValues_[i]--;

            if (overflowValues_[i] == 0) {
              int lastEntry = overflows_ - 1;
              if (i < lastEntry) {
                // remove the entry by moving the last entry to this position
                overflowBuckets_[i] = overflowBuckets_[lastEntry];
                overflowValues_[i] = overflowValues_[lastEntry];

                // clean up to make it easier to catch bugs
                overflowBuckets_[lastEntry] = 0;
                overflowValues_[lastEntry] = 0;
              }
              overflows_--;
            }
            break;
          }
        }
      }

      if (!hasOverflow) {
        // getDelta is guaranteed to return a value greater than zero
        // because baselineCount is zero (i.e., number of deltas with zero
        // value) So it's safe to decrement here
        delta--;
        setDelta(bucket, delta);
      }

      if (delta == 0) {
        ++baselineCount_;
      }
    }
  }
}

void DenseHll::sortOverflows() {
  // traditional insertion sort (ok for small arrays)
  for (int i = 1; i < overflows_; i++) {
    auto bucket = overflowBuckets_[i];
    int j = i - 1;
    for (; j >= 0 && overflowBuckets_[j] > bucket; j--) {
    }

    // Shift [j + 1, i - 1] entries by one to the right.
    // Insert bucket into j + 1 position.
    if (j + 1 < i) {
      auto value = overflowValues_[i];
      memmove(
          overflowBuckets_.data() + j + 2,
          overflowBuckets_.data() + j + 1,
          sizeof(overflowBuckets_[0]) * (i - j - 1));
      memmove(
          overflowValues_.data() + j + 2,
          overflowValues_.data() + j + 1,
          sizeof(overflowValues_[0]) * (i - j - 1));
      overflowBuckets_[j + 1] = bucket;
      overflowValues_[j + 1] = value;
    }
  }
}

int32_t DenseHll::serializedSize() const {
  return 1 /* type + version */
      + 1 /* indexBitLength */
      + 1 /* baseline */
      + (1 << indexBitLength_) / 2 /* buckets */
      + 2 /* overflow bucket count */
      + 2 * overflows_ /* overflow bucket indexes */
      + overflows_ /* overflow bucket values */;
}

// static
bool DenseHll::canDeserialize(const char* input) {
  return *reinterpret_cast<const int8_t*>(input) == kPrestoDenseV2;
}

// static
bool DenseHll::canDeserialize(const char* input, int size) {
  if (size < 5) {
    // Min serialized sparse HLL size is 5 bytes.
    return false;
  }

  common::InputByteStream stream(input);
  auto version = stream.read<int8_t>();
  if (kPrestoDenseV2 != version) {
    return false;
  }

  auto indexBitLength = stream.read<int8_t>();
  if (indexBitLength < 4 || indexBitLength > 16) {
    return false;
  }

  auto baseline = stream.read<int8_t>();

  // Min size with no overflow buckets/values.
  int minSizeNoOverflow = 5 + pow(2, (indexBitLength - 1));
  if (size < minSizeNoOverflow) {
    return false;
  }

  auto numBuckets = 1 << indexBitLength;
  const int8_t* deltas = stream.read<int8_t>(numBuckets / 2);
  auto overflows = stream.read<int16_t>();

  int sizeWithOverflow = minSizeNoOverflow + 2 * overflows + overflows;
  if (size < sizeWithOverflow) {
    return false;
  }

  const uint16_t* overflowBuckets =
      overflows ? stream.read<uint16_t>(overflows) : nullptr;
  const int8_t* overflowValues =
      overflows ? stream.read<int8_t>(overflows) : nullptr;

  auto hllView = DenseHllView{
      indexBitLength,
      baseline,
      deltas,
      overflows,
      overflowBuckets,
      overflowValues};

  for (int i = 0; i < numBuckets; i++) {
    int value = hllView.getValue(i);
    // Value is used to left shift 1L so value must be in [0,63].
    if (value < 0 || value > 63) {
      return false;
    }
  }

  return true;
}

// static
int8_t DenseHll::deserializeIndexBitLength(const char* input) {
  common::InputByteStream stream(input);
  stream.read<int8_t>();
  return stream.read<int8_t>();
}

// static
int32_t DenseHll::estimateInMemorySize(int8_t indexBitLength) {
  // Note: we don't take into account overflow entries since their number can
  // vary.
  return sizeof(indexBitLength_) + sizeof(baseline_) + sizeof(baselineCount_) +
      (1 << indexBitLength) / 2;
}

void DenseHll::serialize(char* output) {
  // sort overflow arrays to get consistent serialization for equivalent HLLs
  sortOverflows();

  common::OutputByteStream stream(output);
  stream.appendOne(kPrestoDenseV2);
  stream.appendOne(indexBitLength_);
  stream.appendOne(baseline_);
  stream.append(reinterpret_cast<const char*>(deltas_.data()), deltas_.size());
  stream.appendOne(overflows_);
  if (overflows_) {
    stream.append(
        reinterpret_cast<const char*>(overflowBuckets_.data()), overflows_ * 2);
    stream.append(
        reinterpret_cast<const char*>(overflowValues_.data()), overflows_);
  }
}

DenseHll::DenseHll(const char* serialized, HashStringAllocator* allocator)
    : deltas_{StlAllocator<int8_t>(allocator)},
      overflowBuckets_{StlAllocator<uint16_t>(allocator)},
      overflowValues_{StlAllocator<int8_t>(allocator)} {
  auto hll = deserialize(serialized);
  indexBitLength_ = hll.indexBitLength;
  baseline_ = hll.baseline;

  auto numBuckets = 1 << indexBitLength_;
  deltas_.resize(numBuckets / 2);
  std::copy(hll.deltas, hll.deltas + numBuckets / 2, deltas_.data());

  overflows_ = hll.overflows;
  if (overflows_) {
    overflowBuckets_.resize(overflows_);
    overflowValues_.resize(overflows_);
    std::copy(
        hll.overflowBuckets,
        hll.overflowBuckets + overflows_,
        overflowBuckets_.data());
    std::copy(
        hll.overflowValues,
        hll.overflowValues + overflows_,
        overflowValues_.data());
  }

  baselineCount_ = 0;
  for (int i = 0; i < numBuckets; i++) {
    if (getDelta(i) == 0) {
      baselineCount_++;
    }
  }
}

void DenseHll::mergeWith(const DenseHll& other) {
  VELOX_CHECK_EQ(
      indexBitLength_,
      other.indexBitLength_,
      "Cannot merge HLLs with different number of buckets");

  mergeWith(
      other.baseline_,
      other.deltas_.data(),
      other.overflows_,
      other.overflowBuckets_.data(),
      other.overflowValues_.data());
}

void DenseHll::mergeWith(const char* serialized) {
  common::InputByteStream stream(serialized);

  auto version = stream.read<int8_t>();
  VELOX_CHECK_EQ(kPrestoDenseV2, version);

  auto indexBitLength = stream.read<int8_t>();
  VELOX_CHECK_EQ(
      indexBitLength_,
      indexBitLength,
      "Cannot merge HLLs with different number of buckets");

  auto baseline = stream.read<int8_t>();

  auto numBuckets = 1 << indexBitLength_;
  auto deltas = stream.read<int8_t>(numBuckets / 2);
  auto overflows = stream.read<int16_t>();
  auto overflowBuckets = overflows ? stream.read<uint16_t>(overflows) : nullptr;
  auto overflowValues = overflows ? stream.read<int8_t>(overflows) : nullptr;
  mergeWith(baseline, deltas, overflows, overflowBuckets, overflowValues);
}

void DenseHll::mergeWith(
    int8_t otherBaseline,
    const int8_t* otherDeltas,
    int16_t otherOverflows,
    const uint16_t* otherOverflowBuckets,
    const int8_t* otherOverflowValues) {
  int8_t newBaseline = std::max(baseline_, otherBaseline);
  int32_t baselineCount = 0;

  int bucket = 0;
  for (int i = 0; i < deltas_.size(); i++) {
    int newSlot = 0;

    int8_t slot1 = deltas_[i];
    int8_t slot2 = otherDeltas[i];

    for (int shift = 4; shift >= 0; shift -= 4) {
      int8_t delta1 = (slot1 >> shift) & kBucketMask;
      int8_t delta2 = (slot2 >> shift) & kBucketMask;

      int8_t value1 = baseline_ + delta1;
      int8_t value2 = otherBaseline + delta2;

      int16_t overflowEntry = -1;
      if (delta1 == kMaxDelta) {
        overflowEntry = findOverflowEntry(bucket);
        if (overflowEntry != -1) {
          value1 += overflowValues_[overflowEntry];
        }
      }

      if (delta2 == kMaxDelta) {
        value2 += getOverflowImpl(
            bucket, otherOverflows, otherOverflowBuckets, otherOverflowValues);
      }

      int8_t newValue = std::max(value1, value2);
      int8_t newDelta = newValue - newBaseline;

      if (newDelta == 0) {
        baselineCount++;
      }

      newDelta = updateOverflow(bucket, overflowEntry, newDelta);

      newSlot <<= 4;
      newSlot |= newDelta;
      bucket++;
    }

    deltas_[i] = newSlot;
  }

  baseline_ = newBaseline;
  baselineCount_ = baselineCount;

  // All baseline values in one of the HLLs lost to the values
  // in the other HLL, so we need to adjust the final baseline.
  adjustBaselineIfNeeded();
}

int8_t
DenseHll::updateOverflow(int32_t index, int overflowEntry, int8_t delta) {
  if (delta > kMaxDelta) {
    if (overflowEntry != -1) {
      // update existing overflow
      overflowValues_[overflowEntry] = delta - kMaxDelta;
    } else {
      addOverflow(index, delta - kMaxDelta);
    }
    delta = kMaxDelta;
  } else if (overflowEntry != -1) {
    removeOverflow(overflowEntry);
  }

  return delta;
}

void DenseHll::addOverflow(int32_t index, int8_t overflow) {
  overflowBuckets_.resize(overflows_ + 1);
  overflowValues_.resize(overflows_ + 1);

  overflowBuckets_[overflows_] = index;
  overflowValues_[overflows_] = overflow;
  overflows_++;
}

void DenseHll::removeOverflow(int overflowEntry) {
  // Remove existing overflow.
  overflowBuckets_[overflowEntry] = overflowBuckets_[overflows_ - 1];
  overflowValues_[overflowEntry] = overflowValues_[overflows_ - 1];
  overflows_--;
}
} // namespace facebook::velox::common::hll
