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

#include <folly/Bits.h>

#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Doubles.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Portability.h"
#include "velox/common/memory/HashStringAllocator.h"

namespace facebook::velox::functions {

namespace qdigest {

constexpr double kZeroWeightThreshold = 1.0E-5;
constexpr double kUninitializedMaxError = 0.0;

/// Implementation of Q-Digest that matches Presto Java behavior. The
/// serialization format is same as Java. There is one performance improvement:
/// mergeSerialized(const char* serialized) allows merging another serialized
/// digest into the current one without having to deserialize it first. This
/// optimization allows us to perform the merge with one pass instead of two,
/// and skip memory allocation for storing the deserialized digest. The Java
/// behavior is the same as testingMerge.
///
/// Java implementation can be found at:
/// https://github.com/airlift/airlift/blob/57a1bb0182f6336cb03a365be018a73490d9b410/stats/src/main/java/io/airlift/stats/QuantileDigest.java
template <typename T, typename Allocator = StlAllocator<T>>
class QuantileDigest {
  static_assert(
      std::is_same_v<T, int64_t> || std::is_same_v<T, double> ||
      std::is_same_v<T, float>);

 public:
  explicit QuantileDigest(const Allocator& allocator, double maxError);

  QuantileDigest(const Allocator& allocator, const char* serialized);

  void setMaxError(double maxError);

  double getMaxError() const;

  void add(T value, double weight);

  void mergeSerialized(const char* other);

  double getCount() const;

  void scale(double scaleFactor);

  void compress();

  /// Estimate the values of the given quantiles and write the results in order
  /// directly into 'result'.
  void estimateQuantiles(const std::vector<double>& quantiles, T* result) const;

  T estimateQuantile(double quantile) const;

  int64_t serializedByteSize() const;

  int64_t serialize(char* out);

  T getMin() const;

  T getMax() const;

  // For testing only. Calling this method with 'other' being constructed from
  // QuantileDigest(const Allocator& allocator, const char* serialized) is
  // supposed to produce the same result as calling mergeSerialized() directly
  // on 'serialized'.
  void testingMerge(const QuantileDigest& other);

  // Returns nullopt when the digest is empty or the value is out of the
  // min/max range.
  std::optional<double> quantileAtValue(T value) const;

 private:
  using U = std::conditional_t<sizeof(T) == sizeof(int64_t), int64_t, int32_t>;

  template <typename V>
  using RebindAlloc =
      typename std::allocator_traits<Allocator>::template rebind_alloc<V>;

  int32_t calculateHeight(int32_t nodeCount);

  int32_t calculateParentLevel(U first, U second);

  int8_t calculateLevel(int8_t nodeStructure);

  int8_t calculateNodeStructure(int8_t level);

  void writeValue(U value, char*& out);

  void insert(U value, double count);

  void setChild(int32_t parent, U branch, int32_t child);

  int32_t makeSiblings(int32_t first, int32_t second);

  int32_t createLeaf(U value, double count);

  int32_t createNode(U value, int8_t level, double count);

  bool inSameSubtree(U bitsA, U bitsB, int32_t level);

  U preprocessByType(T value) const;

  T postprocessByType(U bits) const;

  U longToBits(U value) const;

  U bitsToLong(U bits) const;

  U getBranchMask(int8_t level);

  int32_t calculateCompressionFactor() const;

  int32_t tryRemove(int32_t node);

  void remove(int32_t node);

  void pushFree(int32_t node);

  int32_t popFree();

  template <typename Func>
  bool postOrderTraverse(
      int32_t node,
      Func callback,
      const std::vector<int32_t, RebindAlloc<int32_t>>& firstChildren,
      const std::vector<int32_t, RebindAlloc<int32_t>>& secondChildren) const {
    if (node == -1) {
      return false;
    } else {
      auto first = firstChildren[node];
      auto second = secondChildren[node];
      if (first != -1 &&
          !postOrderTraverse(first, callback, firstChildren, secondChildren)) {
        return false;
      } else {
        return second != -1 &&
                !postOrderTraverse(
                    second, callback, firstChildren, secondChildren)
            ? false
            : callback(node);
      }
    }
  }

  int32_t mergeRecursive(int32_t node, QuantileDigest other, int32_t otherNode);

  int32_t copyRecursive(QuantileDigest other, int32_t otherNode);

  class SerDe {
   public:
    static void readNode(
        const char*& nodeBegin,
        int8_t& nodeStructure,
        double& count,
        U& value);

    static void readMetadata(
        const char*& metadataBegin,
        int8_t& version,
        double& maxError,
        U& min,
        U& max,
        int32_t& nodeCount);

    static void
    writeNode(int8_t nodeStructure, double count, U value, char*& out);

    static void writeMetadata(
        int8_t version,
        double maxError,
        U min,
        U max,
        int32_t nodeCount,
        char*& out);

   private:
    static void readValue(const char*& in, U& value);

    static void writeValue(U value, char*& out);
  };

  // Merge the current subtree at 'node' with the other subtree ending at
  // 'otherNodeEnd'. This method returns a pair of {newNode, position}.
  // 'newNode' is the root node of the new subtree after the merge. 'position'
  // is the position in the serialized 'other' digest right before the nodes
  // having been read in this call. 'start' is the start position of the entire
  // serialization to be merged. This method checks that 'otherNodeEnd' must be
  // after 'start'.
  std::pair<int32_t, const char*> mergeSerializedRecursive(
      int32_t node,
      const char* start,
      const char* otherNodeEnd);

  // Copy the other subtree ending at 'otherNodeEnd' into the current subtree.
  // This method returns a pair of {newNode, position}. 'newNode' is the root
  // node of the copied subtree in the current digest. 'position' is the
  // position in the serialized 'other' digest right before the nodes having
  // been read in this call. 'start' is the start position of the entire
  // serialization to be merged. This method checks that 'otherNodeEnd' must be
  // after 'start'.
  std::pair<int32_t, const char*> copySerializedRecursive(
      const char* start,
      const char* otherNodeEnd);

  U lowerBound(int32_t node) const;

  U upperBound(int32_t node) const;

  bool validateDigest() const;

  double maxError_;
  double weightedCount_;
  U min_;
  U max_;
  int32_t root_;
  int32_t firstFree_;
  int32_t freeCount_;
  std::vector<double, RebindAlloc<double>> counts_;
  std::vector<int8_t, RebindAlloc<int8_t>> levels_;
  std::vector<U, RebindAlloc<U>> values_;
  std::vector<int32_t, RebindAlloc<int32_t>> lefts_;
  std::vector<int32_t, RebindAlloc<int32_t>> rights_;
};

namespace detail {
template <typename T>
void read(const char*& input, T& value) {
  value = folly::loadUnaligned<T>(input);
  input += sizeof(T);
}

template <typename T>
T read(const char*& input) {
  T value = folly::loadUnaligned<T>(input);
  input += sizeof(T);
  return value;
}

template <typename T>
void write(T value, char*& out) {
  folly::storeUnaligned(out, value);
  out += sizeof(T);
}
} // namespace detail

// static
template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::SerDe::readValue(const char*& in, U& value) {
  if constexpr (std::is_same_v<U, int64_t>) {
    detail::read<int64_t>(in, value);
  } else {
    int64_t bits;
    detail::read<int64_t>(in, bits);
    value = static_cast<U>(
        (bits ^ std::numeric_limits<int64_t>::min()) ^
        std::numeric_limits<int32_t>::min());
  }
}

// static
template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::SerDe::readNode(
    const char*& nodeBegin,
    int8_t& nodeStructure,
    double& count,
    U& value) {
  detail::read<int8_t>(nodeBegin, nodeStructure);
  detail::read<double>(nodeBegin, count);
  readValue(nodeBegin, value);
}

// static
template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::SerDe::readMetadata(
    const char*& metadataBegin,
    int8_t& version,
    double& maxError,
    U& min,
    U& max,
    int32_t& nodeCount) {
  detail::read<int8_t>(metadataBegin, version);

  detail::read<double>(metadataBegin, maxError);

  auto alpha = detail::read<double>(metadataBegin);
  VELOX_CHECK_EQ(alpha, 0.0);

  auto landmark = detail::read<int64_t>(metadataBegin);
  VELOX_CHECK_EQ(landmark, 0);

  int64_t data;
  detail::read<int64_t>(metadataBegin, data);
  min = static_cast<U>(data);
  detail::read<int64_t>(metadataBegin, data);
  max = static_cast<U>(data);

  nodeCount = detail::read<int32_t>(metadataBegin);
}

// static
template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::SerDe::writeValue(U value, char*& out) {
  if constexpr (std::is_same_v<U, int64_t>) {
    detail::write<int64_t>(value, out);
  } else {
    detail::write<int64_t>(
        (value ^ std::numeric_limits<int32_t>::min()) ^
            std::numeric_limits<int64_t>::min(),
        out);
  }
}

// static
template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::SerDe::writeNode(
    int8_t nodeStructure,
    double count,
    U value,
    char*& out) {
  detail::write<int8_t>(nodeStructure, out);
  detail::write<double>(count, out);
  writeValue(value, out);
}

// static
template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::SerDe::writeMetadata(
    int8_t version,
    double maxError,
    U min,
    U max,
    int32_t nodeCount,
    char*& out) {
  detail::write<int8_t>(version, out); // version
  detail::write<double>(maxError, out);
  detail::write<double>(0.0, out); // alpha
  detail::write<int64_t>(0, out); // landmarkInSeconds
  detail::write<int64_t>(static_cast<int64_t>(min), out);
  detail::write<int64_t>(static_cast<int64_t>(max), out);
  detail::write<int32_t>(nodeCount, out);
}

template <typename T, typename Allocator>
QuantileDigest<T, Allocator>::QuantileDigest(
    const Allocator& allocator,
    double maxError)
    : maxError_{maxError},
      weightedCount_{0},
      min_{std::numeric_limits<U>::max()},
      max_{std::numeric_limits<U>::min()},
      root_{-1},
      firstFree_{-1},
      freeCount_{0},
      counts_{RebindAlloc<double>(allocator)},
      levels_{RebindAlloc<int8_t>(allocator)},
      values_{RebindAlloc<U>(allocator)},
      lefts_{RebindAlloc<int32_t>(allocator)},
      rights_{RebindAlloc<int32_t>(allocator)} {}

template <typename T, typename Allocator>
QuantileDigest<T, Allocator>::QuantileDigest(
    const Allocator& allocator,
    const char* in)
    : weightedCount_{0},
      root_{-1},
      firstFree_{-1},
      freeCount_{0},
      counts_{RebindAlloc<double>(allocator)},
      levels_{RebindAlloc<int8_t>(allocator)},
      values_{RebindAlloc<U>(allocator)},
      lefts_{RebindAlloc<int32_t>(allocator)},
      rights_{RebindAlloc<int32_t>(allocator)} {
  int8_t version;
  int32_t nodeCount;
  SerDe::readMetadata(in, version, maxError_, min_, max_, nodeCount);
  VELOX_CHECK_EQ(version, 0);
  auto height = calculateHeight(nodeCount);

  counts_.resize(nodeCount, 0);
  levels_.resize(nodeCount, 0);
  values_.resize(nodeCount, 0);
  lefts_.resize(nodeCount, -1);
  rights_.resize(nodeCount, -1);

  std::vector<int32_t, RebindAlloc<int32_t>> stack(
      height, RebindAlloc<int32_t>(allocator));
  int32_t top = -1;
  for (auto i = 0; i < nodeCount; ++i) {
    int8_t nodeStructure;
    SerDe::readNode(in, nodeStructure, counts_[i], values_[i]);
    weightedCount_ += counts_[i];

    bool hasRight = (nodeStructure & 2) != 0;
    bool hasLeft = (nodeStructure & 1) != 0;
    levels_[i] = calculateLevel(nodeStructure);
    if (hasLeft || hasRight) {
      levels_[i]++;
    }

    if (hasRight) {
      rights_[i] = stack[top--];
    } else {
      rights_[i] = -1;
    }
    if (hasLeft) {
      lefts_[i] = stack[top--];
    } else {
      lefts_[i] = -1;
    }

    ++top;
    stack[top] = i;
  }
  VELOX_CHECK(
      nodeCount == 0 || top == 0,
      "Tree is corrupted. Expected a single root node");
  root_ = nodeCount - 1;
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::setMaxError(double maxError) {
  maxError_ = maxError;
}

template <typename T, typename Allocator>
double QuantileDigest<T, Allocator>::getMaxError() const {
  return maxError_;
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::calculateHeight(int32_t nodeCount) {
  int32_t height;
  if constexpr (std::is_same_v<U, int64_t>) {
    height = static_cast<int32_t>(64 - count_leading_zeros(min_ ^ max_) + 1);
    VELOX_CHECK(
        height >= 64 || static_cast<int64_t>(nodeCount) <= (1L << height) - 1L,
        "Too many nodes in deserialized tree. Possible corruption");
  } else {
    height =
        static_cast<int32_t>(32 - count_leading_zeros_32bits(min_ ^ max_) + 1);
    VELOX_CHECK(
        height >= 32 || static_cast<int64_t>(nodeCount) <= (1L << height) - 1L,
        "Too many nodes in deserialized tree. Possible corruption");
  }
  return height;
}

template <typename T, typename Allocator>
int8_t QuantileDigest<T, Allocator>::calculateLevel(int8_t nodeStructure) {
  auto level =
      static_cast<int8_t>(static_cast<uint8_t>(nodeStructure) >> 2 & 63);
  if constexpr (std::is_same_v<U, int32_t>) {
    level = (level == 64) ? 32 : level;
  }
  return level;
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::calculateParentLevel(U first, U second) {
  int32_t parentLevel;
  if constexpr (std::is_same_v<U, int64_t>) {
    parentLevel =
        static_cast<int32_t>(64 - count_leading_zeros(first ^ second));
  } else {
    parentLevel =
        static_cast<int32_t>(32 - count_leading_zeros_32bits(first ^ second));
  }
  return parentLevel;
}

template <typename T, typename Allocator>
int8_t QuantileDigest<T, Allocator>::calculateNodeStructure(int8_t level) {
  int8_t nodeStructure;
  if constexpr (std::is_same_v<U, int64_t>) {
    nodeStructure = static_cast<int8_t>(std::max(level - 1, 0) << 2);
  } else {
    nodeStructure =
        static_cast<int8_t>(std::max((level == 32 ? 64 : level) - 1, 0) << 2);
  }
  return nodeStructure;
}

template <typename T, typename Allocator>
double QuantileDigest<T, Allocator>::getCount() const {
  return weightedCount_;
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::scale(double scaleFactor) {
  VELOX_USER_CHECK(scaleFactor > 0.0, "Scale factor should be positive.");
  for (auto i = 0; i < counts_.size(); ++i) {
    counts_[i] *= scaleFactor;
  }

  weightedCount_ *= scaleFactor;
  compress();
}

template <typename T, typename Allocator>
typename QuantileDigest<T, Allocator>::U
QuantileDigest<T, Allocator>::preprocessByType(T value) const {
  if constexpr (std::is_same_v<T, int64_t>) {
    return value;
  }
  if constexpr (std::is_same_v<T, double>) {
    auto bits = *reinterpret_cast<int64_t*>(&value);
    return bits ^ ((bits >> 63) & std::numeric_limits<int64_t>::max());
  } else {
    auto bits = *reinterpret_cast<int32_t*>(&value);
    return bits ^ ((bits >> 31) & std::numeric_limits<int32_t>::max());
  }
}

template <typename T, typename Allocator>
T QuantileDigest<T, Allocator>::postprocessByType(U bits) const {
  if constexpr (std::is_same_v<T, int64_t>) {
    return bits;
  } else if constexpr (std::is_same_v<T, double>) {
    bits = bits ^ ((bits >> 63) & std::numeric_limits<int64_t>::max());
    return *reinterpret_cast<double*>(&bits);
  } else {
    bits = bits ^ ((bits >> 31) & std::numeric_limits<int32_t>::max());
    return *reinterpret_cast<float*>(&bits);
  }
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::add(T value, double weight) {
  VELOX_DCHECK_NE(maxError_, kUninitializedMaxError);
  VELOX_USER_CHECK(weight > 0.0, "weight must be > 0");
  bool needsCompression{false};
  auto processedValue = preprocessByType(value);
  max_ = std::max(max_, processedValue);
  min_ = std::min(min_, processedValue);
  auto previousCount = weightedCount_;
  insert(longToBits(processedValue), weight);
  auto compressionFactor = calculateCompressionFactor();

  VELOX_USER_CHECK_LT(
      weightedCount_,
      kMaxDoubleBelowInt64Max,
      "Weighted count in digest is too large: {}",
      weightedCount_);
  if (needsCompression ||
      checkedDivide(
          static_cast<int64_t>(previousCount),
          static_cast<int64_t>(compressionFactor)) !=
          checkedDivide(
              static_cast<int64_t>(weightedCount_),
              static_cast<int64_t>(compressionFactor))) {
    compress();
  }
}

template <typename T, typename Allocator>
typename QuantileDigest<T, Allocator>::U
QuantileDigest<T, Allocator>::longToBits(U value) const {
  return value ^ std::numeric_limits<U>::min();
}

template <typename T, typename Allocator>
typename QuantileDigest<T, Allocator>::U
QuantileDigest<T, Allocator>::bitsToLong(U bits) const {
  return bits ^ std::numeric_limits<U>::min();
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::calculateCompressionFactor() const {
  if constexpr (std::is_same_v<U, int64_t>) {
    return root_ == -1
        ? 1
        : std::max(
              static_cast<int>(
                  static_cast<double>(levels_[root_] + 1) / maxError_),
              1);
  } else {
    return root_ == -1
        ? 1
        : std::max(
              static_cast<int>(
                  static_cast<double>(
                      (levels_[root_] == 32 ? 64 : levels_[root_]) + 1) /
                  maxError_),
              1);
  }
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::insert(U value, double count) {
  if (count < qdigest::kZeroWeightThreshold) {
    return;
  }
  U lastBranch = 0;
  int32_t parent = -1;
  int32_t current = root_;
  while (current != -1) {
    auto currentValue = values_[current];
    auto currentLevel = levels_[current];
    if (!inSameSubtree(value, currentValue, currentLevel)) {
      setChild(
          parent, lastBranch, makeSiblings(current, createLeaf(value, count)));
      return;
    }

    if (currentLevel == 0 && currentValue == value) {
      counts_[current] += count;
      weightedCount_ += count;
      return;
    }

    U branch = value & getBranchMask(currentLevel);
    parent = current;
    lastBranch = branch;
    if (branch == 0) {
      current = lefts_[current];
    } else {
      current = rights_[current];
    }
  }
  setChild(parent, lastBranch, createLeaf(value, count));
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::createLeaf(U value, double count) {
  return createNode(value, 0, count);
}

template <typename T, typename Allocator>
int32_t
QuantileDigest<T, Allocator>::createNode(U value, int8_t level, double count) {
  auto node = popFree();
  weightedCount_ += count;
  if (node == -1) {
    node = static_cast<int32_t>(counts_.size());
    counts_.push_back(count);
    levels_.push_back(level);
    values_.push_back(value);
    lefts_.push_back(-1);
    rights_.push_back(-1);
  } else {
    counts_[node] = count;
    levels_[node] = level;
    values_[node] = value;
    lefts_[node] = -1;
    rights_[node] = -1;
  }
  return node;
}

template <typename T, typename Allocator>
bool QuantileDigest<T, Allocator>::inSameSubtree(
    U bitsA,
    U bitsB,
    int32_t level) {
  if constexpr (std::is_same_v<U, int64_t>) {
    return (level == 64) ||
        ((static_cast<uint64_t>(bitsA) >> level) ==
         (static_cast<uint64_t>(bitsB) >> level));
  } else {
    return (level == 32) ||
        ((static_cast<uint32_t>(bitsA) >> level) ==
         (static_cast<uint32_t>(bitsB) >> level));
  }
}

template <typename T, typename Allocator>
typename QuantileDigest<T, Allocator>::U
QuantileDigest<T, Allocator>::getBranchMask(int8_t level) {
  return static_cast<U>(1) << (level - 1);
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::makeSiblings(
    int32_t first,
    int32_t second) {
  auto firstValue = values_[first];
  auto secondValue = values_[second];
  auto parentLevel = calculateParentLevel(firstValue, secondValue);
  auto parent = createNode(firstValue, parentLevel, 0.0);
  auto branch = firstValue & getBranchMask(levels_[parent]);
  if (branch == 0) {
    lefts_[parent] = first;
    rights_[parent] = second;
  } else {
    lefts_[parent] = second;
    rights_[parent] = first;
  }
  return parent;
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::setChild(
    int32_t parent,
    U branch,
    int32_t child) {
  if (parent == -1) {
    root_ = child;
  } else if (branch == 0) {
    lefts_[parent] = child;
  } else {
    rights_[parent] = child;
  }
}

template <typename T, typename Allocator>
bool QuantileDigest<T, Allocator>::validateDigest() const {
  std::unordered_set<
      int32_t,
      std::hash<int32_t>,
      std::equal_to<int32_t>,
      RebindAlloc<int32_t>>
      free(lefts_.get_allocator());
  auto iterator = firstFree_;
  while (iterator != -1) {
    free.insert(iterator);
    iterator = lefts_[iterator];
  }
  std::vector<bool, RebindAlloc<unsigned long>> visited(
      lefts_.size(), false, RebindAlloc<unsigned long>(lefts_.get_allocator()));

  // Check that visited nodes are not in the free list and are visited only
  // once.
  postOrderTraverse(
      root_,
      [&free, &visited](int32_t node) {
        VELOX_CHECK_EQ(free.count(node), 0);
        VELOX_CHECK_EQ(bool(visited[node]), false);
        visited[node] = true;

        return true;
      },
      lefts_,
      rights_);
  // Check that all nodes that are not in the free list are visited.
  for (auto i = 0; i < visited.size(); ++i) {
    VELOX_CHECK(visited[i] == true || free.count(i) == 1);
  }
  return true;
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::compress() {
  double bound = std::floor(
      weightedCount_ / static_cast<double>(calculateCompressionFactor()));
  postOrderTraverse(
      root_,
      [this, bound](int32_t node) mutable {
        auto left = lefts_[node];
        auto right = rights_[node];
        if (left == -1 && right == -1) {
          return true;
        } else {
          double leftCount = (left == -1) ? 0.0 : counts_[left];
          double rightCount = (right == -1) ? 0.0 : counts_[right];
          bool shouldCompress =
              (counts_[node] + leftCount + rightCount) < bound;
          if (left != -1 &&
              (shouldCompress || leftCount < qdigest::kZeroWeightThreshold)) {
            lefts_[node] = tryRemove(left);
            counts_[node] += leftCount;
          }

          if (right != -1 &&
              (shouldCompress || rightCount < qdigest::kZeroWeightThreshold)) {
            rights_[node] = tryRemove(right);
            counts_[node] += rightCount;
          }

          return true;
        }
      },
      lefts_,
      rights_);
  if (root_ != -1 && counts_[root_] < qdigest::kZeroWeightThreshold) {
    root_ = tryRemove(root_);
  }
  VELOX_DCHECK(validateDigest());
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::tryRemove(int32_t node) {
  VELOX_USER_CHECK_NE(node, -1, "node is -1");
  auto left = lefts_[node];
  auto right = rights_[node];
  if (left == -1 && right == -1) {
    remove(node);
    return -1;
  } else if (left != -1 && right != -1) {
    counts_[node] = 0.0;
    return node;
  } else {
    remove(node);
    return left != -1 ? left : right;
  }
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::remove(int32_t node) {
  if (node == counts_.size() - 1) {
    counts_.pop_back();
    levels_.pop_back();
    values_.pop_back();
    lefts_.pop_back();
    rights_.pop_back();
  } else {
    pushFree(node);
  }
  if (node == root_) {
    root_ = -1;
  }
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::pushFree(int32_t node) {
  lefts_[node] = firstFree_;
  firstFree_ = node;
  ++freeCount_;
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::popFree() {
  auto node = firstFree_;
  if (node == -1) {
    return node;
  } else {
    firstFree_ = lefts_[firstFree_];
    --freeCount_;
    return node;
  }
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::testingMerge(
    const QuantileDigest<T, Allocator>& other) {
  if (maxError_ == kUninitializedMaxError) {
    maxError_ = other.getMaxError();
  } else {
    VELOX_CHECK_EQ(other.getMaxError(), maxError_);
  }
  root_ = mergeRecursive(root_, other, other.root_);
  max_ = std::max(max_, other.max_);
  min_ = std::min(min_, other.min_);
  compress();
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::mergeSerialized(const char* other) {
  int8_t version;
  double maxError;
  U min;
  U max;
  int32_t nodeCount;
  SerDe::readMetadata(other, version, maxError, min, max, nodeCount);
  VELOX_CHECK_EQ(version, 0);
  if (maxError_ == kUninitializedMaxError) {
    maxError_ = maxError;
  } else {
    VELOX_CHECK_EQ(maxError, maxError_);
  }

  if (nodeCount == 0) {
    return;
  }

  VELOX_CHECK_GT(nodeCount, 0, "nodeCount is negative");
  auto size = nodeCount * (sizeof(int8_t) + sizeof(double) + sizeof(int64_t));

  const char* pos;
  std::tie(root_, pos) = mergeSerializedRecursive(root_, other, other + size);
  max_ = std::max(max_, max);
  min_ = std::min(min_, min);
  VELOX_DCHECK(validateDigest());

  compress();
}

template <typename T, typename Allocator>
std::pair<int32_t, const char*>
QuantileDigest<T, Allocator>::mergeSerializedRecursive(
    int32_t node,
    const char* start,
    const char* otherNodeEnd) {
  VELOX_CHECK_GT(
      otherNodeEnd - start,
      0,
      "otherNodeEnd is not after start. Serialization is likely corrupted.");
  const char* nodeBegin =
      otherNodeEnd - sizeof(int8_t) - sizeof(double) - sizeof(int64_t);
  auto lastChild = nodeBegin;

  int8_t nodeStructure;
  double count;
  U value;
  SerDe::readNode(nodeBegin, nodeStructure, count, value);

  bool hasRight = (nodeStructure & 2) != 0;
  bool hasLeft = (nodeStructure & 1) != 0;
  int8_t level = calculateLevel(nodeStructure);
  if (hasLeft || hasRight) {
    level++;
  }

  if (node == -1) {
    return copySerializedRecursive(start, otherNodeEnd);
  } else if (!inSameSubtree(
                 values_[node], value, std::max(levels_[node], level))) {
    auto [newNode, position] = copySerializedRecursive(start, otherNodeEnd);
    return std::make_pair(makeSiblings(node, newNode), position);
  } else {
    if (levels_[node] > level) {
      auto branch = value & getBranchMask(levels_[node]);
      if (branch == 0) {
        auto [left, pos] =
            mergeSerializedRecursive(lefts_[node], start, otherNodeEnd);
        lefts_[node] = left;
        return std::make_pair(node, pos);
      } else {
        auto [left, pos] =
            mergeSerializedRecursive(rights_[node], start, otherNodeEnd);
        rights_[node] = left;
        return std::make_pair(node, pos);
      }
    } else if (levels_[node] < level) {
      auto branch = values_[node] & getBranchMask(level);
      int32_t left = node, right = node;
      auto position = lastChild;
      if (branch == 0) {
        if (hasRight) {
          std::tie(right, position) = copySerializedRecursive(start, position);
        } else {
          right = -1;
        }
        if (hasLeft) {
          std::tie(left, position) =
              mergeSerializedRecursive(node, start, position);
        }
      } else {
        if (hasRight) {
          std::tie(right, position) =
              mergeSerializedRecursive(node, start, position);
        }
        if (hasLeft) {
          std::tie(left, position) = copySerializedRecursive(start, position);
        } else {
          left = -1;
        }
      }

      auto result = createNode(value, level, count);
      lefts_[result] = left;
      rights_[result] = right;
      return std::make_pair(result, position);
    } else {
      weightedCount_ += count;
      counts_[node] += count;
      auto position = lastChild;
      int32_t left = lefts_[node];
      int32_t right = rights_[node];
      if (hasRight) {
        std::tie(right, position) =
            mergeSerializedRecursive(rights_[node], start, position);
      }
      if (hasLeft) {
        std::tie(left, position) =
            mergeSerializedRecursive(lefts_[node], start, position);
      }

      lefts_[node] = left;
      rights_[node] = right;
      return std::make_pair(node, position);
    }
  }
}

template <typename T, typename Allocator>
std::pair<int32_t, const char*>
QuantileDigest<T, Allocator>::copySerializedRecursive(
    const char* start,
    const char* otherNodeEnd) {
  VELOX_CHECK_GT(
      otherNodeEnd - start,
      0,
      "otherNodeEnd is not after start. Serialization is likely corrupted.");
  const char* nodeBegin =
      otherNodeEnd - sizeof(int8_t) - sizeof(double) - sizeof(int64_t);
  auto lastChild = nodeBegin;

  int8_t nodeStructure;
  double count;
  U value;
  SerDe::readNode(nodeBegin, nodeStructure, count, value);

  bool hasRight = (nodeStructure & 2) != 0;
  bool hasLeft = (nodeStructure & 1) != 0;
  int8_t level = calculateLevel(nodeStructure);
  if (hasLeft || hasRight) {
    level++;
  }

  auto node = createNode(value, level, count);
  const char* position = lastChild;
  rights_[node] = -1;
  lefts_[node] = -1;
  if (hasRight) {
    std::tie(rights_[node], position) =
        copySerializedRecursive(start, position);
  }
  if (hasLeft) {
    std::tie(lefts_[node], position) = copySerializedRecursive(start, position);
  }
  return std::make_pair(node, position);
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::mergeRecursive(
    int32_t node,
    QuantileDigest<T, Allocator> other,
    int32_t otherNode) {
  if (otherNode == -1) {
    return node;
  } else if (node == -1) {
    return copyRecursive(other, otherNode);
  } else if (!inSameSubtree(
                 values_[node],
                 other.values_[otherNode],
                 std::max(levels_[node], other.levels_[otherNode]))) {
    return makeSiblings(node, copyRecursive(other, otherNode));
  } else {
    if (levels_[node] > other.levels_[otherNode]) {
      int32_t left;
      auto branch = other.values_[otherNode] & getBranchMask(levels_[node]);
      if (branch == 0) {
        left = mergeRecursive(lefts_[node], other, otherNode);
        lefts_[node] = left;
      } else {
        left = mergeRecursive(rights_[node], other, otherNode);
        rights_[node] = left;
      }
      return node;
    } else if (levels_[node] < other.levels_[otherNode]) {
      auto branch = values_[node] & getBranchMask(other.levels_[otherNode]);
      int32_t left, right;
      if (branch == 0) {
        left = mergeRecursive(node, other, other.lefts_[otherNode]);
        right = copyRecursive(other, other.rights_[otherNode]);
      } else {
        left = copyRecursive(other, other.lefts_[otherNode]);
        right = mergeRecursive(node, other, other.rights_[otherNode]);
      }

      auto result = createNode(
          other.values_[otherNode],
          other.levels_[otherNode],
          other.counts_[otherNode]);
      lefts_[result] = left;
      rights_[result] = right;
      return result;
    } else {
      weightedCount_ += other.counts_[otherNode];
      counts_[node] += other.counts_[otherNode];
      auto left = mergeRecursive(lefts_[node], other, other.lefts_[otherNode]);
      auto right =
          mergeRecursive(rights_[node], other, other.rights_[otherNode]);
      lefts_[node] = left;
      rights_[node] = right;
      return node;
    }
  }
}

template <typename T, typename Allocator>
int32_t QuantileDigest<T, Allocator>::copyRecursive(
    QuantileDigest other,
    int32_t otherNode) {
  if (otherNode == -1) {
    return otherNode;
  } else {
    auto node = createNode(
        other.values_[otherNode],
        other.levels_[otherNode],
        other.counts_[otherNode]);
    if (other.lefts_[otherNode] != -1) {
      lefts_[node] = copyRecursive(other, other.lefts_[otherNode]);
    }
    if (other.rights_[otherNode] != -1) {
      rights_[node] = copyRecursive(other, other.rights_[otherNode]);
    }
    return node;
  }
}

inline bool validateQuantiles(const std::vector<double>& quantiles) {
  VELOX_CHECK(!quantiles.empty());
  VELOX_USER_CHECK_GE(quantiles[0], 0.0);
  VELOX_USER_CHECK_LE(quantiles[0], 1.0);
  for (auto i = 1; i < quantiles.size(); ++i) {
    VELOX_USER_CHECK_GE(quantiles[i], quantiles[i - 1]);
    VELOX_USER_CHECK_GE(quantiles[i], 0.0);
    VELOX_USER_CHECK_LE(quantiles[i], 1.0);
  }
  return true;
}

template <typename T, typename Allocator>
void QuantileDigest<T, Allocator>::estimateQuantiles(
    const std::vector<double>& quantiles,
    T* result) const {
  VELOX_DCHECK(validateQuantiles(quantiles));
  int i = -1;
  double sum = 0.0;
  postOrderTraverse(
      root_,
      [this, &result, &quantiles, &i, &sum](int32_t node) {
        sum += counts_[node];
        while (i + 1 < quantiles.size() &&
               sum > quantiles[i + 1] * weightedCount_) {
          result[i + 1] = (postprocessByType(std::min(upperBound(node), max_)));
          i++;
        }
        return i < static_cast<int64_t>(quantiles.size());
      },
      lefts_,
      rights_);
  for (; i + 1 < quantiles.size(); ++i) {
    result[i + 1] = (postprocessByType(max_));
  }
}

template <typename T, typename Allocator>
T QuantileDigest<T, Allocator>::estimateQuantile(double quantile) const {
  T result;
  estimateQuantiles({quantile}, &result);
  return result;
}

template <typename T, typename Allocator>
T QuantileDigest<T, Allocator>::getMin() const {
  T result = std::numeric_limits<T>::min();
  postOrderTraverse(
      root_,
      [this, &result](int32_t node) {
        if (counts_[node] >= qdigest::kZeroWeightThreshold) {
          result = postprocessByType(lowerBound(node));
          return false;
        } else {
          return true;
        }
      },
      lefts_,
      rights_);
  return std::max(postprocessByType(min_), result);
}

template <typename T, typename Allocator>
T QuantileDigest<T, Allocator>::getMax() const {
  T result = std::numeric_limits<T>::max();
  postOrderTraverse(
      root_,
      [this, &result](int32_t node) {
        if (counts_[node] >= qdigest::kZeroWeightThreshold) {
          result = postprocessByType(upperBound(node));
          return false;
        } else {
          return true;
        }
      },
      rights_,
      lefts_);
  return std::min(postprocessByType(max_), result);
}

template <typename T, typename Allocator>
typename QuantileDigest<T, Allocator>::U
QuantileDigest<T, Allocator>::lowerBound(int32_t node) const {
  if constexpr (std::is_same_v<U, int64_t>) {
    uint64_t mask = 0L;
    if (levels_[node] > 0) {
      mask = static_cast<uint64_t>(-1L) >> (64 - levels_[node]);
    }
    return bitsToLong(values_[node] & ~mask);
  } else {
    uint32_t mask = 0;
    if (levels_[node] > 0) {
      mask = static_cast<uint32_t>(-1L) >> (32 - levels_[node]);
    }
    return bitsToLong(values_[node] & ~mask);
  }
}

template <typename T, typename Allocator>
typename QuantileDigest<T, Allocator>::U
QuantileDigest<T, Allocator>::upperBound(int32_t node) const {
  if constexpr (std::is_same_v<U, int64_t>) {
    uint64_t mask = 0L;
    if (levels_[node] > 0) {
      mask = static_cast<uint64_t>(-1L) >> (64 - levels_[node]);
    }
    return bitsToLong(values_[node] | mask);
  } else {
    uint32_t mask = 0L;
    if (levels_[node] > 0) {
      mask = static_cast<uint32_t>(-1) >> (32 - levels_[node]);
    }
    return bitsToLong(values_[node] | mask);
  }
}

template <typename T, typename Allocator>
int64_t QuantileDigest<T, Allocator>::serializedByteSize() const {
  auto nodeCount = counts_.size() - freeCount_;
  return /*version*/ sizeof(char) + sizeof(maxError_) +
      /*alpha*/ sizeof(double) + /*landmarkInSeconds*/ sizeof(int64_t) +
      /*min*/ sizeof(int64_t) + /*max*/ sizeof(int64_t) +
      /*nodeCount*/ sizeof(int32_t) +
      (sizeof(typename decltype(counts_)::value_type) * nodeCount) +
      (sizeof(typename decltype(levels_)::value_type) * nodeCount) +
      /*values*/ (sizeof(int64_t) * nodeCount);
}

template <typename T, typename Allocator>
int64_t QuantileDigest<T, Allocator>::serialize(char* out) {
  VELOX_DCHECK(validateDigest());
  compress();
  const char* outStart = out;
  SerDe::writeMetadata(
      0,
      maxError_,
      min_,
      max_,
      static_cast<int32_t>(counts_.size() - freeCount_),
      out);
  postOrderTraverse(
      root_,
      [&](int32_t node) {
        auto nodeStructure = calculateNodeStructure(levels_[node]);
        if (lefts_[node] != -1) {
          nodeStructure = static_cast<int8_t>(nodeStructure | 1);
        }
        if (rights_[node] != -1) {
          nodeStructure = static_cast<int8_t>(nodeStructure | 2);
        }
        SerDe::writeNode(nodeStructure, counts_[node], values_[node], out);

        return true;
      },
      lefts_,
      rights_);
  VELOX_CHECK_EQ(out - outStart, serializedByteSize());
  return out - outStart;
}

template <typename T, typename Allocator>
std::optional<double> QuantileDigest<T, Allocator>::quantileAtValue(
    T value) const {
  if (weightedCount_ == 0 || root_ == -1) {
    return std::nullopt;
  }

  auto sortableValue = preprocessByType(value);
  if (sortableValue > preprocessByType(getMax()) ||
      sortableValue < preprocessByType(getMin())) {
    return std::nullopt;
  }

  double bucketCount = 0.0;
  postOrderTraverse(
      root_,
      [this, sortableValue, &bucketCount](int32_t node) {
        if (upperBound(node) >= sortableValue) {
          return false;
        }
        bucketCount += counts_[node];
        return true;
      },
      lefts_,
      rights_);
  return bucketCount / weightedCount_;
}

} // namespace qdigest

} // namespace facebook::velox::functions
