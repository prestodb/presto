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

#include <type_traits>

#include "velox/expression/ComplexViewTypes.h"
#include "velox/functions/Udf.h"
#include "velox/functions/lib/CheckedArithmetic.h"
#include "velox/type/Conversions.h"

namespace facebook::velox::functions {

template <typename TExecCtx, bool isMax>
struct ArrayMinMaxFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  static constexpr int32_t reuse_strings_from_arg = 0;

  template <typename T>
  void update(T& currentValue, const T& candidateValue) {
    if constexpr (isMax) {
      if (candidateValue > currentValue) {
        currentValue = candidateValue;
      }
    } else {
      if (candidateValue < currentValue) {
        currentValue = candidateValue;
      }
    }
  }

  template <typename TReturn, typename TInput>
  void assign(TReturn& out, const TInput& value) {
    out = value;
  }

  void assign(out_type<Varchar>& out, const arg_type<Varchar>& value) {
    out.setNoCopy(value);
  }

  template <typename TReturn, typename TInput>
  bool callForFloatOrDouble(TReturn& out, const TInput& array) {
    bool hasNull = false;
    auto it = array.begin();

    // Find the first non-null item (if any)
    while (it != array.end()) {
      if (it->has_value()) {
        break;
      }

      hasNull = true;
      ++it;
    }

    // Return false if end of array is reached without finding a non-null item.
    if (it == array.end()) {
      return false;
    }

    // If first non-null item is NAN, return immediately.
    auto currentValue = it->value();
    if (std::isnan(currentValue)) {
      assign(out, currentValue);
      return true;
    }

    ++it;
    while (it != array.end()) {
      if (it->has_value()) {
        auto newValue = it->value();
        if (std::isnan(newValue)) {
          assign(out, newValue);
          return true;
        }
        update(currentValue, newValue);
      } else {
        hasNull = true;
      }
      ++it;
    }

    // If we found a null, return false. Note that, if we found
    // a NAN, the function will return at earlier stage as soon as
    // a NAN is observed.
    if (hasNull) {
      return false;
    }

    assign(out, currentValue);
    return true;
  }

  template <typename TReturn, typename TInput>
  FOLLY_ALWAYS_INLINE bool call(TReturn& out, const TInput& array) {
    // Result is null if array is empty.
    if (array.size() == 0) {
      return false;
    }

    if constexpr (
        std::is_same_v<TReturn, float> || std::is_same_v<TReturn, double>) {
      return callForFloatOrDouble(out, array);
    }

    if (!array.mayHaveNulls()) {
      // Input array does not have nulls.
      auto currentValue = *array[0];
      for (auto i = 1; i < array.size(); i++) {
        update(currentValue, array[i].value());
      }
      assign(out, currentValue);
      return true;
    }

    auto it = array.begin();
    // Result is null if any element is null.
    if (!it->has_value()) {
      return false;
    }

    auto currentValue = it->value();
    ++it;
    while (it != array.end()) {
      if (!it->has_value()) {
        return false;
      }
      update(currentValue, it->value());
      ++it;
    }

    assign(out, currentValue);
    return true;
  }

  bool compare(
      exec::GenericView currentValue,
      exec::GenericView candidateValue) {
    static constexpr CompareFlags kFlags = {
        .nullHandlingMode =
            CompareFlags::NullHandlingMode::kNullAsIndeterminate};

    // We'll either get a result or throw.
    auto compareResult = candidateValue.compare(currentValue, kFlags).value();
    if constexpr (isMax) {
      return compareResult > 0;
    } else {
      return compareResult < 0;
    }
  }

  bool call(
      out_type<Orderable<T1>>& out,
      const arg_type<Array<Orderable<T1>>>& array) {
    // Result is null if array is empty.
    if (array.size() == 0) {
      return false;
    }

    // Result is null if any element is null.
    if (!array[0].has_value()) {
      return false;
    }

    int currentIndex = 0;
    for (auto i = 1; i < array.size(); i++) {
      if (!array[i].has_value()) {
        return false;
      }

      auto currentValue = array[currentIndex].value();
      auto candidateValue = array[i].value();
      if (compare(currentValue, candidateValue)) {
        currentIndex = i;
      }
    }
    out.copy_from(array[currentIndex].value());
    return true;
  }
};

template <typename TExecCtx>
struct ArrayMinFunction : public ArrayMinMaxFunction<TExecCtx, false> {};

template <typename TExecCtx>
struct ArrayMaxFunction : public ArrayMinMaxFunction<TExecCtx, true> {};

template <typename TExecCtx, typename T>
struct ArrayJoinFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  template <typename C>
  void writeValue(out_type<velox::Varchar>& result, const C& value) {
    result +=
        util::Converter<TypeKind::VARCHAR, void, util::DefaultCastPolicy>::cast(
            value);
  }

  template <typename C>
  void writeOutput(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Varchar>& delim,
      const C& value,
      bool& firstNonNull) {
    if (!firstNonNull) {
      writeValue(result, delim);
    }
    writeValue(result, value);
    firstNonNull = false;
  }

  void createOutputString(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Array<T>>& inputArray,
      const arg_type<velox::Varchar>& delim,
      std::optional<std::string> nullReplacement = std::nullopt) {
    bool firstNonNull = true;
    if (inputArray.size() == 0) {
      return;
    }

    for (const auto& entry : inputArray) {
      if (entry.has_value()) {
        writeOutput(result, delim, entry.value(), firstNonNull);
      } else if (nullReplacement.has_value()) {
        writeOutput(result, delim, nullReplacement.value(), firstNonNull);
      }
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Array<T>>& inputArray,
      const arg_type<velox::Varchar>& delim) {
    createOutputString(result, inputArray, delim);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Array<T>>& inputArray,
      const arg_type<velox::Varchar>& delim,
      const arg_type<velox::Varchar>& nullReplacement) {
    createOutputString(result, inputArray, delim, nullReplacement.getString());
    return true;
  }
};

/// Function Signature: combinations(array(T), n) -> array(array(T))
/// Returns n-element combinations of the input array. If the input array has no
/// duplicates, combinations returns n-element subsets. Order of subgroup is
/// deterministic but unspecified. Order of elements within a subgroup are
/// deterministic but unspecified. n must not be greater than 5, and the total
/// size of subgroups generated must be smaller than 100000.
template <typename TExecParams, typename T>
struct CombinationsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  static constexpr int32_t kMaxCombinationSize = 5;
  static constexpr int64_t kMaxNumberOfCombinations = 100000;
  static constexpr int32_t reuse_strings_from_arg = 0;

  int64_t calculateCombinationCount(
      int64_t inputArraySize,
      int64_t combinationSize) {
    int64_t combinationCount = 1;
    for (int i = 1;
         i <= combinationSize && combinationCount <= kMaxNumberOfCombinations;
         i++, inputArraySize--) {
      combinationCount = (combinationCount * inputArraySize) / i;
    }
    return combinationCount;
  }

  void resetCombination(std::vector<int>& combination, int to) {
    std::iota(combination.begin(), combination.begin() + to, 0);
  }

  std::vector<int> firstCombination(int64_t size) {
    std::vector<int> combination(size, 0);
    std::iota(combination.begin(), combination.end(), 0);
    return combination;
  }

  bool nextCombination(std::vector<int>& combination, int64_t inputArraySize) {
    for (int i = 0; i < combination.size() - 1; i++) {
      if (combination[i] + 1 < combination[i + 1]) {
        combination[i]++;
        resetCombination(combination, i);
        return true;
      }
    }
    if (combination.size() > 0 && combination.back() + 1 < inputArraySize) {
      combination.back()++;
      resetCombination(combination, combination.size() - 1);
      return true;
    }
    return false;
  }

  void appendEntryFromCombination(
      out_type<velox::Array<velox::Array<T>>>& result,
      const arg_type<velox::Array<T>>& array,
      std::vector<int>& combination) {
    auto& innerArray = result.add_item();
    for (auto idx : combination) {
      if (!array[idx].has_value()) {
        innerArray.add_null();
        continue;
      }

      if constexpr (std::is_same_v<T, Varchar>) {
        innerArray.add_item().setNoCopy(array[idx].value());
      } else {
        innerArray.push_back(array[idx].value());
      }
    }
  }

  /// Employs an iterative approach of generating combinations. Each
  /// 'combination' is a set of numbers that represent the indices of the
  /// elements within the input array. Later, using this, an entry is generated
  /// in the output array by copying the elements corresponding to those indices
  /// from the input array.
  /// The iterative approach is based on the fact that if combinations are
  /// lexicographically sorted based on their indices in the input array then
  /// each combination in that sequence is the next lowest lexicographic order
  /// that can be formed when compared to its previous consecutive combination.
  // So we start with the first combination (first k elements 0,1,2,...k the
  // lowest lexicographic order) and on each iteration generate the next lowest
  // lexicographic order. eg. for (0,1,2,3) combinations of size 3 are
  // (0, 1, 2), (0, 1, 3), (0, 2, 3), (1, 2, 3)
  FOLLY_ALWAYS_INLINE void call(
      out_type<velox::Array<velox::Array<T>>>& result,
      const arg_type<velox::Array<T>>& array,
      const int32_t& combinationSize) {
    auto arraySize = array.size();
    VELOX_USER_CHECK_GE(
        combinationSize,
        0,
        "combination size must not be negative: {}",
        combinationSize);
    VELOX_USER_CHECK_LE(
        combinationSize,
        kMaxCombinationSize,
        "combination size must not exceed {}: {}",
        kMaxCombinationSize,
        combinationSize);
    if (combinationSize > arraySize) {
      // An empty array should be returned
      return;
    }
    if (combinationSize == 0) {
      // return a single empty array element within the array.
      result.add_item();
      return;
    }
    int64_t combinationCount =
        calculateCombinationCount(arraySize, combinationSize);
    VELOX_USER_CHECK_LE(
        combinationCount,
        kMaxNumberOfCombinations,
        "combinations exceed max size of {}",
        kMaxNumberOfCombinations);
    result.reserve(combinationCount);
    std::vector<int> currentCombination = firstCombination(combinationSize);
    do {
      appendEntryFromCombination(result, array, currentCombination);
    } while (nextCombination(currentCombination, arraySize));
  }
};

template <typename T>
struct ArraySumFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T)
  template <typename TOutput, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TOutput& out, const TInput& array) {
    TOutput sum = 0;
    for (const auto& item : array) {
      if (item.has_value()) {
        if constexpr (std::is_same_v<TOutput, int64_t>) {
          sum = checkedPlus<TOutput>(sum, *item);
        } else {
          sum += *item;
        }
      }
    }
    out = sum;
    return;
  }

  template <typename TOutput, typename TInput>
  FOLLY_ALWAYS_INLINE void callNullFree(TOutput& out, const TInput& array) {
    // Not nulls path
    TOutput sum = 0;
    for (const auto& item : array) {
      if constexpr (std::is_same_v<TOutput, int64_t>) {
        sum = checkedPlus<TOutput>(sum, item);
      } else {
        sum += item;
      }
    }
    out = sum;
    return;
  }
};

template <typename T>
struct ArrayAverageFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      double& out,
      const arg_type<Array<double>>& array) {
    // If the array is empty, then set result to null.
    if (array.size() == 0) {
      return false;
    }

    double sum = 0;
    size_t count = 0;
    for (const auto& item : array) {
      if (item.has_value()) {
        sum += *item;
        count++;
      }
    }
    if (count != 0) {
      out = sum / count;
    }
    return count != 0;
  }

  FOLLY_ALWAYS_INLINE bool callNullFree(
      double& out,
      const null_free_arg_type<Array<double>>& array) {
    // If the array is empty, then set result to null.
    if (array.size() == 0) {
      return false;
    }

    double sum = 0;
    for (const auto& item : array) {
      sum += item;
    }
    out = sum / array.size();
    return true;
  }
};

template <typename TExecCtx, typename T>
struct ArrayHasDuplicatesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecCtx);

  FOLLY_ALWAYS_INLINE void call(
      bool& out,
      const arg_type<velox::Array<T>>& inputArray) {
    folly::F14FastSet<arg_type<T>> uniqSet;
    int16_t numNulls = 0;
    out = false;
    for (const auto& item : inputArray) {
      if (item.has_value()) {
        if (!uniqSet.insert(item.value()).second) {
          out = true;
          break;
        }
      } else {
        numNulls++;
        if (numNulls == 2) {
          out = true;
          break;
        }
      }
    }
  }

  FOLLY_ALWAYS_INLINE void callNullFree(
      bool& out,
      const null_free_arg_type<velox::Array<T>>& inputArray) {
    folly::F14FastSet<null_free_arg_type<T>> uniqSet;
    out = false;
    for (const auto& item : inputArray) {
      if (!uniqSet.insert(item).second) {
        out = true;
        break;
      }
    }
  }
};

// Function Signature: array<T> -> map<T, int>, where T is ("bigint", "varchar")
// Returns a map with frequency of each element in the input array vector.
template <typename TExecParams, typename T>
struct ArrayFrequencyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  FOLLY_ALWAYS_INLINE void call(
      out_type<velox::Map<T, int>>& out,
      arg_type<velox::Array<T>> inputArray) {
    frequencyCount_.clear();

    for (const auto& item : inputArray.skipNulls()) {
      frequencyCount_[item]++;
    }

    // To make the output order of key value pairs deterministic (since F14
    // does not provide ordering guarantees), we do another iteration in the
    // input and look up element frequencies in the F14 map. To prevent
    // duplicates in the output, we remove the keys we already added.
    for (const auto& item : inputArray.skipNulls()) {
      auto it = frequencyCount_.find(item);
      if (it != frequencyCount_.end()) {
        auto [keyWriter, valueWriter] = out.add_item();
        keyWriter = item;
        valueWriter = it->second;
        frequencyCount_.erase(it);
      }
    }
  }

 private:
  folly::F14FastMap<arg_type<T>, int> frequencyCount_;
};

template <typename TExecParams, typename T>
struct ArrayNormalizeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  FOLLY_ALWAYS_INLINE void callNullFree(
      out_type<velox::Array<T>>& result,
      const null_free_arg_type<velox::Array<T>>& inputArray,
      const null_free_arg_type<T>& p) {
    VELOX_USER_CHECK_GE(
        p, 0, "array_normalize only supports non-negative p: {}", p);

    // If the input array is empty, then the empty result should be returned,
    // same as Presto.
    if (inputArray.size() == 0) {
      return;
    }

    result.reserve(inputArray.size());

    // If p = 0, then it is L0 norm. Presto version returns the input array.
    if (p == 0) {
      result.add_items(inputArray);
      return;
    }

    // Calculate p-norm.
    T sum = 0;
    for (const auto& item : inputArray) {
      sum += pow(abs(item), p);
    }

    T pNorm = pow(sum, 1.0 / p);

    // If the input array is a zero vector then pNorm = 0.
    // Return the input array for this case, same as Presto.
    if (pNorm == 0) {
      result.add_items(inputArray);
      return;
    }

    // Construct result array from the input array and pNorm.
    for (const auto& item : inputArray) {
      result.add_item() = item / pNorm;
    }
  }
};

/// This class implements the array concat function.
///
/// DEFINITION:
/// concat(array1, array2, ..., arrayN) → array
/// Concatenates the arrays array1, array2, ..., arrayN. This function
/// provides the same functionality as the SQL-standard concatenation
/// operator (||).
///
///  Note:
///   - For compatibility with Presto a maximum arity of 254 is enforced.
template <typename TExec, typename T>
struct ArrayConcatFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec)

  static constexpr int32_t kMinArity = 2;
  static constexpr int32_t kMaxArity = 254;

  void call(
      out_type<Array<T>>& out,
      const arg_type<Variadic<Array<T>>>& arrays) {
    VELOX_USER_CHECK_GE(
        arrays.size(),
        kMinArity,
        "There must be {} or more arguments to concat",
        kMinArity);
    VELOX_USER_CHECK_LE(
        arrays.size(), kMaxArity, "Too many arguments for concat function");
    int64_t elementCount = 0;
    for (const auto& array : arrays) {
      elementCount += array.value().size();
    }
    out.reserve(elementCount);
    for (const auto& array : arrays) {
      out.add_items(array.value());
    }
  }

  void call(
      out_type<Array<T>>& out,
      const arg_type<Array<T>>& array,
      const arg_type<T>& element) {
    out.reserve(array.size() + 1);
    out.add_items(array);
    out.push_back(element);
  }

  void call(
      out_type<Array<T>>& out,
      const arg_type<T>& element,
      const arg_type<Array<T>>& array) {
    out.reserve(array.size() + 1);
    out.push_back(element);
    out.add_items(array);
  }
};

inline void checkIndexArrayTrim(int64_t size, int64_t arraySize) {
  if (size < 0) {
    VELOX_USER_FAIL("size must not be negative: {}", size);
  }

  if (size > arraySize) {
    VELOX_USER_FAIL(
        "size must not exceed array cardinality. arraySize: {}, size: {}",
        arraySize,
        size);
  }
}

template <typename T>
struct ArrayTrimFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Fast path for primitives.
  template <typename Out, typename In>
  void call(Out& out, const In& inputArray, int64_t size) {
    checkIndexArrayTrim(size, inputArray.size());

    int64_t end = inputArray.size() - size;
    for (int i = 0; i < end; ++i) {
      out.push_back(inputArray[i]);
    }
  }

  // Generic implementation.
  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Generic<T1>>>& inputArray,
      const int64_t& size) {
    checkIndexArrayTrim(size, inputArray.size());

    int64_t end = inputArray.size() - size;
    for (int i = 0; i < end; ++i) {
      out.push_back(inputArray[i]);
    }
  }
};

template <typename T>
struct ArrayTrimFunctionString {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  // String version that avoids copy of strings.
  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& out,
      const arg_type<Array<Varchar>>& inputArray,
      int64_t size) {
    checkIndexArrayTrim(size, inputArray.size());

    int64_t end = inputArray.size() - size;
    for (int i = 0; i < end; ++i) {
      if (inputArray[i].has_value()) {
        auto& newItem = out.add_item();
        newItem.setNoCopy(inputArray[i].value());
      } else {
        out.add_null();
      }
    }
  }
};

template <typename T>
struct ArrayRemoveNullFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Fast path for primitives.
  template <typename Out, typename In>
  FOLLY_ALWAYS_INLINE void call(Out& out, const In& inputArray) {
    for (int i = 0; i < inputArray.size(); ++i) {
      if (inputArray[i].has_value()) {
        out.push_back(inputArray[i].value());
      }
    }
  }

  // Generic implementation.
  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Generic<T1>>>& inputArray) {
    for (int i = 0; i < inputArray.size(); ++i) {
      if (inputArray[i].has_value()) {
        out.push_back(inputArray[i].value());
      }
    }
  }
};

template <typename T>
struct ArrayRemoveNullFunctionString {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  // String version that avoids copy of strings.
  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& out,
      const arg_type<Array<Varchar>>& inputArray) {
    for (int i = 0; i < inputArray.size(); ++i) {
      if (inputArray[i].has_value()) {
        auto& newItem = out.add_item();
        newItem.setNoCopy(inputArray[i].value());
      }
    }
  }
};

template <typename T>
struct ArrayNGramsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T)

  // Fast path for primitives.
  template <typename Out, typename In>
  void call(Out& out, const In& input, int32_t n) {
    VELOX_USER_CHECK_GT(n, 0, "N must be greater than zero.");

    if (n > input.size()) {
      auto& newItem = out.add_item();
      newItem.copy_from(input);
      return;
    }

    for (auto i = 0; i <= input.size() - n; ++i) {
      auto& newItem = out.add_item();
      for (auto j = 0; j < n; ++j) {
        if (input[i + j].has_value()) {
          auto& newGranularItem = newItem.add_item();
          newGranularItem = input[i + j].value();
        } else {
          newItem.add_null();
        }
      }
    }
  }

  // Generic implementation.
  void call(
      out_type<Array<Array<Generic<T1>>>>& out,
      const arg_type<Array<Generic<T1>>>& input,
      int32_t n) {
    VELOX_USER_CHECK_GT(n, 0, "N must be greater than zero.");

    if (n > input.size()) {
      auto& newItem = out.add_item();
      newItem.copy_from(input);
      return;
    }

    for (auto i = 0; i <= input.size() - n; ++i) {
      auto& newItem = out.add_item();
      for (auto j = 0; j < n; ++j) {
        if (input[i + j].has_value()) {
          auto& newGranularItem = newItem.add_item();
          newGranularItem.copy_from(input[i + j].value());
        } else {
          newItem.add_null();
        }
      }
    }
  }
};

template <typename T>
struct ArrayNGramsFunctionString {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  // String version that avoids copy of strings.
  void call(
      out_type<Array<Array<Varchar>>>& out,
      const arg_type<Array<Varchar>>& input,
      int32_t n) {
    VELOX_USER_CHECK_GT(n, 0, "N must be greater than zero.");

    if (n > input.size()) {
      auto& newItem = out.add_item();
      newItem.copy_from(input);
      return;
    }

    for (auto i = 0; i <= input.size() - n; ++i) {
      auto& newItem = out.add_item();
      for (auto j = 0; j < n; ++j) {
        if (input[i + j].has_value()) {
          auto& newGranularItem = newItem.add_item();
          newGranularItem.setNoCopy(input[i + j].value());
        } else {
          newItem.add_null();
        }
      }
    }
  }
};

/// This class implements the array flatten function.
///
/// DEFINITION:
/// flatten(x) → array
/// Flattens an array(array(T)) to an array(T) by concatenating the contained
/// arrays.
template <typename T>
struct ArrayFlattenFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T)

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Array<Generic<T1>>>>& arrays) {
    int64_t elementCount = 0;
    for (const auto& array : arrays) {
      if (array.has_value()) {
        elementCount += array.value().size();
      }
    }
    out.reserve(elementCount);
    for (const auto& array : arrays) {
      if (array.has_value()) {
        out.add_items(array.value());
      }
    }
  }
};

/// This class implements the array union function.
///
/// DEFINITION:
/// array_union(x, y) → array
/// Returns an array of the elements in the union of x and y, without
/// duplicates.
template <typename T>
struct ArrayUnionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T)

  template <typename Out, typename In>
  void call(Out& out, const In& inputArray1, const In& inputArray2) {
    folly::F14FastSet<typename In::element_t> elementSet;
    bool nullAdded = false;
    auto addItems = [&](auto& inputArray) {
      for (const auto& item : inputArray) {
        if (item.has_value()) {
          if (elementSet.insert(item.value()).second) {
            out.push_back(item.value());
          }
        } else if (!nullAdded) {
          nullAdded = true;
          out.add_null();
        }
      }
    };
    addItems(inputArray1);
    addItems(inputArray2);
  }
};

/// This class implements the array_remove function.
///
/// DEFINITION:
/// array_remove(x, element) -> array
/// Remove all elements that equal element from array x.
template <typename T>
struct ArrayRemoveFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Fast path for primitives.
  template <typename Out, typename In, typename E>
  void call(Out& out, const In& inputArray, E element) {
    for (const auto& item : inputArray) {
      if (item.has_value()) {
        if (element != item.value()) {
          out.push_back(item.value());
        }
      } else {
        out.add_null();
      }
    }
  }

  // Generic implementation.
  void call(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Generic<T1>>>& array,
      const arg_type<Generic<T1>>& element) {
    static constexpr CompareFlags kFlags = CompareFlags::equality(
        CompareFlags::NullHandlingMode::kNullAsIndeterminate);
    std::vector<std::optional<exec::GenericView>> toCopyItems;
    for (const auto& item : array) {
      if (!item.has_value()) {
        toCopyItems.push_back(std::nullopt);
        continue;
      }

      auto result = element.compare(item.value(), kFlags);
      VELOX_USER_CHECK(
          result.has_value(),
          "array_remove does not support arrays with elements that are null or contain null")
      if (result.value()) {
        toCopyItems.push_back(item.value());
      }
    }

    for (const auto& item : toCopyItems) {
      out.push_back(item);
    }
  }
};

template <typename T>
struct ArrayRemoveFunctionString {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  // String version that avoids copy of strings.
  void call(
      out_type<Array<Varchar>>& out,
      const arg_type<Array<Varchar>>& inputArray,
      const arg_type<Varchar>& element) {
    for (const auto& item : inputArray) {
      if (item.has_value()) {
        auto result = element.compare(item.value());
        if (result) {
          out.push_back(item.value());
        }
      } else {
        out.add_null();
      }
    }
  }
};
} // namespace facebook::velox::functions
