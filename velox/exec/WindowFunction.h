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

#include "velox/core/QueryConfig.h"
#include "velox/exec/WindowPartition.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::exec {

// Represents arguments for window functions. Stores argument type,
// the constant value(if it is one) or the column index in the input row
// (if the argument is a FieldAccessTypedExpr).
struct WindowFunctionArg {
  const TypePtr type;
  const VectorPtr constantValue;
  std::optional<const column_index_t> index;
};

class WindowFunction {
 public:
  /// The data process mode for calculating the window function.
  enum class ProcessMode {
    /// Process can only start after all the rows from a partition become
    /// available.
    kPartition,
    /// Process can start as soon as rows are available within a partition,
    /// without waiting for all the rows in the partition to be ready.
    kRows,
  };

  /// Indicates whether this is an aggregate window function and its process
  /// unit.
  struct Metadata {
    ProcessMode processMode;
    bool isAggregate;

    static Metadata defaultMetadata() {
      static Metadata defaultValue{ProcessMode::kPartition, false};
      return defaultValue;
    }
  };

  explicit WindowFunction(
      TypePtr resultType,
      memory::MemoryPool* pool,
      HashStringAllocator* stringAllocator)
      : resultType_(std::move(resultType)),
        pool_(pool),
        stringAllocator_(stringAllocator) {}

  virtual ~WindowFunction() = default;

  // Row number to use in WindowPartition::extractColumn to request a NULL
  // value.
  static constexpr vector_size_t kNullRow = -1;

  const TypePtr& resultType() const {
    return resultType_;
  }

  memory::MemoryPool* pool() const {
    return pool_;
  }

  const HashStringAllocator* stringAllocator() const {
    return stringAllocator_;
  }

  /// This function is invoked by the Window operator when it
  /// starts processing a new partition of rows in the input data.
  /// The partition is stream of rows with the same values of the
  /// partition keys and ordered by the sorting keys of the window.
  /// The WindowPartition object can be used to access the
  /// underlying rows of the partition.
  virtual void resetPartition(const WindowPartition* partition) = 0;

  /// This function is invoked by the Window Operator to compute
  /// the window function for a batch of rows.
  /// @param peerGroupStarts  A buffer of the indexes of rows at which the
  /// peer group of the current row starts. Rows are peers if they
  /// have the same value in the partition ordering.
  /// @param peerGroupEnds  A buffer of the indexes of rows at which the
  /// peer group of the current row ends.
  /// @param frameStarts  A buffer of the indexes of rows at which the
  /// frame for the current row starts.
  /// @param frameEnds  A buffer of the indexes of rows at which the frame
  /// for the current row ends.
  /// @param validRows A SelectivityVector whose bits are turned on for
  /// valid (and off for empty) window frames in this batch of rows.
  /// Empty window frames have boundaries that violate the condition that
  /// the frameStart <= frameEnd for that row.
  /// @param resultOffset  This function is invoked multiple times for a
  /// partition as output buffers are available for it. resultOffset
  /// is the offset in the result buffer corresponding to the current
  /// block of rows. The function author is expected to populate values
  /// (one for each input row) in the result buffer starting at this index.
  /// Note : It can be assumed that this function will be invoked for
  /// rows of the partition in the order of the ORDER BY clause.
  /// Subsequent calls to apply(...) will not work on a prior
  /// portion of the partition.
  /// @param result : The vector of result values to be populated.
  virtual void apply(
      const BufferPtr& peerGroupStarts,
      const BufferPtr& peerGroupEnds,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) = 0;

  static std::unique_ptr<WindowFunction> create(
      const std::string& name,
      const std::vector<WindowFunctionArg>& args,
      const TypePtr& resultType,
      bool ignoreNulls,
      memory::MemoryPool* pool,
      HashStringAllocator* stringAllocator,
      const core::QueryConfig& config);

 protected:
  // defaultResult is a vector of size 1 containing the default value to be
  // returned for rows with empty frames. For aggregate window functions,
  // defaultResult contains the value returned by the aggregate when there are
  // no input rows.
  void setEmptyFramesResult(
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& defaultResult,
      const VectorPtr& result);

  const TypePtr resultType_;
  memory::MemoryPool* pool_;
  HashStringAllocator* const stringAllocator_;

  // Used for setting null for empty frames.
  SelectivityVector invalidRows_;
};

/// Information from the Window operator that is useful for the function logic.
/// @param args  Vector of the input arguments to the function. These could be
/// constants or positions of the input argument column in the input row of the
/// operator. These indices are used to access data from the WindowPartition
/// object.
/// @param resultType  Type of the result of the function.
using WindowFunctionFactory = std::function<std::unique_ptr<WindowFunction>(
    const std::vector<WindowFunctionArg>& args,
    const TypePtr& resultType,
    bool ignoreNulls,
    memory::MemoryPool* pool,
    HashStringAllocator* stringAllocator,
    const core::QueryConfig& config)>;

/// Register a window function with the specified name and signatures.
/// Registering a function with the same name a second time overrides the first
/// registration.
bool registerWindowFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    WindowFunction::Metadata metadata,
    WindowFunctionFactory factory);

/// Returns signatures of the window function with the specified name.
/// Returns empty std::optional if function with that name is not found.
std::optional<std::vector<FunctionSignaturePtr>> getWindowFunctionSignatures(
    const std::string& name);

struct WindowFunctionEntry {
  std::vector<FunctionSignaturePtr> signatures;
  WindowFunctionFactory factory;
  WindowFunction::Metadata metadata;
};

/// Returns window function metadata.
WindowFunction::Metadata getWindowFunctionMetadata(const std::string& name);

using WindowFunctionMap = std::unordered_map<std::string, WindowFunctionEntry>;

/// Returns a map of all window function names to their registrations.
WindowFunctionMap& windowFunctions();
} // namespace facebook::velox::exec
