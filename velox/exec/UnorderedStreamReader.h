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

#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"

#include <folly/Likely.h>

namespace facebook::velox {

/// Abstract class defining the interface for a stream of values to read in
/// batch by UnorderedStreamReader.
class BatchStream {
 public:
  virtual ~BatchStream() = default;

  /// Returns the next batch from the stream. The function returns true with
  /// read data in 'batch', otherwise returns false if it reaches to the end of
  /// the stream.
  virtual bool nextBatch(RowVectorPtr& batch) = 0;
};

/// Implements a union reader to read from a group of streams with one at a time
/// in batch. The union reader owns the streams. At each call of nextBatch(), it
/// returns the next batch from the current reading stream, and the reader will
/// switch to the next stream internally if the current stream reaches to the
/// end.
///
/// NOTE: this object is not thread safe.
template <typename BatchStream>
class UnorderedStreamReader {
 public:
  explicit UnorderedStreamReader(
      std::vector<std::unique_ptr<BatchStream>> streams)
      : currentStream(0), streams_(std::move(streams)) {
    static_assert(std::is_base_of_v<BatchStream, BatchStream>);
  }

  /// Returns the next batch from the current reading stream. The function will
  /// switch to the next stream if the current stream reaches to the end. The
  /// function returns true with read data in 'batch', otherwise returns
  /// false if all the streams have been read out.
  bool nextBatch(RowVectorPtr& batch) {
    while (FOLLY_LIKELY(currentStream < streams_.size())) {
      if (FOLLY_LIKELY(streams_[currentStream]->nextBatch(batch))) {
        return true;
      }
      ++currentStream;
    }
    return false;
  }

 private:
  // Points to the current reading stream in 'streams_'.
  vector_size_t currentStream{0};
  // A list of streams to read in batch sequentially.
  std::vector<std::unique_ptr<BatchStream>> streams_;
};

} // namespace facebook::velox
