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

#include "velox/exec/Driver.h"

namespace facebook::velox::exec {

class MergeExchange;

class MergeSource {
 public:
  virtual ~MergeSource() {}

  virtual BlockingReason next(RowVectorPtr& data, ContinueFuture* future) = 0;

  virtual BlockingReason enqueue(
      RowVectorPtr input,
      ContinueFuture* future) = 0;

  // Factory methods to create MergeSources.
  static std::shared_ptr<MergeSource> createLocalMergeSource();

  static std::shared_ptr<MergeSource> createMergeExchangeSource(
      MergeExchange* mergeExchange,
      const std::string& taskId);
};

/// Coordinates data transfer between single producer and single consumer. Used
/// to implement merge join.
class MergeJoinSource {
 public:
  /// Called by the consumer to fetch next batch of data.
  BlockingReason next(ContinueFuture* future, RowVectorPtr* data);

  /// Called by the producer to enqueue more data or signal that no more data
  /// is coming by passing nullptr for data.
  BlockingReason enqueue(RowVectorPtr data, ContinueFuture* future);

  /// Called by the consumer to signal that it doesn't need any more data. For
  /// example, if the merge join ran out of data on the right side.
  void close();

 private:
  struct State {
    bool atEnd;
    RowVectorPtr data;
  };

  folly::Synchronized<State> state_;

  // Satisfied when data becomes available or the producer reports that it
  // finished producing, e.g. state_.data is not nullptr or state_.atEnd is
  // true.
  std::optional<VeloxPromise<bool>> consumerPromise_;

  // Satisfied when previously enqueued data has been consumed, e.g. state_.data
  // is nullptr.
  std::optional<VeloxPromise<bool>> producerPromise_;
};

} // namespace facebook::velox::exec
