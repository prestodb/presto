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

#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/writer/WriterContext.h"

namespace facebook::velox::dwrf {
using StreamList =
    std::vector<std::pair<const DwrfStreamIdentifier*, DataBufferHolder*>>;

StreamList getStreamList(WriterContext& context);

class LayoutPlanner {
 public:
  explicit LayoutPlanner(StreamList streamList);
  virtual ~LayoutPlanner() = default;

  void iterateIndexStreams(
      std::function<void(const DwrfStreamIdentifier&, DataBufferHolder&)>
          consumer);

  void iterateDataStreams(
      std::function<void(const DwrfStreamIdentifier&, DataBufferHolder&)>
          consumer);

  virtual void plan();

 protected:
  StreamList streams_;
  size_t indexCount_;

  class NodeSizeSorter {
   public:
    static void sort(StreamList::iterator begin, StreamList::iterator end);
  };

  VELOX_FRIEND_TEST(LayoutPlannerTests, Basic);
};

} // namespace facebook::velox::dwrf
