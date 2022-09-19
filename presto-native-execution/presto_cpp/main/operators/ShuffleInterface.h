/*
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

#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

class ShuffleInterface {
 public:
  /// Write to the shuffle one row at a time.
  virtual void collect(int32_t partition, std::string_view data) = 0;

  /// Tell the shuffle system the writer is done.
  /// @param success set to false indicate aborted client.
  virtual void noMoreData(bool success) = 0;

  /// Check by the reader to see if more blocks are available for this
  /// partition.
  virtual bool hasNext(int32_t partition) const = 0;

  /// Read the next block of data for this partition.
  /// @param success set to false indicate aborted client.
  virtual velox::BufferPtr next(int32_t partition, bool success) = 0;
  /// Return true if all the data is finished writing and is ready to
  /// to be read while noMoreData signals the shuffle service that there
  /// is no more data to be writen.
  virtual bool readyForRead() const = 0;
};

} // namespace facebook::presto::operators