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

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "velox/common/future/VeloxPromise.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::operators::test {

// Replacement for velox::exec::test::readCursor, which Velox no longer
// provides as a public test utility (velox#20482). The implementation is
// the same as the removed one: create a TaskCursor for 'params' and
// collect its output batches with readCursorAsync.
inline std::pair<
    std::unique_ptr<facebook::velox::exec::TaskCursor>,
    std::vector<facebook::velox::RowVectorPtr>>
readCursor(
    const facebook::velox::exec::CursorParameters& params,
    std::function<void(facebook::velox::exec::TaskCursor*)> addSplits =
        [](facebook::velox::exec::TaskCursor* taskCursor) {
          if (taskCursor->noMoreSplits()) {
            return;
          }
          taskCursor->setNoMoreSplits();
        },
    uint64_t maxWaitMicros = 5'000'000) {
  auto cursor = facebook::velox::exec::TaskCursor::create(params);
  // Declared after 'cursor' so that it is destroyed first, also on exception.
  std::vector<facebook::velox::RowVectorPtr> result;
  facebook::velox::exec::test::readCursorAsync(
      *cursor,
      [addSplitsVoid = std::move(addSplits)](
          facebook::velox::exec::TaskCursor* taskCursor) {
        addSplitsVoid(taskCursor);
        return facebook::velox::ContinueFuture::makeEmpty();
      },
      maxWaitMicros,
      [&](const facebook::velox::RowVectorPtr& vector) {
        result.push_back(vector);
      });
  return {std::move(cursor), std::move(result)};
}

} // namespace facebook::presto::operators::test
