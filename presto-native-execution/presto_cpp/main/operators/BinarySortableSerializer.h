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

#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/StringVectorBuffer.h"

namespace facebook::presto::operators {
/// BinarySortableSerializer is responsible for serializing sort keys from a
/// RowVector source. The key is generated so that a
/// lexicographical sort of the key will produce the ordering.
///
/// Based on Hive's BinarySortableSerDe:
///
/// BinarySortableSerDe can be used to write data in a way that the data can be
/// compared byte-by-byte with the same order.
///
class BinarySortableSerializer {
 public:
  BinarySortableSerializer(
      const velox::RowVectorPtr& source,
      const std::vector<velox::core::SortOrder>& sortOrders,
      const std::vector<velox::core::FieldAccessTypedExprPtr>& sortFields);

  /// Serialize the data into an raw buffer, the caller needs to ensure there
  /// serialized data won't overflow the buffer.
  void serialize(velox::vector_size_t rowId, velox::StringVectorBuffer* out);

 private:
  const velox::RowVectorPtr input_;
  const std::vector<velox::core::SortOrder> sortOrders_;
  const std::vector<std::pair<int32_t, velox::column_index_t>> sortChannels_;
};
} // namespace facebook::presto::operators
