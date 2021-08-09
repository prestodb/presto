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

#include "velox/vector/RowSerde.h"

namespace facebook::velox::exec {

// Row-wise serialization for use in hash tables and order by.

class ContainerRowSerde : public RowSerde {
 public:
  void serialize(const BaseVector& source, vector_size_t index, ByteStream& out)
      const override;

  void deserialize(ByteStream& in, vector_size_t index, BaseVector* result)
      const override;

  int32_t compare(
      ByteStream& left,
      const DecodedVector& right,
      vector_size_t index,
      CompareFlags flags) const override;

  int32_t compare(
      ByteStream& left,
      ByteStream& right,
      const Type* type,
      CompareFlags flags) const override;

  uint64_t hash(ByteStream& data, const Type* type) const override;

  static const ContainerRowSerde& instance();
};

} // namespace facebook::velox::exec
