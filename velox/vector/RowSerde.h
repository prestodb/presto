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

#include "velox/common/memory/ByteStream.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox {

class RowSerde {
 public:
  virtual ~RowSerde() = default;

  // Writes the contents of 'vector' at 'index' to 'out'.
  virtual void serialize(
      const BaseVector& source,
      vector_size_t index,
      ByteStream& out) const = 0;

  // Reads data from in, which is expected to have been produced by
  // serialize with a 'vector' of type 'result->type()'. Stores the
  // result in 'result at 'index'.
  virtual void deserialize(
      ByteStream& in,
      vector_size_t index,
      BaseVector* result) const = 0;

  // Returns < 0 if 'left' < 'right' at 'row', 0 if equal and > 0
  // otherwise. 'left' is expected to be data from 'serialize'
  // with a 'vector' of the same type as 'right'.
  virtual int32_t compare(
      ByteStream& left,
      const DecodedVector& right,
      vector_size_t index,
      CompareFlags flags = CompareFlags()) const = 0;

  //  Compares the contents of 'left' and 'right', both are expected to be
  //  produced by serialize of vectors of type 'type'.
  virtual int32_t compare(
      ByteStream& left,
      ByteStream& right,
      const Type* type,
      CompareFlags flags = CompareFlags()) const = 0;

  // Computes a hash for 'data', which is expected to have been
  // produced by serialize with a 'vector' of type 'type'.
  virtual uint64_t hash(ByteStream& data, const Type* type) const = 0;
};

} // namespace facebook::velox
