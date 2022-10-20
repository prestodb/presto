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

#include <iostream>

#include "velox/expression/VectorReaders.h"
#include "velox/expression/VectorWriters.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;

/// This file contains examples to use VectorReader and VectorWriter to
/// read/write Velox vectors.
/// VectorReaders and VectorWriters are efficient and lazy and should be used
/// unless there's a clear reason not to.

namespace {
// produce serial number for each call
auto serial() {
  static int64_t n = 0;
  return n++;
}
} // namespace

int main() {
  const int num_rows = 8;

  /****************** Vector Writer **********************/

  // Define rows to write
  SelectivityVector rows{num_rows};
  // Array<Map<int,int>>
  auto type = TypeFactory<TypeKind::ARRAY>::create(
      TypeFactory<TypeKind::MAP>::create(BIGINT(), BIGINT()));
  auto pool = memory::getDefaultScopedMemoryPool();

  // result vector
  VectorPtr result;

  // 1. Make sure result is writable for rows of interest
  BaseVector::ensureWritable(rows, type, pool.get(), result);

  // 2. Define a vector writer VectorWriter<T> where T is the type expressed in
  // the simple function type system.
  exec::VectorWriter<Array<Map<int64_t, int64_t>>> vectorWriter;

  // 3. Initialize the writer to write to result vector.
  vectorWriter.init(*result->as<ArrayVector>());

  rows.applyToSelected([&](vector_size_t row) {
    // 4. To write to a specific row call setOffset(row) followed by current()
    // to get the writer at that row
    vectorWriter.setOffset(row);
    auto& arrayWriter = vectorWriter.current();

    // Insert a map element to the array
    auto& child = arrayWriter.add_item();

    // Insert 2 kv pairs to the map
    child.emplace(serial(), serial());
    child.emplace(serial(), serial());

    // Insert an empty map
    arrayWriter.add_item();

    // 5. After finishing writing the row call commit(), or commit(false) to
    // write a null
    vectorWriter.commit();
  });

  // 6. After finishing writing all rows, call finish()
  vectorWriter.finish();

  /****************** Vector Reader **********************/

  // 1. Decode the vector for rows of interest.
  DecodedVector decoded;
  decoded.decode(*result, rows);

  // 2. Define vectorReader<T> where T is the type of the vector being read, T
  // is expressed in the simple function type system
  exec::VectorReader<Array<Map<int64_t, int64_t>>> reader(&decoded);

  std::cout << "Reading Vector: " << std::endl;
  rows.applyToSelected([&](vector_size_t row) {
    // Check if the row is null.
    if (reader.isSet(row) == false) {
      std::cout << "[]" << std::endl;
      return;
    }

    std::cout << "[";

    // 3. To read a row call reader[row] and it will return a std::like object
    // that represents the elements at the row.
    // arrayView has std::vector<std::optional<V>> interface
    auto arrayView = reader[row];

    // Elements of the array have std::map<int, std::optional<int>>
    // interface.
    for (const auto& container : arrayView) {
      if (container.has_value()) {
        std::cout << " {";
        for (const auto& [k, v] : container.value()) {
          std::cout << "(" << k << ", "
                    << (v.has_value() ? std::to_string(v.value()) : "") << ")";
        }
        std::cout << "} ";
      } else {
        std::cout << " null ";
      }
    }

    std::cout << "]" << std::endl;
  });

  // Null-free reading when knowing the container doesn't have null value

  assert(!decoded.mayHaveNullsRecursive());
  std::cout << "Reading null-free Vector: " << std::endl;
  rows.applyToSelected([&](vector_size_t row) {
    std::cout << "[";

    // Read the row null-free. arrayView has std::vector<V>
    auto arrayView = reader.readNullFree(row);
    // Elements of the array have std::map<int, int> interface.
    for (const auto& container : arrayView) {
      std::cout << " {";
      for (const auto& [k, v] : container) {
        std::cout << "(" << k << ", " << v << ")";
      }
      std::cout << "} ";
    }

    std::cout << "]" << std::endl;
  });

  return 0;
}
