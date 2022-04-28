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

#include <memory>
#include <mutex>

#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dss.h"
#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dsstypes.h"

namespace facebook::velox::tpch {

/// This class exposes a thread-safe and reproducible iterator over TPC-H
/// synthetically generated data, backed by DBGEN.
///
/// Note that because DBGEN is an old C codebase which relies on global
/// variables for state, it is not possible to use the underlying functions to
/// generate datasets in parallel. This class provides thread-safety by ensuring
/// mutual exclusion (via mutex) when using iterators to generate data in
/// parallel. This class follows RAII, so the internal lock will be held for as
/// long as instances are in scope.
class DBGenIterator {
 public:
  // Use this function to create instances of DBGEN iterators. This call might
  // block in case other iterators are still in scope (and thus hold the
  // internal lock).
  static DBGenIterator create(size_t scaleFactor);

  // Generate different types of records.
  void genNation(size_t index, code_t& code);
  void genRegion(size_t index, code_t& code);
  void genOrder(size_t index, order_t& order);
  void genSupplier(size_t index, supplier_t& supplier);
  void genPart(size_t index, part_t& part);
  void genCustomer(size_t index, customer_t& customer);

 private:
  // Should not instantiate directly.
  explicit DBGenIterator(std::unique_lock<std::mutex>&& lease)
      : lockGuard_(std::move(lease)) {}

  // unique_lock instead of lock_guard so it's movable.
  std::unique_lock<std::mutex> lockGuard_;
};

} // namespace facebook::velox::tpch
