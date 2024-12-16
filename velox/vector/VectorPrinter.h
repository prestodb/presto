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

#include "velox/vector/BaseVector.h"

namespace facebook::velox {

/// Returns human-friendly text representation of the vector's data.
std::string printVector(const BaseVector& vector);

/// Returns human-friendly text representation of the vector's data in rows
/// [from, from + size).
/// @param from Zero-based row number of the first row to print. Must be
/// non-negative. If greater than vector size, no rows are printed.
/// @param size Number of of rows to print. If 'from' + 'size' is greater than
/// vector size, a subset of rows starting from 'from' to the end of the vector
/// are printed.
std::string printVector(
    const BaseVector& vector,
    vector_size_t from,
    vector_size_t size = 10);

/// Returns human-friendly text representation of the vector's data in 'rows'.
/// @param rows A set of rows to print.
std::string printVector(
    const BaseVector& vector,
    const SelectivityVector& rows);

class VectorPrinter {
 public:
  struct Options {
    // Options that control summarization of types.
    velox::Type::TypeSummaryOptions types;

    // Maximum number of child vectors to include in the summary.
    size_t maxChildren{5};

    // Whether to include the names of the RowVector child vectors.
    bool includeChildNames{false};

    // Whether to include unique IDs for each node in the vector hierarchy.
    bool includeNodeIds{false};

    // Optional indent to add to all lines. Useful when embedding the output of
    // the printer into some other text. Each indentation is 3 spaces. 0 means
    // no indentation. 1 - 3 spaces. 2 - 6 spaces.
    int32_t indent{0};

    // Whether to skip printing the summary of the top level vector. Similar to
    // 'indent', useful when embedding the output of the printer into some other
    // text.
    bool skipTopSummary{false};

    // Workaround for compiler error: default member initializer for
    // 'maxChildren' needed within definition of enclosing class 'VectorPrinter'
    // outside of member functions
    //
    // static std::string summarizeToText(
    //    const BaseVector& vector,
    //    Options options = {});
    static Options defaultOptions() {
      return {};
    }
  };

  // Returns a summary of the vector in human-readable text format.
  //
  // Prints a hierarchy of vectors with up to options.maxChildren children at
  // each level. For each vector, prints a header that includes the data type,
  // number of rows, encoding.
  //
  // For example,
  //
  // A flat vector of integers:
  //
  //  INTEGER 8 rows FLAT 32B
  //
  // A flat array of integers with 3 null arrays, 1 empty array and the rest
  // of arrays of average size 3:
  //
  //  ARRAY 8 rows ARRAY 288B
  //       Stats: 3 nulls, 1 empty, sizes: [2...4, avg 3]
  //    BIGINT 12 rows FLAT 128B
  //
  // A dictionary over map with 4 unique maps:
  //
  //  MAP 8 rows DICTIONARY 192B
  //      Stats: 0 nulls, 4 unique
  //    MAP 4 rows MAP 160B
  //        Stats: 0 nulls, 1 empty, sizes: [1...4, avg 2]
  //      INTEGER 8 rows FLAT 32B
  //      REAL 8 rows FLAT 32B
  static std::string summarizeToText(
      const BaseVector& vector,
      const Options& options = Options::defaultOptions());
};

} // namespace facebook::velox
