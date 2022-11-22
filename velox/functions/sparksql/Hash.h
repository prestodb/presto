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
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions::sparksql {

// Supported types:
//   - Boolean
//   - Integer types (tinyint, smallint, integer, bigint)
//   - Varchar, varbinary
//   - Real, double
//
// TODO:
//   - Decimal
//   - Date
//   - Timestamp
//   - Row, Array: hash the elements in order
//   - Map: iterate over map, hashing key then value. Since map ordering is
//        unspecified, hashing logically equivalent maps may result in
//        different hash values.

std::vector<std::shared_ptr<exec::FunctionSignature>> hashSignatures();

std::shared_ptr<exec::VectorFunction> makeHash(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

// Supported types:
//   - Bools
//   - Integer types (byte, short, int, long)
//   - String, Binary
//   - Float, Double
//
// Unsupported:
//   - Decimal
//   - Datetime
//   - Structs, Arrays: hash the elements in order
//   - Maps: iterate over map, hashing key then value. Since map ordering is
//        unspecified, hashing logically equivalent maps may result in
//        different hash values.

std::vector<std::shared_ptr<exec::FunctionSignature>> xxhash64Signatures();

std::shared_ptr<exec::VectorFunction> makeXxHash64(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

} // namespace facebook::velox::functions::sparksql
