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
#include <string>

namespace facebook::velox::core {
class IExpr;
class SortOrder;
} // namespace facebook::velox::core

namespace facebook::velox::duckdb {

// Parses an input expression using DuckDB's internal postgresql-based parser,
// converting it to an IExpr tree. Takes a single expression as input.
//
// One caveat to keep in mind when using DuckDB's parser is that all identifiers
// are lower-cased, what prevents you to use functions and column names
// containing upper case letters (e.g: "concatRow" will be parsed as
// "concatrow").
std::shared_ptr<const core::IExpr> parseExpr(const std::string& exprString);

// Parses an ORDER BY clause using DuckDB's internal postgresql-based parser,
// converting it to a pair of an IExpr tree and a core::SortOrder. Uses ASC
// NULLS LAST as the default sort order.
std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder> parseOrderByExpr(
    const std::string& exprString);

} // namespace facebook::velox::duckdb
