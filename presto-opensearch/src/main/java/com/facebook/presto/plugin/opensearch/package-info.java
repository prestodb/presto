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

/**
 * OpenSearch connector for Presto.
 * <p>
 * This connector enables Presto to query OpenSearch indices using SQL.
 * It supports:
 * <ul>
 *   <li>Full SQL query capabilities (SELECT, WHERE, projections,
 *   aggregations)</li>
 *   <li>Predicate pushdown for efficient filtering at the
 *   source</li>
 *   <li>Comprehensive type system including arrays, maps, and nested
 *   structures</li>
 *   <li>Vector search support for k-NN queries</li>
 *   <li>Distributed execution with shard-aware splitting</li>
 *   <li>Scroll API for efficient pagination of large result sets</li>
 * </ul>
 */
package com.facebook.presto.plugin.opensearch;
