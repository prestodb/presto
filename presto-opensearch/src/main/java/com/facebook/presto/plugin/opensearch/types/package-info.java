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
 * Type mapping utilities for OpenSearch connector.
 * <p>
 * This package contains classes for mapping between OpenSearch field types
 * and Presto types, including support for:
 * <ul>
 *   <li>Primitive types (text, keyword, numeric types, boolean, date)</li>
 *   <li>Complex types (nested, object)</li>
 *   <li>Array types</li>
 *   <li>Vector types (knn_vector for similarity search)</li>
 * </ul>
 */
package com.facebook.presto.plugin.opensearch.types;
