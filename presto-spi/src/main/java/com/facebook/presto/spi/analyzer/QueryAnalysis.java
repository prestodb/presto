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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.spi.function.FunctionKind;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The QueryAnalysis interface should be used to represent the query semantic analysis.
 * This interface should be implemented by respective analyzer to represent query semantic analysis.
 */
public interface QueryAnalysis
{
    /**
     * Returns the update type of the query.
     *
     * @return a String representing the type of update (e.g., "INSERT", "CREATE TABLE", "DELETE", etc)
     */
    String getUpdateType();

    /**
     * Returns the expanded query, which might have been enhanced after analyzing materialized view.
     */
    Optional<String> getExpandedQuery();

    /**
     * Returns function names based on the kinds (scalar, aggregate and window).
     */
    Map<FunctionKind, Set<String>> getInvokedFunctions();
}
