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
package com.facebook.presto.verifier.framework;

import static java.util.Objects.requireNonNull;

public enum DeterminismAnalysis
{
    DETERMINISTIC(Category.DETERMINISTIC),
    NON_DETERMINISTIC_COLUMNS(Category.NON_DETERMINISTIC),
    NON_DETERMINISTIC_ROW_COUNT(Category.NON_DETERMINISTIC),
    NON_DETERMINISTIC_LIMIT_CLAUSE(Category.NON_DETERMINISTIC),
    NON_DETERMINISTIC_CATALOG(Category.NON_DETERMINISTIC),
    ANALYSIS_FAILED_INCONSISTENT_SCHEMA(Category.UNKNOWN),
    ANALYSIS_FAILED_QUERY_FAILURE(Category.UNKNOWN),
    ANALYSIS_FAILED_DATA_CHANGED(Category.UNKNOWN),
    ANALYSIS_FAILED(Category.UNKNOWN);

    private enum Category
    {
        DETERMINISTIC,
        NON_DETERMINISTIC,
        UNKNOWN
    }

    private final Category category;

    DeterminismAnalysis(Category category)
    {
        this.category = requireNonNull(category, "category is null");
    }

    public boolean isDeterministic()
    {
        return category == Category.DETERMINISTIC;
    }

    public boolean isNonDeterministic()
    {
        return category == Category.NON_DETERMINISTIC;
    }

    public boolean isUnknown()
    {
        return category == Category.UNKNOWN;
    }
}
