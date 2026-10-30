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
package com.facebook.presto.spi.materializedview;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Describes how an aggregate result may be combined by a materialized view at a coarser grouping.
 */
public final class MaterializedViewAggregationMergeStrategy
{
    private final String sourceFunctionName;
    private final Optional<String> mergeFunctionName;
    private final boolean duplicateInsensitive;

    public MaterializedViewAggregationMergeStrategy(String sourceFunctionName, Optional<String> mergeFunctionName, boolean duplicateInsensitive)
    {
        this.sourceFunctionName = canonicalize(sourceFunctionName);
        this.mergeFunctionName = requireNonNull(mergeFunctionName, "mergeFunctionName is null").map(MaterializedViewAggregationMergeStrategy::canonicalize);
        this.duplicateInsensitive = duplicateInsensitive;
    }

    public String getSourceFunctionName()
    {
        return sourceFunctionName;
    }

    public Optional<String> getMergeFunctionName()
    {
        return mergeFunctionName;
    }

    public boolean isDuplicateInsensitive()
    {
        return duplicateInsensitive;
    }

    private static String canonicalize(String functionName)
    {
        String canonicalName = requireNonNull(functionName, "functionName is null").trim().toLowerCase(Locale.ENGLISH);
        if (canonicalName.isEmpty()) {
            throw new IllegalArgumentException("functionName is empty");
        }
        return canonicalName;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        MaterializedViewAggregationMergeStrategy that = (MaterializedViewAggregationMergeStrategy) object;
        return duplicateInsensitive == that.duplicateInsensitive &&
                sourceFunctionName.equals(that.sourceFunctionName) &&
                mergeFunctionName.equals(that.mergeFunctionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceFunctionName, mergeFunctionName, duplicateInsensitive);
    }
}
