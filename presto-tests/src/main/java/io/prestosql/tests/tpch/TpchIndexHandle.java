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
package io.prestosql.tests.tpch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorIndexHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TpchIndexHandle
        implements ConnectorIndexHandle
{
    private final String tableName;
    private final double scaleFactor;
    private final Set<String> indexColumnNames;
    private final TupleDomain<ColumnHandle> fixedValues;

    @JsonCreator
    public TpchIndexHandle(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("scaleFactor") double scaleFactor,
            @JsonProperty("indexColumnNames") Set<String> indexColumnNames,
            @JsonProperty("fixedValues") TupleDomain<ColumnHandle> fixedValues)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.scaleFactor = scaleFactor;
        this.indexColumnNames = ImmutableSet.copyOf(requireNonNull(indexColumnNames, "indexColumnNames is null"));
        this.fixedValues = requireNonNull(fixedValues, "fixedValues is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public double getScaleFactor()
    {
        return scaleFactor;
    }

    @JsonProperty
    public Set<String> getIndexColumnNames()
    {
        return indexColumnNames;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getFixedValues()
    {
        return fixedValues;
    }
}
