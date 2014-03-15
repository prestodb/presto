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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.IndexHandle;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class TpchIndexHandle
        implements IndexHandle
{
    private final String connectorId;
    private final String tableName;
    private final double scaleFactor;
    private final Set<String> indexColumnNames;
    private final TupleDomain fixedValues;

    @JsonCreator
    public TpchIndexHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("scaleFactor") double scaleFactor,
            @JsonProperty("indexColumnNames") Set<String> indexColumnNames,
            @JsonProperty("fixedValues") TupleDomain fixedValues)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.scaleFactor = scaleFactor;
        this.indexColumnNames = ImmutableSet.copyOf(checkNotNull(indexColumnNames, "indexColumnNames is null"));
        this.fixedValues = checkNotNull(fixedValues, "fixedValues is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
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
    public TupleDomain getFixedValues()
    {
        return fixedValues;
    }
}
