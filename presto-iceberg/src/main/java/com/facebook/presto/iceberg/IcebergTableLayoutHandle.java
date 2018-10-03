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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class IcebergTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final String database;
    private final String tableName;
    private final TupleDomain<ColumnHandle> predicates;
    private final Map<String, HiveColumnHandle> nameToColumnHandle;

    @JsonCreator
    public IcebergTableLayoutHandle(
            @JsonProperty("database") String database,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("predicates") TupleDomain<ColumnHandle> predicates,
            @JsonProperty("nameToColumnHandle") Map<String, HiveColumnHandle> nameToColumnHandle)
    {
        this.database = database;
        this.tableName = tableName;
        this.predicates = predicates;
        this.nameToColumnHandle = nameToColumnHandle;
    }

    @JsonProperty
    public String getDatabase()
    {
        return this.database;
    }

    @JsonProperty
    public String getTableName()
    {
        return this.tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPredicates()
    {
        return this.predicates;
    }

    @JsonProperty
    public Map<String, HiveColumnHandle> getNameToColumnHandle()
    {
        return this.nameToColumnHandle;
    }
}
