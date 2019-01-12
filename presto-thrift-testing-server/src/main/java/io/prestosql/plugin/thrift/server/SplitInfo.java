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
package io.prestosql.plugin.thrift.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class SplitInfo
{
    private final String schemaName;
    private final String tableName;
    private final int partNumber;
    private final int totalParts;
    private final boolean indexSplit;
    private final List<String> lookupColumnNames;
    private final List<List<String>> keys;

    @JsonCreator
    public SplitInfo(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalParts") int totalParts,
            @JsonProperty("indexSplit") boolean indexSplit,
            @JsonProperty("lookupColumnNames") List<String> lookupColumnNames,
            @JsonProperty("keys") List<List<String>> keys)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partNumber = partNumber;
        this.totalParts = totalParts;
        this.indexSplit = indexSplit;
        this.lookupColumnNames = requireNonNull(lookupColumnNames, "lookupColumnNames is null");
        this.keys = requireNonNull(keys, "keys is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
    }

    @JsonProperty
    public int getTotalParts()
    {
        return totalParts;
    }

    @JsonProperty
    public boolean isIndexSplit()
    {
        return indexSplit;
    }

    @JsonProperty
    public List<String> getLookupColumnNames()
    {
        return lookupColumnNames;
    }

    @JsonProperty
    public List<List<String>> getKeys()
    {
        return keys;
    }

    public static SplitInfo normalSplit(String schemaName, String tableName, int partNumber, int totalParts)
    {
        return new SplitInfo(schemaName, tableName, partNumber, totalParts, false, ImmutableList.of(), ImmutableList.of());
    }

    public static SplitInfo indexSplit(String schemaName, String tableName, List<String> lookupColumnNames, List<List<String>> keys)
    {
        return new SplitInfo(schemaName, tableName, 0, 0, true, lookupColumnNames, keys);
    }
}
