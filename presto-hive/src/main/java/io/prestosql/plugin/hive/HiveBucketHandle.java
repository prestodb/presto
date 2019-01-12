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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HiveBucketHandle
{
    private final List<HiveColumnHandle> columns;
    // Number of buckets in the table, as specified in table metadata
    private final int tableBucketCount;
    // Number of buckets the table will appear to have when the Hive connector
    // presents the table to the engine for read.
    private final int readBucketCount;

    @JsonCreator
    public HiveBucketHandle(
            @JsonProperty("columns") List<HiveColumnHandle> columns,
            @JsonProperty("tableBucketCount") int tableBucketCount,
            @JsonProperty("readBucketCount") int readBucketCount)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.tableBucketCount = tableBucketCount;
        this.readBucketCount = readBucketCount;
    }

    @JsonProperty
    public List<HiveColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public int getTableBucketCount()
    {
        return tableBucketCount;
    }

    @JsonProperty
    public int getReadBucketCount()
    {
        return readBucketCount;
    }

    public HiveBucketProperty toTableBucketProperty()
    {
        return new HiveBucketProperty(
                columns.stream()
                        .map(HiveColumnHandle::getName)
                        .collect(toList()),
                tableBucketCount,
                ImmutableList.of());
    }
}
