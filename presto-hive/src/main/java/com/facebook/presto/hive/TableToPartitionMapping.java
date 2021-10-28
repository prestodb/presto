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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Column;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jol.info.ClassLayout;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.util.Objects.requireNonNull;

public class TableToPartitionMapping
{
    public static TableToPartitionMapping empty()
    {
        return new TableToPartitionMapping(Optional.empty(), ImmutableMap.of());
    }

    public static TableToPartitionMapping mapColumnsByIndex(Map<Integer, Column> partitionSchemaDifference)
    {
        return new TableToPartitionMapping(Optional.empty(), partitionSchemaDifference);
    }

    // Overhead of ImmutableMap is not accounted because of its complexity.
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TableToPartitionMapping.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();
    private static final int OPTIONAL_INSTANCE_SIZE = ClassLayout.parseClass(Optional.class).instanceSize();

    private final Optional<Map<Integer, Integer>> tableToPartitionColumns;
    private final Map<Integer, Column> partitionSchemaDifference;

    @JsonCreator
    public TableToPartitionMapping(
            @JsonProperty("tableToPartitionColumns") Optional<Map<Integer, Integer>> tableToPartitionColumns,
            @JsonProperty("partitionSchemaDifference") Map<Integer, Column> partitionSchemaDifference)
    {
        if (tableToPartitionColumns.map(TableToPartitionMapping::isIdentityMapping).orElse(true)) {
            this.tableToPartitionColumns = Optional.empty();
        }
        else {
            this.tableToPartitionColumns = requireNonNull(tableToPartitionColumns, "tableToPartitionColumns is null")
                    .map(ImmutableMap::copyOf);
        }
        this.partitionSchemaDifference = ImmutableMap.copyOf(requireNonNull(partitionSchemaDifference, "partitionSchemaDifference is null"));
    }

    @VisibleForTesting
    static boolean isIdentityMapping(Map<Integer, Integer> map)
    {
        for (int i = 0; i < map.size(); i++) {
            if (!Objects.equals(map.get(i), i)) {
                return false;
            }
        }
        return true;
    }

    @JsonProperty
    public Optional<Map<Integer, Integer>> getTableToPartitionColumns()
    {
        return tableToPartitionColumns;
    }

    @JsonProperty
    public Map<Integer, Column> getPartitionSchemaDifference()
    {
        return partitionSchemaDifference;
    }

    public Optional<Column> getPartitionColumn(int tableColumnIndex)
    {
        return getPartitionColumnIndex(tableColumnIndex)
                .flatMap(partitionColumnIndex -> Optional.ofNullable(partitionSchemaDifference.get(partitionColumnIndex)));
    }

    private Optional<Integer> getPartitionColumnIndex(int tableColumnIndex)
    {
        if (!tableToPartitionColumns.isPresent()) {
            return Optional.of(tableColumnIndex);
        }
        return Optional.ofNullable(tableToPartitionColumns.get().get(tableColumnIndex));
    }

    public int getEstimatedSizeInBytes()
    {
        int result = INSTANCE_SIZE;
        result += sizeOfObjectArray(partitionSchemaDifference.size());
        for (Column column : partitionSchemaDifference.values()) {
            result += INTEGER_INSTANCE_SIZE + column.getEstimatedSizeInBytes();
        }
        result += OPTIONAL_INSTANCE_SIZE;
        if (tableToPartitionColumns.isPresent()) {
            result += sizeOfObjectArray(tableToPartitionColumns.get().size()) + 2 * tableToPartitionColumns.get().size() * INTEGER_INSTANCE_SIZE;
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionSchemaDifference", partitionSchemaDifference)
                .add("tableToPartitionColumns", tableToPartitionColumns)
                .toString();
    }
}
