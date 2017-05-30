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
package com.facebook.presto.raptorx.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class RollbackInfo
{
    private static final JsonCodec<RollbackInfo> CODEC = jsonCodec(RollbackInfo.class);

    private final List<Long> createdTableIds;
    private final List<Long> writtenTableIds;
    private final List<TableColumn> addedColumns;

    @JsonCreator
    public RollbackInfo(
            @JsonProperty("createdTableIds") List<Long> createdTableIds,
            @JsonProperty("writtenTableIds") List<Long> writtenTableIds,
            @JsonProperty("addedColumns") List<TableColumn> addedColumns)
    {
        this.createdTableIds = ImmutableList.copyOf(requireNonNull(createdTableIds, "createdTableIds is null"));
        this.writtenTableIds = ImmutableList.copyOf(requireNonNull(writtenTableIds, "modifiedTableIds is null"));
        this.addedColumns = ImmutableList.copyOf(requireNonNull(addedColumns, "addedColumns is null"));
    }

    @JsonProperty
    public List<Long> getCreatedTableIds()
    {
        return createdTableIds;
    }

    @JsonProperty
    public List<Long> getWrittenTableIds()
    {
        return writtenTableIds;
    }

    @JsonProperty
    public List<TableColumn> getAddedColumns()
    {
        return addedColumns;
    }

    public RollbackInfo withCreatedTable(long tableId)
    {
        return new RollbackInfo(
                ImmutableList.<Long>builder()
                        .addAll(createdTableIds)
                        .add(tableId)
                        .build(),
                writtenTableIds,
                addedColumns);
    }

    public RollbackInfo withWrittenTable(long tableId)
    {
        return new RollbackInfo(
                createdTableIds,
                ImmutableList.<Long>builder()
                        .addAll(writtenTableIds)
                        .add(tableId)
                        .build(),
                addedColumns);
    }

    public RollbackInfo withAddedColumn(long tableId, long columnId)
    {
        return new RollbackInfo(
                createdTableIds,
                writtenTableIds,
                ImmutableList.<TableColumn>builder()
                        .addAll(addedColumns)
                        .add(new TableColumn(tableId, columnId))
                        .build());
    }

    public byte[] toBytes()
    {
        return CODEC.toJsonBytes(this);
    }

    public static RollbackInfo fromBytes(Optional<byte[]> bytes)
    {
        return bytes.map(CODEC::fromJson)
                .orElseGet(() -> new RollbackInfo(ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));
    }

    public static class TableColumn
    {
        private final long tableId;
        private final long columnId;

        @JsonCreator
        public TableColumn(
                @JsonProperty("tableId") long tableId,
                @JsonProperty("columnId") long columnId)
        {
            this.tableId = tableId;
            this.columnId = columnId;
        }

        @JsonProperty
        public long getTableId()
        {
            return tableId;
        }

        @JsonProperty
        public long getColumnId()
        {
            return columnId;
        }
    }
}
