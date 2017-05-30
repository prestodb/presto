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

import com.facebook.presto.raptorx.storage.CompressionType;
import com.google.common.collect.ImmutableList;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Function;

import static com.facebook.presto.raptorx.util.DatabaseUtil.getOptionalLong;
import static com.facebook.presto.raptorx.util.DatabaseUtil.optionalUtf8String;
import static com.facebook.presto.raptorx.util.DatabaseUtil.utf8String;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

public class TableInfo
{
    private final long tableId;
    private final String tableName;
    private final long schemaId;
    private final long distributionId;
    private final OptionalLong temporalColumnId;
    private final CompressionType compressionType;
    private final long createTime;
    private final long updateTime;
    private final long rowCount;
    private final Optional<String> comment;
    private final List<ColumnInfo> columns;

    public TableInfo(
            long tableId,
            String tableName,
            long schemaId,
            long distributionId,
            OptionalLong temporalColumnId,
            CompressionType compressionType,
            long createTime,
            long updateTime,
            long rowCount,
            Optional<String> comment,
            List<ColumnInfo> columns)
    {
        this.tableId = tableId;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaId = schemaId;
        this.distributionId = distributionId;
        this.temporalColumnId = requireNonNull(temporalColumnId, "temporalColumnId is null");
        this.compressionType = requireNonNull(compressionType, "compressionType is null");
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.rowCount = rowCount;
        this.comment = requireNonNull(comment, "comment is null");
        this.columns = requireNonNull(columns, "columns is null").stream()
                .sorted(comparing(ColumnInfo::getOrdinal))
                .collect(toImmutableList());
        checkArgument(columns.isEmpty() || !temporalColumnId.isPresent() || columns.stream()
                        .map(ColumnInfo::getColumnId)
                        .anyMatch(columnId -> columnId == temporalColumnId.getAsLong()),
                "temporalColumnId not in columns list");
    }

    public long getTableId()
    {
        return tableId;
    }

    public String getTableName()
    {
        return tableName;
    }

    public long getSchemaId()
    {
        return schemaId;
    }

    public long getDistributionId()
    {
        return distributionId;
    }

    public OptionalLong getTemporalColumnId()
    {
        return temporalColumnId;
    }

    public CompressionType getCompressionType()
    {
        return compressionType;
    }

    public long getCreateTime()
    {
        return createTime;
    }

    public long getUpdateTime()
    {
        return updateTime;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    public TableInfo.Builder builder()
    {
        return new TableInfo.Builder(this);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .toString();
    }

    public Optional<ColumnInfo> getTemporalColumn()
    {
        if (!temporalColumnId.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(columns.stream()
                .filter(column -> column.getColumnId() == temporalColumnId.getAsLong())
                .collect(onlyElement()));
    }

    public List<ColumnInfo> getBucketColumns()
    {
        return orderedColumns(ColumnInfo::getBucketOrdinal);
    }

    public List<ColumnInfo> getSortColumns()
    {
        return orderedColumns(ColumnInfo::getSortOrdinal);
    }

    @SuppressWarnings("ConstantConditions")
    private List<ColumnInfo> orderedColumns(Function<ColumnInfo, OptionalInt> function)
    {
        return columns.stream()
                .filter(column -> function.apply(column).isPresent())
                .sorted(comparingInt(column -> function.apply(column).getAsInt()))
                .collect(toImmutableList());
    }

    public static class Mapper
            implements RowMapper<TableInfo>
    {
        @Override
        public TableInfo map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new Builder()
                    .setTableId(rs.getLong("table_id"))
                    .setTableName(utf8String(rs.getBytes("table_name")))
                    .setSchemaId(rs.getLong("schema_id"))
                    .setDistributionId(rs.getLong("distribution_id"))
                    .setTemporalColumnId(getOptionalLong(rs, "temporal_column_id"))
                    .setCompressionType(CompressionType.valueOf(rs.getString("compression_type")))
                    .setCreateTime(rs.getLong("create_time"))
                    .setUpdateTime(rs.getLong("update_time"))
                    .setRowCount(rs.getLong("row_count"))
                    .setComment(optionalUtf8String(rs.getBytes("comment")))
                    .setColumns(ImmutableList.of())
                    .build();
        }
    }

    public static class Builder
    {
        private long tableId;
        private String tableName;
        private long schemaId;
        private long distributionId;
        private OptionalLong temporalColumnId;
        private CompressionType compressionType;
        private long createTime;
        private long updateTime;
        private long rowCount;
        private Optional<String> comment;
        private List<ColumnInfo> columns;

        public Builder() {}

        public Builder(TableInfo source)
        {
            this.tableId = source.getTableId();
            this.tableName = source.getTableName();
            this.schemaId = source.getSchemaId();
            this.distributionId = source.getDistributionId();
            this.temporalColumnId = source.getTemporalColumnId();
            this.compressionType = source.getCompressionType();
            this.createTime = source.getCreateTime();
            this.updateTime = source.getUpdateTime();
            this.rowCount = source.getRowCount();
            this.comment = source.getComment();
            this.columns = ImmutableList.copyOf(source.getColumns());
        }

        public Builder setTableId(long tableId)
        {
            this.tableId = tableId;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setSchemaId(long schemaId)
        {
            this.schemaId = schemaId;
            return this;
        }

        public Builder setDistributionId(long distributionId)
        {
            this.distributionId = distributionId;
            return this;
        }

        public Builder setTemporalColumnId(OptionalLong temporalColumnId)
        {
            this.temporalColumnId = temporalColumnId;
            return this;
        }

        public Builder setCompressionType(CompressionType compressionType)
        {
            this.compressionType = compressionType;
            return this;
        }

        public Builder setCreateTime(long createTime)
        {
            this.createTime = createTime;
            return this;
        }

        public Builder setUpdateTime(long updateTime)
        {
            this.updateTime = updateTime;
            return this;
        }

        public Builder setRowCount(long rowCount)
        {
            this.rowCount = rowCount;
            return this;
        }

        public Builder addRowCount(long rowCount)
        {
            this.rowCount += rowCount;
            return this;
        }

        public Builder setComment(Optional<String> comment)
        {
            this.comment = comment;
            return this;
        }

        public Builder setColumns(List<ColumnInfo> columns)
        {
            this.columns = ImmutableList.copyOf(columns);
            return this;
        }

        public TableInfo build()
        {
            return new TableInfo(
                    tableId,
                    tableName,
                    schemaId,
                    distributionId,
                    temporalColumnId,
                    compressionType,
                    createTime,
                    updateTime,
                    rowCount,
                    comment,
                    columns);
        }
    }
}
