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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.metadata.ChunkMetadata;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimaps;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.raptorx.metadata.IndexWriter.chunkIndexTable;
import static com.facebook.presto.raptorx.metadata.IndexWriter.jdbcType;
import static com.facebook.presto.raptorx.metadata.IndexWriter.maxColumn;
import static com.facebook.presto.raptorx.metadata.IndexWriter.minColumn;
import static com.facebook.presto.raptorx.metadata.ShardHashing.dbShard;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toSet;

public class ChunkOrganizerUtil
{
    private ChunkOrganizerUtil() {}

    public static Collection<ChunkIndexInfo> getOrganizationEligibleChunks(
            List<Jdbi> shardDbi,
            TableInfo tableInfo,
            Collection<ChunkMetadata> chunks,
            boolean includeSortColumns)
    {
        Map<Long, ChunkMetadata> chunksById = uniqueIndex(chunks, ChunkMetadata::getChunkId);
        long tableId = tableInfo.getTableId();

        ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();
        columnsBuilder.add("chunk_id");

        // include temporal columns if present
        Optional<ColumnInfo> temporalColumn = Optional.empty();
        if (tableInfo.getTemporalColumnId().isPresent()) {
            temporalColumn = tableInfo.getTemporalColumn();
            columnsBuilder.add(minColumn(temporalColumn.get().getColumnId()), maxColumn(temporalColumn.get().getColumnId()));
        }

        // include sort columns if needed
        Optional<List<ColumnInfo>> sortColumns = Optional.empty();
        if (includeSortColumns) {
            sortColumns = Optional.of(tableInfo.getSortColumns());
            for (ColumnInfo column : sortColumns.get()) {
                columnsBuilder.add(minColumn(column.getColumnId()), maxColumn(column.getColumnId()));
            }
        }
        String columnToSelect = Joiner.on(",\n").join(columnsBuilder.build());

        ImmutableList.Builder<ChunkIndexInfo> indexInfoBuilder = ImmutableList.builder();

        Jdbi dbi = shardDbi.get(dbShard(tableId, shardDbi.size()));
        try (Connection connection = dbi.open().getConnection()) {
            for (List<ChunkMetadata> partitionedChunks : partition(chunks, 1000)) {
                String chunkIds = Joiner.on(",").join(nCopies(partitionedChunks.size(), "?"));
                String sql = format("" +
                                "SELECT %s\n" +
                                "FROM %s\n" +
                                "WHERE chunk_id IN (%s) AND end_commit_id is NULL",
                        columnToSelect, chunkIndexTable(tableId), chunkIds);

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < partitionedChunks.size(); i++) {
                        statement.setLong(i + 1, partitionedChunks.get(i).getChunkId());
                    }
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            long chunkId = resultSet.getLong("chunk_id");

                            Optional<ChunkRange> sortRange = Optional.empty();
                            if (includeSortColumns) {
                                sortRange = getChunkRange(sortColumns.get(), resultSet);
                                if (!sortRange.isPresent()) {
                                    continue;
                                }
                            }
                            Optional<ChunkRange> temporalRange = Optional.empty();
                            if (temporalColumn.isPresent()) {
                                temporalRange = getChunkRange(ImmutableList.of(temporalColumn.get()), resultSet);
                                if (!temporalRange.isPresent()) {
                                    continue;
                                }
                            }
                            ChunkMetadata chunkMetadata = chunksById.get(chunkId);
                            indexInfoBuilder.add(toChunkIndexInfo(chunkMetadata, temporalRange, sortRange));
                        }
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return indexInfoBuilder.build();
    }

    private static ChunkIndexInfo toChunkIndexInfo(ChunkMetadata shardMetadata, Optional<ChunkRange> temporalRange, Optional<ChunkRange> sortRange)
    {
        return new ChunkIndexInfo(
                shardMetadata.getTableId(),
                shardMetadata.getBucketNumber(),
                shardMetadata.getChunkId(),
                shardMetadata.getRowCount(),
                shardMetadata.getUncompressedSize(),
                sortRange,
                temporalRange);
    }

    public static Collection<Collection<ChunkIndexInfo>> getChunksByDaysBuckets(TableInfo tableInfo, Collection<ChunkIndexInfo> chunks, TemporalFunction temporalFunction)
    {
        if (chunks.isEmpty()) {
            return ImmutableList.of();
        }

        // Neither bucketed nor temporal, no partitioning required
        if (tableInfo.getBucketColumns().isEmpty() && !tableInfo.getTemporalColumnId().isPresent()) {
            return ImmutableList.of(chunks);
        }

        // if only bucketed, partition by bucket number
        if (!tableInfo.getBucketColumns().isEmpty() && !tableInfo.getTemporalColumnId().isPresent()) {
            return Multimaps.index(chunks, chunk -> chunk.getBucketNumber()).asMap().values();
        }

        // if temporal, partition into days first
        ImmutableMultimap.Builder<Long, ChunkIndexInfo> chunksByDaysBuilder = ImmutableMultimap.builder();
        chunks.stream()
                .filter(chunk -> chunk.getTemporalRange().isPresent())
                .forEach(chunk -> {
                    long day = temporalFunction.getDayFromRange(chunk.getTemporalRange().get());
                    chunksByDaysBuilder.put(day, chunk);
                });

        Collection<Collection<ChunkIndexInfo>> byDays = chunksByDaysBuilder.build().asMap().values();

        // if table is bucketed further partition by bucket number
        if (tableInfo.getBucketColumns().isEmpty()) {
            return byDays;
        }

        ImmutableList.Builder<Collection<ChunkIndexInfo>> sets = ImmutableList.builder();
        for (Collection<ChunkIndexInfo> s : byDays) {
            sets.addAll(Multimaps.index(s, ChunkIndexInfo::getBucketNumber).asMap().values());
        }
        return sets.build();
    }

    private static Optional<ChunkRange> getChunkRange(List<ColumnInfo> columns, ResultSet resultSet)
            throws SQLException
    {
        ImmutableList.Builder<Object> minValuesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Object> maxValuesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (ColumnInfo tableColumn : columns) {
            long columnId = tableColumn.getColumnId();
            Type type = tableColumn.getType();

            Object min = getValue(resultSet, type, minColumn(columnId));
            Object max = getValue(resultSet, type, maxColumn(columnId));

            if (min == null || max == null) {
                return Optional.empty();
            }

            minValuesBuilder.add(min);
            maxValuesBuilder.add(max);
            typeBuilder.add(type);
        }

        List<Type> types = typeBuilder.build();
        Tuple minTuple = new Tuple(types, minValuesBuilder.build());
        Tuple maxTuple = new Tuple(types, maxValuesBuilder.build());

        return Optional.of(ChunkRange.of(minTuple, maxTuple));
    }

    private static Object getValue(ResultSet resultSet, Type type, String columnName)
            throws SQLException
    {
        JDBCType jdbcType = jdbcType(type);
        Object value = getValue(resultSet, type, columnName, jdbcType);
        return resultSet.wasNull() ? null : value;
    }

    private static Object getValue(ResultSet resultSet, Type type, String columnName, JDBCType jdbcType)
            throws SQLException
    {
        switch (jdbcType) {
            case BOOLEAN:
                return resultSet.getBoolean(columnName);
            case INTEGER:
                return resultSet.getInt(columnName);
            case BIGINT:
                return resultSet.getLong(columnName);
            case DOUBLE:
                return resultSet.getDouble(columnName);
            case VARBINARY:
                return wrappedBuffer(resultSet.getBytes(columnName)).toStringUtf8();
        }
        throw new IllegalArgumentException("Unhandled type: " + type);
    }

    static OrganizationSet createOrganizationSet(TableInfo tableInfo, Set<ChunkIndexInfo> chunksToCompact)
    {
        Set<Long> chunkIds = chunksToCompact.stream()
                .map(ChunkIndexInfo::getChunkId)
                .collect(toSet());

        Set<Integer> bucketNumber = chunksToCompact.stream()
                .map(ChunkIndexInfo::getBucketNumber)
                .collect(toSet());

        checkArgument(bucketNumber.size() == 1);
        return new OrganizationSet(tableInfo, chunkIds, getOnlyElement(bucketNumber));
    }
}
