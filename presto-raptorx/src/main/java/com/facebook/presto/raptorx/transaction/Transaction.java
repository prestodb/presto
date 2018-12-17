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
package com.facebook.presto.raptorx.transaction;

import com.facebook.presto.raptorx.RaptorColumnHandle;
import com.facebook.presto.raptorx.metadata.BucketChunks;
import com.facebook.presto.raptorx.metadata.ChunkMetadata;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.DistributionInfo;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.SchemaInfo;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.metadata.TableStats;
import com.facebook.presto.raptorx.metadata.ViewInfo;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.util.CloseableIterator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

@NotThreadSafe
public class Transaction
{
    private final Metadata metadata;

    private final long transactionId;
    private final long commitId;

    private final Set<Long> registeredTableIds = new HashSet<>();

    private final List<Action> actions = new ArrayList<>();

    private final Map<Long, Optional<SchemaInfo>> schemaInfos = new HashMap<>();
    private final Map<String, Optional<Long>> schemaIds = new HashMap<>();
    private final Set<String> schemasAdded = new HashSet<>();
    private final Set<String> schemasRemoved = new HashSet<>();

    private final Map<Long, Optional<TableInfo>> tableInfos = new HashMap<>();
    private final Map<Long, Map<String, Optional<Long>>> tableIds = new HashMap<>();
    private final SetMultimap<Long, String> tablesAdded = HashMultimap.create();
    private final SetMultimap<Long, String> tablesRemoved = HashMultimap.create();

    private final Map<Long, Optional<ViewInfo>> viewInfos = new HashMap<>();
    private final Map<Long, Map<String, Optional<Long>>> viewIds = new HashMap<>();
    private final SetMultimap<Long, String> viewsAdded = HashMultimap.create();
    private final SetMultimap<Long, String> viewsRemoved = HashMultimap.create();

    private final Map<Long, Map<Long, ChunkInfo>> chunksAdded = new HashMap<>();
    private final Map<Long, Set<Long>> chunksRemoved = new HashMap<>();

    public Transaction(Metadata metadata, long transactionId, long commitId)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.transactionId = transactionId;
        this.commitId = commitId;
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    public long getCommitId()
    {
        return commitId;
    }

    public OptionalLong getRegisteredTransactionId()
    {
        return registeredTableIds.isEmpty() ? OptionalLong.empty() : OptionalLong.of(transactionId);
    }

    public void registerTable(long tableId)
    {
        if (registeredTableIds.isEmpty()) {
            metadata.registerTransaction(transactionId);
        }
        if (registeredTableIds.add(tableId)) {
            metadata.registerTransactionTable(transactionId, tableId);
        }
    }

    public List<Action> getActions()
    {
        return ImmutableList.copyOf(actions);
    }

    public long nextTableId()
    {
        return metadata.nextTableId();
    }

    public long nextColumnId()
    {
        return metadata.nextColumnId();
    }

    public Optional<Long> getSchemaId(String schemaName)
    {
        return schemaIds.computeIfAbsent(schemaName, x -> metadata.getSchemaId(commitId, schemaName));
    }

    public Optional<Long> getTableId(SchemaTableName table)
    {
        return getSchemaId(table.getSchemaName())
                .flatMap(schemaId -> getTableId(schemaId, table.getTableName()));
    }

    private Optional<Long> getTableId(long schemaId, String tableName)
    {
        return tableIds.computeIfAbsent(schemaId, x -> new HashMap<>())
                .computeIfAbsent(tableName, x -> metadata.getTableId(commitId, schemaId, tableName));
    }

    public Optional<Long> getViewId(SchemaTableName view)
    {
        return getSchemaId(view.getSchemaName())
                .flatMap(schemaId -> getViewId(schemaId, view.getTableName()));
    }

    private Optional<Long> getViewId(long schemaId, String viewName)
    {
        return viewIds.computeIfAbsent(schemaId, x -> new HashMap<>())
                .computeIfAbsent(viewName, x -> metadata.getViewId(commitId, schemaId, viewName));
    }

    public List<String> listSchemas()
    {
        return concat(
                metadata.listSchemas(commitId).stream(),
                schemasAdded.stream())
                .filter(not(schemasRemoved::contains))
                .sorted()
                .collect(toList());
    }

    public List<SchemaTableName> listTables()
    {
        // TODO: this should fetch all tables at once
        return listSchemas().stream()
                .flatMap(schema -> listTables(schema).stream())
                .collect(toList());
    }

    public List<SchemaTableName> listTables(String schemaName)
    {
        return getSchemaId(schemaName)
                .map(schemaId -> concat(
                        metadata.listTables(commitId, schemaId).stream(),
                        tablesAdded.get(schemaId).stream())
                        .filter(not(tablesRemoved.get(schemaId)::contains))
                        .sorted()
                        .map(tableName -> new SchemaTableName(schemaName, tableName))
                        .collect(toList()))
                .orElse(emptyList());
    }

    public List<SchemaTableName> listViews()
    {
        // TODO: this should fetch all views at once
        return listSchemas().stream()
                .flatMap(schema -> listViews(schema).stream())
                .collect(toList());
    }

    public List<SchemaTableName> listViews(String schemaName)
    {
        return getSchemaId(schemaName)
                .map(schemaId -> concat(
                        metadata.listViews(commitId, schemaId).stream(),
                        viewsAdded.get(schemaId).stream())
                        .filter(not(viewsRemoved.get(schemaId)::contains))
                        .sorted()
                        .map(viewName -> new SchemaTableName(schemaName, viewName))
                        .collect(toList()))
                .orElse(emptyList());
    }

    public void createSchema(String schemaName)
    {
        long schemaId = metadata.nextSchemaId();
        actions.add(new CreateSchemaAction(schemaId, schemaName));

        schemaInfos.put(schemaId, Optional.of(new SchemaInfo(schemaId, schemaName)));
        schemaIds.put(schemaName, Optional.of(schemaId));
        schemasAdded.add(schemaName);
        schemasRemoved.remove(schemaName);
    }

    public void dropSchema(String schemaName)
    {
        long schemaId = getRequiredSchemaId(schemaName);
        actions.add(new DropSchemaAction(schemaId));

        schemaInfos.put(schemaId, Optional.empty());
        schemaIds.put(schemaName, Optional.empty());
        schemasAdded.remove(schemaName);
        schemasRemoved.add(schemaName);
    }

    public void renameSchema(String source, String target)
    {
        long schemaId = getRequiredSchemaId(source);
        actions.add(new RenameSchemaAction(schemaId, target));

        SchemaInfo newInfo = new SchemaInfo(schemaId, target);
        schemaInfos.put(schemaId, Optional.of(newInfo));

        schemaIds.put(source, Optional.empty());
        schemasAdded.remove(source);
        schemasRemoved.add(source);

        schemaIds.put(target, Optional.of(schemaId));
        schemasAdded.add(target);
        schemasRemoved.remove(target);
    }

    public long createDistribution(List<Type> columnTypes, List<Long> bucketNodes)
    {
        return metadata.createDistribution(Optional.empty(), columnTypes, bucketNodes);
    }

    public DistributionInfo getDistributionInfo(long distributionId)
    {
        return metadata.getDistributionInfo(distributionId);
    }

    public Optional<DistributionInfo> getDistributionInfo(String distributionName)
    {
        return metadata.getDistributionInfo(distributionName);
    }

    public void createTable(
            long tableId,
            long schemaId,
            String tableName,
            long distributionId,
            OptionalLong temporalColumnId,
            CompressionType compressionType,
            long createTime,
            Optional<String> comment,
            List<ColumnInfo> columns,
            boolean organized)
    {
        TableInfo info = new TableInfo.Builder()
                .setTableId(tableId)
                .setTableName(tableName)
                .setSchemaId(schemaId)
                .setDistributionId(distributionId)
                .setTemporalColumnId(temporalColumnId)
                .setCompressionType(compressionType)
                .setCreateTime(createTime)
                .setUpdateTime(createTime)
                .setRowCount(0)
                .setComment(comment)
                .setColumns(columns)
                .setOrganized(organized)
                .build();
        actions.add(new CreateTableAction(info));

        tableInfos.put(tableId, Optional.of(info));
        tableIds.computeIfAbsent(schemaId, x -> new HashMap<>())
                .put(tableName, Optional.of(tableId));
        tablesAdded.put(schemaId, tableName);
        tablesRemoved.remove(schemaId, tableName);
    }

    public void dropTable(long tableId)
    {
        TableInfo info = getTableInfo(tableId);
        actions.add(new DropTableAction(tableId));

        tableInfos.put(tableId, Optional.empty());
        tableIds.computeIfAbsent(info.getSchemaId(), x -> new HashMap<>())
                .put(info.getTableName(), Optional.empty());
        tablesAdded.remove(info.getSchemaId(), info.getTableName());
        tablesRemoved.put(info.getSchemaId(), info.getTableName());
    }

    public void renameTable(long tableId, SchemaTableName newName)
    {
        TableInfo oldInfo = getTableInfo(tableId);
        long newSchemaId = getRequiredSchemaId(newName.getSchemaName());
        TableInfo newInfo = oldInfo.builder()
                .setTableName(newName.getTableName())
                .setSchemaId(newSchemaId)
                .setUpdateTime(System.currentTimeMillis())
                .build();

        actions.add(new RenameTableAction(tableId, newSchemaId, newName.getTableName()));

        tableInfos.put(tableId, Optional.of(newInfo));

        tableIds.computeIfAbsent(oldInfo.getSchemaId(), x -> new HashMap<>())
                .put(oldInfo.getTableName(), Optional.empty());
        tablesAdded.remove(oldInfo.getSchemaId(), oldInfo.getTableName());
        tablesRemoved.put(oldInfo.getSchemaId(), oldInfo.getTableName());

        tableIds.computeIfAbsent(newSchemaId, x -> new HashMap<>())
                .put(newName.getTableName(), Optional.of(tableId));
        tablesAdded.put(newSchemaId, newName.getTableName());
        tablesRemoved.remove(newSchemaId, newName.getTableName());
    }

    public void addColumn(long tableId, String columnName, Type type, Optional<String> comment)
    {
        TableInfo oldInfo = getTableInfo(tableId);

        int maxOrdinal = oldInfo.getColumns().stream()
                .mapToInt(ColumnInfo::getOrdinal).max()
                .orElseThrow(() -> new PrestoException(RAPTOR_INTERNAL_ERROR, "Table has no columns"));

        ColumnInfo columnInfo = new ColumnInfo(
                nextColumnId(),
                columnName,
                type,
                comment,
                maxOrdinal + 1,
                OptionalInt.empty(),
                OptionalInt.empty());

        TableInfo newInfo = oldInfo.builder()
                .setColumns(ImmutableList.<ColumnInfo>builder()
                        .addAll(oldInfo.getColumns())
                        .add(columnInfo)
                        .build())
                .setUpdateTime(System.currentTimeMillis())
                .build();

        actions.add(new AddColumnAction(tableId, columnInfo));

        tableInfos.put(tableId, Optional.of(newInfo));
    }

    public void dropColumn(long tableId, long columnId)
    {
        TableInfo oldInfo = getTableInfo(tableId);
        TableInfo newInfo = oldInfo.builder()
                .setColumns(oldInfo.getColumns().stream()
                        .filter(column -> column.getColumnId() != columnId)
                        .collect(toImmutableList()))
                .setUpdateTime(System.currentTimeMillis())
                .build();

        actions.add(new DropColumnAction(tableId, columnId));

        tableInfos.put(tableId, Optional.of(newInfo));
    }

    public void renameColumn(long tableId, long columnId, String newName)
    {
        TableInfo oldInfo = getTableInfo(tableId);
        TableInfo newInfo = oldInfo.builder()
                .setColumns(oldInfo.getColumns().stream()
                        .map(column -> (column.getColumnId() == columnId) ?
                                column.withColumnName(newName) : column)
                        .collect(toImmutableList()))
                .setUpdateTime(System.currentTimeMillis())
                .build();

        actions.add(new RenameColumnAction(tableId, columnId, newName));

        tableInfos.put(tableId, Optional.of(newInfo));
    }

    public void createView(
            long schemaId,
            String viewName,
            String viewData,
            long createTime,
            Optional<String> comment)
    {
        long viewId = metadata.nextViewId();
        ViewInfo info = new ViewInfo(viewId, viewName, schemaId, createTime, createTime, viewData, comment);

        actions.add(new CreateViewAction(info));

        viewInfos.put(viewId, Optional.of(info));
        viewIds.computeIfAbsent(schemaId, x -> new HashMap<>())
                .put(viewName, Optional.of(viewId));
        viewsAdded.put(schemaId, viewName);
        viewsRemoved.remove(schemaId, viewName);
    }

    public void dropView(long viewId)
    {
        ViewInfo info = getViewInfo(viewId);
        actions.add(new DropViewAction(viewId));

        viewInfos.put(viewId, Optional.empty());
        viewIds.computeIfAbsent(info.getSchemaId(), x -> new HashMap<>())
                .put(info.getViewName(), Optional.empty());
        viewsAdded.remove(info.getSchemaId(), info.getViewName());
        viewsRemoved.put(info.getSchemaId(), info.getViewName());
    }

    public void insertChunks(long tableId, Collection<ChunkInfo> chunks)
    {
        if (chunks.isEmpty()) {
            return;
        }

        actions.add(new InsertChunksAction(tableId, chunks));

        Map<Long, ChunkInfo> map = chunksAdded.computeIfAbsent(tableId, x -> new HashMap<>());
        for (ChunkInfo chunk : chunks) {
            map.put(chunk.getChunkId(), chunk);
        }

        tableInfos.put(tableId, Optional.of(getTableInfo(tableId).builder()
                .addRowCount(chunks.stream().mapToLong(ChunkInfo::getRowCount).sum())
                .setUpdateTime(System.currentTimeMillis())
                .build()));
    }

    public void deleteChunks(long tableId, Set<Long> chunkIds)
    {
        if (chunkIds.isEmpty()) {
            return;
        }

        actions.add(new DeleteChunksAction(tableId, chunkIds));

        long rowCount = metadata.getChunkRowCount(commitId, tableId, chunkIds);

        chunkIds = new HashSet<>(chunkIds);
        Map<Long, ChunkInfo> added = chunksAdded.computeIfAbsent(tableId, x -> new HashMap<>());
        Iterator<Long> iterator = chunkIds.iterator();
        while (iterator.hasNext()) {
            long chunkId = iterator.next();
            ChunkInfo chunk = added.remove(chunkId);
            if (chunk != null) {
                rowCount += chunk.getRowCount();
                iterator.remove();
            }
        }

        chunksRemoved.computeIfAbsent(tableId, x -> new HashSet<>()).addAll(chunkIds);

        tableInfos.put(tableId, Optional.of(getTableInfo(tableId).builder()
                .addRowCount(-rowCount)
                .setUpdateTime(System.currentTimeMillis())
                .build()));
    }

    public Collection<ChunkMetadata> getChunks(long tableId)
    {
        Collection<ChunkMetadata> addedChunks = ImmutableList.of();
        Collection<ChunkInfo> addedChunkInfos = chunksAdded.getOrDefault(tableId, ImmutableMap.of()).values();
        if (!addedChunkInfos.isEmpty()) {
            TableInfo table = getTableInfo(tableId);
            addedChunks = addedChunkInfos.stream()
                    .map(chunk -> ChunkMetadata.from(table, chunk))
                    .collect(toImmutableList());
        }
        Set<Long> deletedChunks = chunksRemoved.getOrDefault(tableId, ImmutableSet.of());
        return metadata.getChunks(commitId, tableId, addedChunks, deletedChunks);
    }

    public CloseableIterator<BucketChunks> getBucketChunks(long tableId, boolean merged, TupleDomain<RaptorColumnHandle> constraint)
    {
        Collection<ChunkInfo> addedChunks = chunksAdded.getOrDefault(tableId, ImmutableMap.of()).values();
        Set<Long> deletedChunks = chunksRemoved.getOrDefault(tableId, ImmutableSet.of());
        TupleDomain<Long> idConstraint = constraint.transform(RaptorColumnHandle::getColumnId);
        return metadata.getBucketChunks(commitId, tableId, addedChunks, deletedChunks, idConstraint, merged);
    }

    public SchemaInfo getSchemaInfo(long schemaId)
    {
        return schemaInfos.computeIfAbsent(schemaId, x -> Optional.of(metadata.getSchemaInfo(commitId, schemaId)))
                .orElseThrow(() -> new PrestoException(RAPTOR_INTERNAL_ERROR, "Invalid schema ID: " + schemaId));
    }

    public TableInfo getTableInfo(long tableId)
    {
        return tableInfos.computeIfAbsent(tableId, x -> Optional.of(metadata.getTableInfo(commitId, tableId)))
                .orElseThrow(() -> new PrestoException(RAPTOR_INTERNAL_ERROR, "Invalid table ID: " + tableId));
    }

    public ViewInfo getViewInfo(long viewId)
    {
        return viewInfos.computeIfAbsent(viewId, x -> Optional.of(metadata.getViewInfo(commitId, viewId)))
                .orElseThrow(() -> new PrestoException(RAPTOR_INTERNAL_ERROR, "Invalid view ID: " + viewId));
    }

    public Collection<TableStats> listTableStats(SchemaTablePrefix prefix)
    {
        if (!actions.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Table stats cannot be accessed with uncommitted modifications");
        }

        Optional<Long> schemaId = Optional.ofNullable(prefix.getSchemaName()).flatMap(this::getSchemaId);
        Optional<Long> tableId = Optional.empty();
        if (schemaId.isPresent() && (prefix.getTableName() != null)) {
            tableId = getTableId(schemaId.get(), prefix.getTableName());
        }

        return metadata.listTableStats(commitId, schemaId, tableId);
    }

    private long getRequiredSchemaId(String schemaName)
    {
        return getSchemaId(schemaName)
                .orElseThrow(() -> new PrestoException(RAPTOR_INTERNAL_ERROR, "Schema does not exist: " + schemaName));
    }

    private static <T> Predicate<T> not(Predicate<T> predicate)
    {
        return predicate.negate();
    }
}
