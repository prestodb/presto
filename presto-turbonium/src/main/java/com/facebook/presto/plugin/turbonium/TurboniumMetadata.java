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

package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.plugin.turbonium.config.TurboniumConfigManager;
import com.facebook.presto.plugin.turbonium.config.db.TurboniumConfigSpec;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public class TurboniumMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(TurboniumMetadata.class);
    public static final String SCHEMA_NAME = "memory";
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final NodeManager nodeManager;
    private final String connectorId;
    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<String, Long> tableIds = new HashMap<>();
    private final Map<Long, TurboniumTableHandle> tables = new HashMap<>();
    private final TurboniumConfigManager configManager;

    @Inject
    public TurboniumMetadata(NodeManager nodeManager, TurboniumConnectorId connectorId, TurboniumConfigManager configManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.configManager = requireNonNull(configManager, "configManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            lock.readLock().lock();
            Long tableId = tableIds.get(tableName.getTableName());
            if (tableId == null) {
                return null;
            }
            return tables.get(tableId);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TurboniumTableHandle turboniumTableHandle = (TurboniumTableHandle) tableHandle;
        return turboniumTableHandle.toTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull != null && !schemaNameOrNull.equals(SCHEMA_NAME)) {
            return ImmutableList.of();
        }
        try {
            lock.readLock().lock();
            return tables.values().stream()
                    .map(TurboniumTableHandle::toSchemaTableName)
                    .collect(toList());
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TurboniumTableHandle turboniumTableHandle = (TurboniumTableHandle) tableHandle;
        return turboniumTableHandle.getColumnHandles().stream()
                .collect(toMap(TurboniumColumnHandle::getName, Function.identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        TurboniumColumnHandle turboniumColumnHandle = (TurboniumColumnHandle) columnHandle;
        return turboniumColumnHandle.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        try {
            lock.readLock().lock();
            return tables.values().stream()
                    .filter(table -> prefix.matches(table.toSchemaTableName()))
                    .collect(toMap(TurboniumTableHandle::toSchemaTableName, handle -> handle.toTableMetadata().getColumns()));
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            lock.writeLock().lock();
            TurboniumTableHandle handle = (TurboniumTableHandle) tableHandle;
            Long tableId = tableIds.remove(handle.getTableName());
            if (tableId != null) {
                tables.remove(tableId);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        try {
            lock.writeLock().lock();
            TurboniumTableHandle oldTableHandle = (TurboniumTableHandle) tableHandle;
            TurboniumTableHandle newTableHandle = new TurboniumTableHandle(
                    oldTableHandle.getConnectorId(),
                    oldTableHandle.getSchemaName(),
                    newTableName.getTableName(),
                    oldTableHandle.getTableId(),
                    oldTableHandle.getColumnHandles(),
                    oldTableHandle.getHosts(),
                    oldTableHandle.getSplitsPerWorker());
            tableIds.remove(oldTableHandle.getTableName());
            tableIds.put(newTableName.getTableName(), oldTableHandle.getTableId());
            tables.remove(oldTableHandle.getTableId());
            tables.put(oldTableHandle.getTableId(), newTableHandle);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of());
    }

    @Override
    public TurboniumOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        try {
            lock.writeLock().lock();
            long nextId = nextTableId.getAndIncrement();
            Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
            checkState(!nodes.isEmpty(), "No Memory nodes available");
            Optional<TurboniumPartitioningHandle> partitioningHandle = layout
                    .map(ConnectorNewTableLayout::getPartitioning)
                    .map(TurboniumPartitioningHandle.class::cast);
            checkState(partitioningHandle.isPresent(), "Invalid table layout, no partitioning");
            long splitsPerWorker = partitioningHandle.get().getSplitsPerWorker();
            List<String> nodeIds = partitioningHandle.get().getBucketToNode();
            Map<String, Node> nodesById = uniqueIndex(nodes, Node::getNodeIdentifier);
            ImmutableList.Builder<HostAddress> hosts = ImmutableList.builder();
            for (String nodeId : nodeIds) {
                Node node = nodesById.get(nodeId);
                if (node == null) {
                    throw new PrestoException(NO_NODES_AVAILABLE, "Node for bucket is offline: " + nodeId);
                }
                hosts.add(node.getHostAndPort());
            }
            tableIds.put(tableMetadata.getTable().getTableName(), nextId);
            TurboniumConfigSpec config = configManager.getStaticConfig();
            TurboniumTableHandle table = new TurboniumTableHandle(
                    connectorId,
                    nextId,
                    tableMetadata,
                    hosts.build(),
                    splitsPerWorker);
            tables.put(table.getTableId(), table);
            return new TurboniumOutputTableHandle(table, ImmutableSet.copyOf(tableIds.values()), config);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }

    @Override
    public TurboniumInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TurboniumTableHandle turboniumTableHandle = (TurboniumTableHandle) tableHandle;
        // Preserve splitsPerWorker that table was created with
        TurboniumConfigSpec spec = configManager.getStaticConfig();
        TurboniumConfigSpec newSpec = new TurboniumConfigSpec(spec.getMaxDataPerNode(), spec.getMaxTableSizePerNode(), turboniumTableHandle.getSplitsPerWorker(), spec.getDisableEncoding());
        return new TurboniumInsertTableHandle(turboniumTableHandle, ImmutableSet.copyOf(tableIds.values()), newSpec);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        requireNonNull(handle, "handle is null");
        checkArgument(handle instanceof TurboniumTableHandle);
        TurboniumTableHandle tableHandle = (TurboniumTableHandle) handle;
        log.info("COLUMN CONSTRAINT: %s\nDOMAINS %s\nPREDICATE: %s\nDESIRED COLUMNS %s", constraint.getSummary().getColumnDomains(), constraint.getSummary().getDomains(), constraint.predicate(), desiredColumns);
        log.info("COLUMN HANDLES: %s", ((TurboniumTableHandle) handle).getColumnHandles());
        return ImmutableList.of(new ConnectorTableLayoutResult(getTableLayout(session, tableHandle, constraint.getSummary()), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        TurboniumTableLayoutHandle layoutHandle = (TurboniumTableLayoutHandle) handle;
        return getTableLayout(session, layoutHandle.getTable(), layoutHandle.getConstraint());
    }

    private ConnectorTableLayout getTableLayout(ConnectorSession session, TurboniumTableHandle handle, TupleDomain<ColumnHandle> constraint)
    {
        return new ConnectorTableLayout(
                new TurboniumTableLayoutHandle(handle, constraint),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> partitionColumns = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            partitionColumns.add(column.getName());
        }
        ImmutableList.Builder<String> bucketToNode = ImmutableList.builder();
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        int splitsPerNode = configManager.getConfig().getSplitsPerNode();
        List<Node> nodeList = new ArrayList<>(nodes);
        Collections.shuffle(nodeList);
        nodeList.stream().forEach(node -> bucketToNode.add(node.getNodeIdentifier()));
        return Optional.of(new ConnectorNewTableLayout(new TurboniumPartitioningHandle(bucketToNode.build(), splitsPerNode), partitionColumns.build()));
    }

    public synchronized Map<String, Long> getTableIds()
    {
        try {
            lock.readLock().lock();
            return ImmutableMap.copyOf(tableIds);
        }
        finally {
            lock.readLock().unlock();
        }
    }
}
