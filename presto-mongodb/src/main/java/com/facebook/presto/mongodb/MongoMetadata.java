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
package com.facebook.presto.mongodb;

import com.facebook.presto.mongodb.MongoIndex.MongodbIndexKey;
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
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.mongodb.TypeUtils.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MongoMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(MongoMetadata.class);

    private final MongoSession mongoSession;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public MongoMetadata(MongoSession mongoSession)
    {
        this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return mongoSession.getAllSchemas();
    }

    @Override
    public MongoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try {
            return mongoSession.getTable(tableName).getTableHandle();
        }
        catch (TableNotFoundException e) {
            log.debug(e, "Table(%s) not found", tableName);
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(session, tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();

        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : mongoSession.getAllTables(schemaName)) {
                tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase(ENGLISH)));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = checkType(tableHandle, MongoTableHandle.class, "tableHandle");
        List<MongoColumnHandle> columns = mongoSession.getTable(table.getSchemaTableName()).getColumns();

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (MongoColumnHandle columnHandle : columns) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, MongoTableHandle.class, "tableHandle");
        return checkType(columnHandle, MongoColumnHandle.class, "columnHandle").toColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        MongoTableHandle tableHandle = checkType(table, MongoTableHandle.class, "table");

        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty(); //TODO: sharding key
        ImmutableList.Builder<LocalProperty<ColumnHandle>> localProperties = ImmutableList.builder();

        MongoTable tableInfo = mongoSession.getTable(tableHandle.getSchemaTableName());
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);

        for (MongoIndex index : tableInfo.getIndexes()) {
            for (MongodbIndexKey key : index.getKeys()) {
                if (!key.getSortOrder().isPresent()) {
                    continue;
                }
                if (columns.get(key.getName()) != null) {
                    localProperties.add(new SortingProperty<>(columns.get(key.getName()), key.getSortOrder().get()));
                }
            }
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(
                new MongoTableLayoutHandle(tableHandle, constraint.getSummary()),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                partitioningColumns,
                Optional.empty(),
                localProperties.build());

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        MongoTableLayoutHandle layout = checkType(handle, MongoTableLayoutHandle.class, "layout");

        // tables in this connector have a single layout
        return getTableLayouts(session, layout.getTable(), Constraint.<ColumnHandle>alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        mongoSession.createTable(tableMetadata.getTable(), buildColumnHandles(tableMetadata));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = checkType(tableHandle, MongoTableHandle.class, "tableHandle");

        mongoSession.dropTable(table.getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        List<MongoColumnHandle> columns = buildColumnHandles(tableMetadata);

        mongoSession.createTable(tableMetadata.getTable(), columns);

        setRollback(() -> mongoSession.dropTable(tableMetadata.getTable()));

        return new MongoOutputTableHandle(
                tableMetadata.getTable(),
                columns.stream().filter(c -> !c.isHidden()).collect(toList()));
    }

    @Override
    public void finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        clearRollback();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MongoTableHandle table = checkType(tableHandle, MongoTableHandle.class, "tableHandle");
        List<MongoColumnHandle> columns = mongoSession.getTable(table.getSchemaTableName()).getColumns();

        return new MongoInsertTableHandle(
                table.getSchemaTableName(),
                columns.stream().filter(c -> !c.isHidden()).collect(toList()));
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, MongoTableHandle.class, "tableHandle").getSchemaTableName();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        MongoTableHandle tableHandle = mongoSession.getTable(tableName).getTableHandle();

        List<ColumnMetadata> columns = ImmutableList.copyOf(
                getColumnHandles(session, tableHandle).values().stream()
                        .map(MongoColumnHandle.class::cast)
                        .map(MongoColumnHandle::toColumnMetadata)
                        .collect(toList()));

        return new ConnectorTableMetadata(tableName, columns);
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    private static List<MongoColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        return tableMetadata.getColumns().stream()
                .map(m -> new MongoColumnHandle(m.getName(), m.getType(), m.isHidden()))
                .collect(toList());
    }
}
