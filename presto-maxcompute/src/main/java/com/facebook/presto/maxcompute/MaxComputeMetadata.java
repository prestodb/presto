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
package com.facebook.presto.maxcompute;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.maxcompute.MaxComputeHandleResolver.convertColumnHandle;
import static com.facebook.presto.maxcompute.MaxComputeHandleResolver.convertLayout;
import static com.facebook.presto.maxcompute.MaxComputeHandleResolver.convertTableHandle;
import static com.facebook.presto.maxcompute.util.DataTypes.convertToPrestoType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Manages the MaxCompute connector specific metadata information.
 */
public class MaxComputeMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(MaxComputeMetadata.class);

    private final String catalogName;
    private final MaxComputeClient maxComputeClient;
    private final MaxComputeConfig maxComputeConfig;
    private final TypeManager typeManager;
    private final JsonCodec<ViewDefinition> codec;

    @Inject
    MaxComputeMetadata(
            MaxComputeConnectorId connectorId,
            MaxComputeClient maxComputeClient,
            TypeManager typeManager,
            MaxComputeConfig maxComputeConfig,
            JsonCodec<ViewDefinition> codec)
    {
        this.catalogName = requireNonNull(connectorId, "connectorId is null").toString();
        this.maxComputeClient = requireNonNull(maxComputeClient, "odpsClient is null");
        this.maxComputeConfig = requireNonNull(maxComputeConfig, "odpsConfig is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.codec = requireNonNull(codec, "codec is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        Odps odps = maxComputeClient.createOdps();
        Project project = null;
        try {
            project = odps.projects().get(schemaName);
            project.reload();
        }
        catch (OdpsException e) {
            log.warn(e, "error getting project : %s", schemaName);
            return false;
        }
        return project != null;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        List<String> schemaNames = new ArrayList<>();
        Odps odps = maxComputeClient.createOdps();
        odps.projects().iterable(null).forEach(p -> schemaNames.add(p.getName()));
        if (!schemaNames.contains(maxComputeConfig.getDefaultProject())) { // in case user does not hava permission to list the default project
            schemaNames.add(maxComputeConfig.getDefaultProject());
        }
        return schemaNames;
    }

    @Override
    public MaxComputeTableHandle getTableHandle(ConnectorSession session,
                                                SchemaTableName schemaTableName)
    {
        String project = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<MaxComputeColumnHandle> orderColumnHandles = getOrderedColumns(session, schemaTableName);
        Table table = maxComputeClient.getTableMeta(project, tableName);

        log.info("table modified time of %s is %s", table.getName(), table.getLastDataModifiedTime());
        return new MaxComputeTableHandle(
                catalogName,
                tableName,
                project,
                orderColumnHandles,
                table.getLastDataModifiedTime());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MaxComputeTableHandle oth = convertTableHandle(tableHandle);

        List<ColumnMetadata> metadataList = oth.getOrderedColumnHandles().stream()
                .map(MaxComputeColumnHandle::getColumnMetadata)
                .collect(Collectors.toList());

        return new ConnectorTableMetadata(oth.getSchemaTableName(), metadataList);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        MaxComputeTableHandle tableHandle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new MaxComputeTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        MaxComputeTableLayoutHandle layout = convertLayout(handle);

        // tables in this connector have a single layout
        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        List<SchemaTableName> ret = new ArrayList<>();
        Odps odps = maxComputeClient.createOdps(schemaNameOrNull);
        odps.tables().iterable().forEach(t -> ret.add(new SchemaTableName(t.getProject(), t.getName())));

        return ret;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MaxComputeTableHandle maxComputeTableHandle = convertTableHandle(tableHandle);
        log.info("getColumnHandles: odpsTableHandle: %s", tableHandle);

        return maxComputeTableHandle.getOrderedColumnHandles()
                .stream().collect(Collectors.toMap(MaxComputeColumnHandle::getColumnName, Function.identity()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getOrderedColumns(session, tableName).stream()
                        .map(MaxComputeColumnHandle::getColumnMetadata).collect(toList()));
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }

        Map<SchemaTableName, List<ColumnMetadata>> ret = columns.build();
        log.info("listTableColumns: tableColumns for %s is: %s", prefix, ret);
        return ret;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return maxComputeClient.beginInsertTable(session, this, getTableMetadata(session, tableHandle));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    public List<MaxComputeColumnHandle> getOrderedColumns(ConnectorSession session,
                                                          SchemaTableName schemaTableName)
    {
        String project = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        Table table = maxComputeClient.getTableMeta(project, tableName);
        TableSchema tableSchema = table.getSchema();

        List<MaxComputeColumnHandle> orderColumnHandles = new ArrayList<>();
        int originalColumnIndex = 0;
        for (Column c : tableSchema.getColumns()) {
            String columnName = c.getName();
            Type type = convertToPrestoType(c.getType().toString(), typeManager);

            orderColumnHandles.add(
                    new MaxComputeColumnHandle("odps", columnName, type, originalColumnIndex,
                            false));
            originalColumnIndex++;
        }
        for (Column c : tableSchema.getPartitionColumns()) {
            String columnName = c.getName();
            Type type = convertToPrestoType(c.getType().toString(), typeManager);

            orderColumnHandles.add(
                    new MaxComputeColumnHandle("odps", columnName, type, originalColumnIndex,
                            true));
            originalColumnIndex++;
        }
        log.info("getColumns for table[%s]: %s", schemaTableName, orderColumnHandles);
        return orderColumnHandles;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        String project = prefix.getSchemaName();
        String tableName = prefix.getTableName();
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> viewDefinitionMap
                = ImmutableMap.builder();
        Table table = maxComputeClient.getTableMeta(project, tableName);
        if (table != null && table.isVirtualView()) {
            viewDefinitionMap.put(prefix.toSchemaTableName(), createTableViewDefinition(session, prefix.toSchemaTableName(), table));
        }
        return viewDefinitionMap.build();
    }

    private ConnectorViewDefinition createTableViewDefinition(ConnectorSession session, SchemaTableName tableName, Table table)
    {
        ImmutableList.Builder<ViewDefinition.ViewColumn> columns = ImmutableList.builder();
        for (Column column : table.getSchema().getColumns()) {
            columns.add(new ViewDefinition.ViewColumn(column.getName(), convertToPrestoType(column.getType().toString(), typeManager)));
        }
        log.info("view definiton of %s : %s", table.getName(), table.getViewText());
        ViewDefinition viewDefinition = new ViewDefinition(table.getViewText(),
                Optional.ofNullable(catalogName),
                Optional.ofNullable(tableName.getSchemaName()),
                columns.build(),
                Optional.of(session.getUser()),
                false);
        return new ConnectorViewDefinition(tableName, Optional.ofNullable(session.getUser()), codec.toJson(viewDefinition));
    }
}
