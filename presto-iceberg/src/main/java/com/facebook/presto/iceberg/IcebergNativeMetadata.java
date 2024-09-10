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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.iceberg.util.IcebergPrestoModelConverters;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergTableProperties.getFileFormat;
import static com.facebook.presto.iceberg.IcebergTableProperties.getPartitioning;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.populateTableProperties;
import static com.facebook.presto.iceberg.IcebergUtil.verifyTypeSupported;
import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergNamespace;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class IcebergNativeMetadata
        extends IcebergAbstractMetadata
{
    private static final Logger LOG = Logger.get(IcebergNativeMetadata.class);
    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final String TABLE_COMMENT = "comment";

    private final IcebergNativeCatalogFactory catalogFactory;
    private final CatalogType catalogType;

    public IcebergNativeMetadata(
            IcebergNativeCatalogFactory catalogFactory,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            CatalogType catalogType,
            NodeVersion nodeVersion,
            FilterStatsCalculatorService filterStatsCalculatorService,
            StatisticsFileCache statisticsFileCache)
    {
        super(typeManager, functionResolution, rowExpressionService, commitTaskCodec, nodeVersion, filterStatsCalculatorService, statisticsFileCache);
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
    }

    @Override
    protected Table getRawIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return getNativeIcebergTable(catalogFactory, session, schemaTableName);
    }

    @Override
    protected boolean tableExists(ConnectorSession session, SchemaTableName schemaTableName)
    {
        IcebergTableName name = IcebergTableName.from(schemaTableName.getTableName());
        try {
            getIcebergTable(session, new SchemaTableName(schemaTableName.getSchemaName(), name.getTableName()));
        }
        catch (NoSuchTableException e) {
            // return null to throw
            return false;
        }
        return true;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        SupportsNamespaces supportsNamespaces = catalogFactory.getNamespaces(session);
        return supportsNamespaces.listNamespaces()
                .stream()
                .map(IcebergPrestoModelConverters::toPrestoSchemaName)
                .collect(toList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && INFORMATION_SCHEMA.equals(schemaName.get())) {
            return listSchemaNames(session).stream()
                    .map(schema -> new SchemaTableName(INFORMATION_SCHEMA, schema))
                    .collect(toList());
        }

        return catalogFactory.getCatalog(session).listTables(toIcebergNamespace(schemaName))
                .stream()
                .map(IcebergPrestoModelConverters::toPrestoSchemaTableName)
                .collect(toList());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        catalogFactory.getNamespaces(session).createNamespace(toIcebergNamespace(Optional.of(schemaName)),
                properties.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        try {
            catalogFactory.getNamespaces(session).dropNamespace(toIcebergNamespace(Optional.of(schemaName)));
        }
        catch (NamespaceNotEmptyException e) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, format("Iceberg %s catalog does not support rename namespace", catalogType.name()));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = toIcebergSchema(tableMetadata.getColumns());

        verifyTypeSupported(schema);

        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        FileFormat fileFormat = getFileFormat(tableMetadata.getProperties());

        try {
            transaction = catalogFactory.getCatalog(session).newCreateTableTransaction(
                    toIcebergTableIdentifier(schemaTableName), schema, partitionSpec, populateTableProperties(tableMetadata, fileFormat, session));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        Table icebergTable = transaction.table();
        return new IcebergOutputTableHandle(
                schemaName,
                new IcebergTableName(tableName, DATA, Optional.empty(), Optional.empty()),
                toPrestoSchema(icebergTable.schema(), typeManager),
                toPrestoPartitionSpec(icebergTable.spec(), typeManager),
                getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                icebergTable.location(),
                fileFormat,
                getCompressionCodec(session),
                icebergTable.properties());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        verify(icebergTableHandle.getIcebergTableName().getTableType() == DATA, "only the data table can be dropped");
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(icebergTableHandle.getSchemaTableName());
        catalogFactory.getCatalog(session).dropTable(tableIdentifier);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        verify(icebergTableHandle.getIcebergTableName().getTableType() == DATA, "only the data table can be renamed");
        TableIdentifier from = toIcebergTableIdentifier(icebergTableHandle.getSchemaTableName());
        TableIdentifier to = toIcebergTableIdentifier(newTable);
        catalogFactory.getCatalog(session).renameTable(from, to);
    }

    @Override
    public void registerTable(ConnectorSession clientSession, SchemaTableName schemaTableName, Path metadataLocation)
    {
        catalogFactory.getCatalog(clientSession).registerTable(toIcebergTableIdentifier(schemaTableName), metadataLocation.toString());
    }

    @Override
    public void unregisterTable(ConnectorSession clientSession, SchemaTableName schemaTableName)
    {
        catalogFactory.getCatalog(clientSession).dropTable(toIcebergTableIdentifier(schemaTableName), false);
    }
}
