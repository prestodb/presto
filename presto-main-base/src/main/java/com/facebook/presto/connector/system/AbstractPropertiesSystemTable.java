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
package com.facebook.presto.connector.system;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

abstract class AbstractPropertiesSystemTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final TransactionManager transactionManager;
    private final Supplier<Map<ConnectorId, Map<String, PropertyMetadata<?>>>> propertySupplier;

    protected AbstractPropertiesSystemTable(String tableName, TransactionManager transactionManager, Supplier<Map<ConnectorId, Map<String, PropertyMetadata<?>>>> propertySupplier)
    {
        this.tableMetadata = tableMetadataBuilder(new SchemaTableName("metadata", tableName))
                .column("catalog_name", createUnboundedVarcharType())
                .column("property_name", createUnboundedVarcharType())
                .column("default_value", createUnboundedVarcharType())
                .column("type", createUnboundedVarcharType())
                .column("description", createUnboundedVarcharType())
                .column("is_deprecated", BOOLEAN)
                .build();
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.propertySupplier = requireNonNull(propertySupplier, "propertySupplier is null");
    }

    @Override
    public final Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public final ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public final RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        TransactionId transactionId = ((GlobalSystemTransactionHandle) transactionHandle).getTransactionId();

        /*
        Get the list of catalogs. Catalog object will have CatalogContext which can help in retrieving the iceberg connector.
        Catalog object has the Connector object as well.
        Once you have the connector, get the deprecatedTableProperties and populate is_deprecated column.
         */
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(tableMetadata);
        Map<ConnectorId, Map<String, PropertyMetadata<?>>> connectorProperties = propertySupplier.get();
        for (Catalog catalog : transactionManager.getCatalogs(transactionId)) {
            String catalogName = catalog.getCatalogName();
            ConnectorId connectorId = catalog.getConnectorId();
            Map<String, PropertyMetadata<?>> properties = new TreeMap<>(connectorProperties.getOrDefault(connectorId, ImmutableMap.of()));
            boolean isDeprecated;
            Set<String> deprecatedPropertyNames = catalog.getConnector(connectorId).getDeprecatedTableProperties().stream().map(PropertyMetadata::getName).collect(Collectors.toSet());
            for (PropertyMetadata<?> propertyMetadata : properties.values()) {
                isDeprecated = deprecatedPropertyNames.contains(propertyMetadata.getName());
                addTableRow(table, catalogName, isDeprecated, propertyMetadata);
            }
        }
        return table.build().cursor();
    }

    private void addTableRow(InMemoryRecordSet.Builder table,
                             String catalogName,
                             boolean isDeprecated,
                             PropertyMetadata<?> propertyMetadata)
    {
        table.addRow(
                catalogName,
                propertyMetadata.getName(),
                firstNonNull(propertyMetadata.getDefaultValue(), "").toString(),
                propertyMetadata.getSqlType().toString(),
                propertyMetadata.getDescription(),
                isDeprecated);
    }
}
