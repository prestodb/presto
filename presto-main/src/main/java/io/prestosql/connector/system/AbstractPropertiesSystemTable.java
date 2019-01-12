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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.ConnectorId;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
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

        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(tableMetadata);
        Map<ConnectorId, Map<String, PropertyMetadata<?>>> connectorProperties = propertySupplier.get();
        for (Entry<String, ConnectorId> entry : new TreeMap<>(transactionManager.getCatalogNames(transactionId)).entrySet()) {
            String catalog = entry.getKey();
            Map<String, PropertyMetadata<?>> properties = new TreeMap<>(connectorProperties.getOrDefault(entry.getValue(), ImmutableMap.of()));
            for (PropertyMetadata<?> propertyMetadata : properties.values()) {
                table.addRow(
                        catalog,
                        propertyMetadata.getName(),
                        firstNonNull(propertyMetadata.getDefaultValue(), "").toString(),
                        propertyMetadata.getSqlType().toString(),
                        propertyMetadata.getDescription());
            }
        }
        return table.build().cursor();
    }
}
