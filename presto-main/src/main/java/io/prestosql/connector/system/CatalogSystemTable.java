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

import io.prestosql.Session;
import io.prestosql.connector.ConnectorId;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.InMemoryRecordSet.Builder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Map;

import static io.prestosql.connector.system.SystemConnectorSessionUtil.toSession;
import static io.prestosql.metadata.MetadataListing.listCatalogs;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class CatalogSystemTable
        implements SystemTable
{
    public static final SchemaTableName CATALOG_TABLE_NAME = new SchemaTableName("metadata", "catalogs");

    public static final ConnectorTableMetadata CATALOG_TABLE = tableMetadataBuilder(CATALOG_TABLE_NAME)
            .column("catalog_name", createUnboundedVarcharType())
            .column("connector_id", createUnboundedVarcharType())
            .build();
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public CatalogSystemTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return CATALOG_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = toSession(transactionHandle, connectorSession);
        Builder table = InMemoryRecordSet.builder(CATALOG_TABLE);
        for (Map.Entry<String, ConnectorId> entry : listCatalogs(session, metadata, accessControl).entrySet()) {
            table.addRow(entry.getKey(), entry.getValue().toString());
        }
        return table.build().cursor();
    }
}
