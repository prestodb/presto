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
package io.prestosql.connector.system.jdbc;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.InMemoryRecordSet.Builder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Optional;

import static io.prestosql.connector.system.SystemConnectorSessionUtil.toSession;
import static io.prestosql.connector.system.jdbc.FilterUtil.filter;
import static io.prestosql.metadata.MetadataListing.listCatalogs;
import static io.prestosql.metadata.MetadataListing.listSchemas;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class SchemaJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "schemas");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_schem", createUnboundedVarcharType())
            .column("table_catalog", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public SchemaJdbcTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = toSession(transactionHandle, connectorSession);
        Optional<String> catalogFilter = FilterUtil.stringFilter(constraint, 1);

        Builder table = InMemoryRecordSet.builder(METADATA);
        for (String catalog : filter(listCatalogs(session, metadata, accessControl).keySet(), catalogFilter)) {
            for (String schema : listSchemas(session, metadata, accessControl, catalog)) {
                table.addRow(schema, catalog);
            }
        }
        return table.build().cursor();
    }
}
