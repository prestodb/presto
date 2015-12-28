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
package com.facebook.presto.connector.system.jdbc;

import com.facebook.presto.Session;
import com.facebook.presto.connector.system.GlobalSystemTransactionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.connector.system.jdbc.FilterUtil.filter;
import static com.facebook.presto.connector.system.jdbc.FilterUtil.stringFilter;
import static com.facebook.presto.connector.system.jdbc.FilterUtil.tablePrefix;
import static com.facebook.presto.connector.system.jdbc.FilterUtil.toSession;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Types.checkType;
import static java.util.Objects.requireNonNull;

public class TableJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "tables");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_cat", VARCHAR)
            .column("table_schem", VARCHAR)
            .column("table_name", VARCHAR)
            .column("table_type", VARCHAR)
            .column("remarks", VARCHAR)
            .column("type_cat", VARCHAR)
            .column("type_schem", VARCHAR)
            .column("type_name", VARCHAR)
            .column("self_referencing_col_name", VARCHAR)
            .column("ref_generation", VARCHAR)
            .build();

    private final Metadata metadata;

    @Inject
    public TableJdbcTable(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        GlobalSystemTransactionHandle transaction = checkType(transactionHandle, GlobalSystemTransactionHandle.class, "transaction");
        Session session = toSession(transaction.getTransactionId(), connectorSession);
        Optional<String> catalogFilter = stringFilter(constraint, 0);
        Optional<String> schemaFilter = stringFilter(constraint, 1);
        Optional<String> tableFilter = stringFilter(constraint, 2);
        Optional<String> typeFilter = stringFilter(constraint, 3);

        Builder table = InMemoryRecordSet.builder(METADATA);
        for (String catalog : filter(metadata.getCatalogNames().keySet(), catalogFilter)) {
            QualifiedTablePrefix prefix = tablePrefix(catalog, schemaFilter, tableFilter);

            if (FilterUtil.emptyOrEquals(typeFilter, "TABLE")) {
                for (QualifiedObjectName name : metadata.listTables(session, prefix)) {
                    table.addRow(tableRow(name, "TABLE"));
                }
            }

            if (FilterUtil.emptyOrEquals(typeFilter, "VIEW")) {
                for (QualifiedObjectName name : metadata.listViews(session, prefix)) {
                    table.addRow(tableRow(name, "VIEW"));
                }
            }
        }
        return table.build().cursor();
    }

    private static Object[] tableRow(QualifiedObjectName name, String type)
    {
        return new Object[] {name.getCatalogName(), name.getSchemaName(), name.getObjectName(), type,
                             null, null, null, null, null, null};
    }
}
