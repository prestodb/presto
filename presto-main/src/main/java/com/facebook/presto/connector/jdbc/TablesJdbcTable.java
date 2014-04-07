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
package com.facebook.presto.connector.jdbc;

import static com.facebook.presto.metadata.MetadataUtil.columnTypeGetter;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.google.common.collect.ImmutableList;

public class TablesJdbcTable
        implements SystemTable
{
    public static final SchemaTableName JDBC_TABLES_TABLE_NAME = new SchemaTableName("jdbc", "tables");

    public static final ConnectorTableMetadata JDBC_TABLES_TABLE = tableMetadataBuilder(JDBC_TABLES_TABLE_NAME)
            .column("table_cat", STRING)
            .column("table_schem", STRING)
            .column("table_name", STRING)
            .column("table_type", STRING)
            .column("remarks", STRING)
            .column("type_cat", STRING)
            .column("type_schem", STRING)
            .column("type_name", STRING)
            .column("self_referencing_col_name", STRING)
            .column("ref_generation", STRING)
            .build();

    private final Metadata metadata;

    @Inject
    public TablesJdbcTable(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public boolean isDistributed()
    {
        return false;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return JDBC_TABLES_TABLE;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return ImmutableList.copyOf(transform(JDBC_TABLES_TABLE.getColumns(), columnTypeGetter()));
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(JDBC_TABLES_TABLE);
        for (Map.Entry<String, String> entry : metadata.getCatalogNames().entrySet()) {
            QualifiedTablePrefix prefix = new QualifiedTablePrefix(entry.getKey());
            for (QualifiedTableName name : metadata.listTables(prefix)) {
                table.addRow(
                    name.getCatalogName(), // TABLE_CAT String => table catalog (may be null)
                    name.getSchemaName(), // TABLE_SCHEM String => table schema (may be null)
                    name.getTableName(), // TABLE_NAME String => table name
                    "TABLE", // TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
                    "", // REMARKS String => explanatory comment on the table
                    null, // TYPE_CAT String => the types catalog (may be null)
                    null, // TYPE_SCHEM String => the types schema (may be null)
                    null, // TYPE_NAME String => type name (may be null)
                    null, // SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
                    null // REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
                    );
            }
        }
        return table.build().cursor();
    }
}
