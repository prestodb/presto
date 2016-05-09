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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;

import javax.inject.Inject;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.connector.system.jdbc.FilterUtil.filter;
import static com.facebook.presto.connector.system.jdbc.FilterUtil.stringFilter;
import static com.facebook.presto.connector.system.jdbc.FilterUtil.toSession;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Types.checkType;
import static java.util.Objects.requireNonNull;

public class ColumnJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "columns");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_cat", VARCHAR)
            .column("table_schem", VARCHAR)
            .column("table_name", VARCHAR)
            .column("column_name", VARCHAR)
            .column("data_type", BIGINT)
            .column("type_name", VARCHAR)
            .column("column_size", BIGINT)
            .column("buffer_length", BIGINT)
            .column("decimal_digits", BIGINT)
            .column("num_prec_radix", BIGINT)
            .column("nullable", BIGINT)
            .column("remarks", VARCHAR)
            .column("column_def", VARCHAR)
            .column("sql_data_type", BIGINT)
            .column("sql_datetime_sub", BIGINT)
            .column("char_octet_length", BIGINT)
            .column("ordinal_position", BIGINT)
            .column("is_nullable", VARCHAR)
            .column("scope_catalog", VARCHAR)
            .column("scope_schema", VARCHAR)
            .column("scope_table", VARCHAR)
            .column("source_data_type", BIGINT)
            .column("is_autoincrement", VARCHAR)
            .column("is_generatedcolumn", VARCHAR)
            .build();

    private final Metadata metadata;

    @Inject
    public ColumnJdbcTable(Metadata metadata)
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

        Builder table = InMemoryRecordSet.builder(METADATA);
        for (String catalog : filter(metadata.getCatalogNames().keySet(), catalogFilter)) {
            QualifiedTablePrefix prefix = FilterUtil.tablePrefix(catalog, schemaFilter, tableFilter);
            for (Entry<QualifiedObjectName, List<ColumnMetadata>> entry : metadata.listTableColumns(session, prefix).entrySet()) {
                addColumnRows(table, entry.getKey(), entry.getValue());
            }
        }
        return table.build().cursor();
    }

    private static void addColumnRows(Builder builder, QualifiedObjectName tableName, List<ColumnMetadata> columns)
    {
        int ordinalPosition = 1;
        for (ColumnMetadata column : columns) {
            if (column.isHidden()) {
                continue;
            }
            builder.addRow(
                    tableName.getCatalogName(),
                    tableName.getSchemaName(),
                    tableName.getObjectName(),
                    column.getName(),
                    jdbcDataType(column.getType()),
                    column.getType().getDisplayName(),
                    0,
                    0,
                    decimalDigits(column.getType()),
                    10,
                    DatabaseMetaData.columnNullableUnknown,
                    column.getComment(),
                    null,
                    null,
                    null,
                    0,
                    ordinalPosition,
                    "",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
            ordinalPosition++;
        }
    }

    private static int jdbcDataType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Types.BOOLEAN;
        }
        if (type.equals(BIGINT)) {
            return Types.BIGINT;
        }
        if (type.equals(INTEGER)) {
            return Types.INTEGER;
        }
        if (type.equals(DOUBLE)) {
            return Types.DOUBLE;
        }
        if (type.equals(VARCHAR)) {
            return Types.LONGNVARCHAR;
        }
        if (type.equals(VARBINARY)) {
            return Types.LONGVARBINARY;
        }
        if (type.equals(TIME)) {
            return Types.TIME;
        }
        if (type.equals(TIME_WITH_TIME_ZONE)) {
            return Types.TIME_WITH_TIMEZONE;
        }
        if (type.equals(TIMESTAMP)) {
            return Types.TIMESTAMP;
        }
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return Types.TIMESTAMP_WITH_TIMEZONE;
        }
        if (type.equals(DATE)) {
            return Types.DATE;
        }
        if (type instanceof ArrayType) {
            return Types.ARRAY;
        }
        return Types.JAVA_OBJECT;
    }

    private static Integer decimalDigits(Type type)
    {
        if (type.equals(BIGINT)) {
            return 19;
        }
        return null;
    }
}
