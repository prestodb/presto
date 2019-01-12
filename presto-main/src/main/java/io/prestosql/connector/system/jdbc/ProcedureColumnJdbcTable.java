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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;

public class ProcedureColumnJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "procedure_columns");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("procedure_cat", createUnboundedVarcharType())
            .column("procedure_schem", createUnboundedVarcharType())
            .column("procedure_name", createUnboundedVarcharType())
            .column("column_name", createUnboundedVarcharType())
            .column("column_type", BIGINT)
            .column("data_type", BIGINT)
            .column("type_name", createUnboundedVarcharType())
            .column("precision", BIGINT)
            .column("length", BIGINT)
            .column("scale", BIGINT)
            .column("radix", BIGINT)
            .column("nullable", BIGINT)
            .column("remarks", createUnboundedVarcharType())
            .column("column_def", createUnboundedVarcharType())
            .column("sql_data_type", BIGINT)
            .column("sql_datetime_sub", BIGINT)
            .column("char_octet_length", BIGINT)
            .column("ordinal_position", BIGINT)
            .column("is_nullable", createUnboundedVarcharType())
            .column("specific_name", createUnboundedVarcharType())
            .build();

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return InMemoryRecordSet.builder(METADATA).build().cursor();
    }
}
