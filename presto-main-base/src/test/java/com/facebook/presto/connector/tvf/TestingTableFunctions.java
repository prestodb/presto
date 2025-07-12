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
package com.facebook.presto.connector.tvf;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.airlift.slice.Slices.utf8Slice;

public class TestingTableFunctions
{
    /**
     * A table function returning a table with single empty column of type BOOLEAN.
     * The argument `COLUMN` is the column name.
     * The argument `IGNORED` is ignored.
     * Both arguments are optional.
     */
    public static class SimpleTableFunction
            extends AbstractConnectorTableFunction
    {
        private static final String SCHEMA_NAME = "system";
        private static final String FUNCTION_NAME = "simple_table_function";
        private static final String TABLE_NAME = "simple_table";

        public SimpleTableFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    Arrays.asList(
                            ScalarArgumentSpecification.builder()
                                    .name("COLUMN")
                                    .type(VARCHAR)
                                    .defaultValue(utf8Slice("col"))
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("IGNORED")
                                    .type(BIGINT)
                                    .defaultValue(0L)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument argument = (ScalarArgument) arguments.get("COLUMN");
            String columnName = ((Slice) argument.getValue()).toStringUtf8();

            return TableFunctionAnalysis.builder()
                    .handle(new SimpleTableFunctionHandle(getSchema(), TABLE_NAME, columnName))
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(columnName, Optional.of(BOOLEAN)))))
                    .build();
        }

        public static class SimpleTableFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final MockConnectorTableHandle tableHandle;

            public SimpleTableFunctionHandle(String schema, String table, String column)
            {
                this.tableHandle = new MockConnectorTableHandle(
                        new SchemaTableName(schema, table),
                        TupleDomain.all(),
                        Optional.of(ImmutableList.of(new MockConnectorColumnHandle(column, BOOLEAN))));
            }

            public MockConnectorTableHandle getTableHandle()
            {
                return tableHandle;
            }
        }
    }
}
