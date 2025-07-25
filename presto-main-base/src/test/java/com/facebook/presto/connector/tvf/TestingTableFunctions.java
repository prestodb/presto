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
import com.facebook.presto.spi.function.SchemaFunctionName;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.DescriptorArgumentSpecification;
import com.facebook.presto.spi.function.table.ReturnTypeSpecification;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class TestingTableFunctions
{
    private static final String SCHEMA_NAME = "system";
    private static final String TABLE_NAME = "table";
    private static final String COLUMN_NAME = "column";
    private static final ConnectorTableFunctionHandle HANDLE = new TestingTableFunctionPushdownHandle();
    private static final TableFunctionAnalysis ANALYSIS = TableFunctionAnalysis.builder()
            .handle(HANDLE)
            .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
            .build();

    public static class TestConnectorTableFunction
            extends AbstractConnectorTableFunction
    {
        private static final String TEST_FUNCTION = "test_function";

        public TestConnectorTableFunction()
        {
            super(SCHEMA_NAME, TEST_FUNCTION, ImmutableList.of(), ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, TEST_FUNCTION)))
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field("c1", Optional.of(BOOLEAN)))))
                    .build();
        }
    }

    public static class TestConnectorTableFunction2
            extends AbstractConnectorTableFunction
    {
        private static final String TEST_FUNCTION_2 = "test_function2";

        public TestConnectorTableFunction2()
        {
            super(SCHEMA_NAME, TEST_FUNCTION_2, ImmutableList.of(), ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }

    public static class NullArgumentsTableFunction
            extends AbstractConnectorTableFunction
    {
        private static final String NULL_ARGUMENTS_FUNCTION = "null_arguments_function";

        public NullArgumentsTableFunction()
        {
            super(SCHEMA_NAME, NULL_ARGUMENTS_FUNCTION, null, ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }

    public static class DuplicateArgumentsTableFunction
            extends AbstractConnectorTableFunction
    {
        private static final String DUPLICATE_ARGUMENTS_FUNCTION = "duplicate_arguments_function";
        public DuplicateArgumentsTableFunction()
        {
            super(
                    SCHEMA_NAME,
                    DUPLICATE_ARGUMENTS_FUNCTION,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder().name("a").type(INTEGER).build(),
                            ScalarArgumentSpecification.builder().name("a").type(INTEGER).build()),
                    ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }

    public static class MultipleRSTableFunction
            extends AbstractConnectorTableFunction
    {
        private static final String MULTIPLE_SOURCES_FUNCTION = "multiple_sources_function";
        public MultipleRSTableFunction()
        {
            super(
                    SCHEMA_NAME,
                    MULTIPLE_SOURCES_FUNCTION,
                    ImmutableList.of(TableArgumentSpecification.builder().name("t").rowSemantics().build(),
                            TableArgumentSpecification.builder().name("t2").rowSemantics().build()),
                    ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }

    /**
     * A table function returning a table with single empty column of type BOOLEAN.
     * The argument `COLUMN` is the column name.
     * The argument `IGNORED` is ignored.
     * Both arguments are optional.
     */
    public static class SimpleTableFunction
            extends AbstractConnectorTableFunction
    {
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
            private final TestTVFConnectorTableHandle tableHandle;

            public SimpleTableFunctionHandle(String schema, String table, String column)
            {
                this.tableHandle = new TestTVFConnectorTableHandle(
                        new SchemaTableName(schema, table),
                        Optional.of(ImmutableList.of(new TestTVFConnectorColumnHandle(column, BOOLEAN))),
                        TupleDomain.all());
            }

            public TestTVFConnectorTableHandle getTableHandle()
            {
                return tableHandle;
            }
        }
    }

    public static class TwoScalarArgumentsFunction
            extends AbstractConnectorTableFunction
    {
        public TwoScalarArgumentsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "two_arguments_function",
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("TEXT")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("NUMBER")
                                    .type(BIGINT)
                                    .defaultValue(null)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return ANALYSIS;
        }
    }

    public static class TableArgumentFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "table_argument_function";

        public TableArgumentFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT", ImmutableList.of(0))
                    .build();
        }
    }

    public static class DescriptorArgumentFunction
            extends AbstractConnectorTableFunction
    {
        public DescriptorArgumentFunction()
        {
            super(
                    SCHEMA_NAME,
                    "descriptor_argument_function",
                    ImmutableList.of(
                            DescriptorArgumentSpecification.builder()
                                    .name("SCHEMA")
                                    .defaultValue(null)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return ANALYSIS;
        }
    }

    public static class TestingTableFunctionPushdownHandle
            implements ConnectorTableFunctionHandle
    {
        private final TestTVFConnectorTableHandle tableHandle;

        public TestingTableFunctionPushdownHandle()
        {
            this.tableHandle = new TestTVFConnectorTableHandle(
                    new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                    Optional.of(ImmutableList.of(new TestTVFConnectorColumnHandle(COLUMN_NAME, BOOLEAN))),
                    TupleDomain.all());
        }

        public TestTVFConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public static class TestingTableFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final SchemaFunctionName schemaFunctionName;

        @JsonCreator
        public TestingTableFunctionHandle(@JsonProperty("schemaFunctionName") SchemaFunctionName schemaFunctionName)
        {
            this.schemaFunctionName = requireNonNull(schemaFunctionName, "schemaFunctionName is null");
        }

        @JsonProperty
        public SchemaFunctionName getSchemaFunctionName()
        {
            return schemaFunctionName;
        }
    }
}
