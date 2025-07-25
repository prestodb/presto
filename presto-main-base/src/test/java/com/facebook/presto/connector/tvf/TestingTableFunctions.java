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
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.SchemaFunctionName;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.DescriptorArgumentSpecification;
import com.facebook.presto.spi.function.table.ReturnTypeSpecification;
import com.facebook.presto.spi.function.table.ReturnTypeSpecification.DescribedTable;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableArgument;
import com.facebook.presto.spi.function.table.TableArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.connector.tvf.TestingTableFunctions.ConstantFunction.ConstantFunctionSplit.DEFAULT_SPLIT_SIZE;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;
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
    private static final TableFunctionAnalysis NO_DESCRIPTOR_ANALYSIS = TableFunctionAnalysis.builder()
            .handle(HANDLE)
            .requiredColumns("INPUT", ImmutableList.of(0))
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

    public static class TableArgumentRowSemanticsFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "table_argument_row_semantics_function";

        public TableArgumentRowSemanticsFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .rowSemantics()
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

    public static class TwoTableArgumentsFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "two_table_arguments_function";

        public TwoTableArgumentsFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT1")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT2")
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
                    .requiredColumns("INPUT1", ImmutableList.of(0))
                    .requiredColumns("INPUT2", ImmutableList.of(0))
                    .build();
        }
    }

    public static class OnlyPassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public OnlyPassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "only_pass_through_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class MonomorphicStaticReturnTypeFunction
            extends AbstractConnectorTableFunction
    {
        public MonomorphicStaticReturnTypeFunction()
        {
            super(
                    SCHEMA_NAME,
                    "monomorphic_static_return_type_function",
                    ImmutableList.of(),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(BOOLEAN, INTEGER))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .build();
        }
    }

    public static class PolymorphicStaticReturnTypeFunction
            extends AbstractConnectorTableFunction
    {
        public PolymorphicStaticReturnTypeFunction()
        {
            super(
                    SCHEMA_NAME,
                    "polymorphic_static_return_type_function",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(BOOLEAN, INTEGER))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class PassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public PassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "pass_through_function",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .passThroughColumns()
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("x"),
                            ImmutableList.of(BOOLEAN))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class DifferentArgumentTypesFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "different_arguments_function";

        public DifferentArgumentTypesFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT_1")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            DescriptorArgumentSpecification.builder()
                                    .name("LAYOUT")
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .rowSemantics()
                                    .passThroughColumns()
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("ID")
                                    .type(BIGINT)
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_3")
                                    .pruneWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT_1", ImmutableList.of(0))
                    .requiredColumns("INPUT_2", ImmutableList.of(0))
                    .requiredColumns("INPUT_3", ImmutableList.of(0))
                    .build();
        }
    }

    public static class RequiredColumnsFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "required_columns_function";

        public RequiredColumnsFunction()
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
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field("column", Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT", ImmutableList.of(0, 1))
                    .build();
        }
    }

    public static class TestingTableFunctionPushdownHandle
            implements ConnectorTableFunctionHandle
    {
        private final MockConnectorTableHandle tableHandle;

        public TestingTableFunctionPushdownHandle()
        {
            this.tableHandle = new MockConnectorTableHandle(
                    new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                    TupleDomain.all(),
                    Optional.of(ImmutableList.of(new MockConnectorColumnHandle(COLUMN_NAME, BOOLEAN))));
        }

        public MockConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }

    // for testing execution by operator

    public static class IdentityFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "identity_function";

        public IdentityFunction()
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
            List<RowType.Field> inputColumns = ((TableArgument) arguments.get("INPUT")).getRowType().getFields();
            Descriptor returnedType = new Descriptor(inputColumns.stream()
                    .map(field -> new Descriptor.Field(field.getName().orElse("anonymous_column"), Optional.of(field.getType())))
                    .collect(toImmutableList()));
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .returnedType(returnedType)
                    .requiredColumns("INPUT", IntStream.range(0, inputColumns.size()).boxed().collect(toImmutableList()))
                    .build();
        }
    }

    public static class IdentityPassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "identity_pass_through_function";

        public IdentityPassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .requiredColumns("INPUT", ImmutableList.of(0)) // per spec, function must require at least one column
                    .build();
        }
    }

    public static class RepeatFunction
            extends AbstractConnectorTableFunction
    {
        public RepeatFunction()
        {
            super(
                    SCHEMA_NAME,
                    "repeat",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("N")
                                    .type(INTEGER)
                                    .defaultValue(2L)
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument count = (ScalarArgument) arguments.get("N");
            requireNonNull(count.getValue(), "count value for function repeat() is null");
            checkArgument((long) count.getValue() > 0, "count value for function repeat() must be positive");

            return TableFunctionAnalysis.builder()
                    .handle(new RepeatFunctionHandle((long) count.getValue()))
                    .requiredColumns("INPUT", ImmutableList.of(0)) // per spec, function must require at least one column
                    .build();
        }

        public static class RepeatFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final long count;

            @JsonCreator
            public RepeatFunctionHandle(@JsonProperty("count") long count)
            {
                this.count = count;
            }

            @JsonProperty
            public long getCount()
            {
                return count;
            }
        }
    }

    public static class EmptyOutputFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "empty_output";

        public EmptyOutputFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("column", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }
    }

    public static class EmptyOutputWithPassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "empty_output_with_pass_through";

        public EmptyOutputWithPassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .passThroughColumns()
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("column", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }
    }

    public static class TestInputsFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "test_inputs_function";

        public TestInputsFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .rowSemantics()
                                    .name("INPUT_1")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_3")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_4")
                                    .keepWhenEmpty()
                                    .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("boolean_result", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .requiredColumns("INPUT_1", IntStream.range(0, ((TableArgument) arguments.get("INPUT_1")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_2", IntStream.range(0, ((TableArgument) arguments.get("INPUT_2")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_3", IntStream.range(0, ((TableArgument) arguments.get("INPUT_3")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_4", IntStream.range(0, ((TableArgument) arguments.get("INPUT_4")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }
    }

    public static class PassThroughInputFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "pass_through";

        public PassThroughInputFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT_1")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(
                            new Descriptor.Field("input_1_present", Optional.of(BOOLEAN)),
                            new Descriptor.Field("input_2_present", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .requiredColumns("INPUT_1", ImmutableList.of(0))
                    .requiredColumns("INPUT_2", ImmutableList.of(0))
                    .build();
        }
    }

    public static class TestInputFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "test_input";

        public TestInputFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("got_input", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }
    }

    public static class TestSingleInputRowSemanticsFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "test_single_input_function";

        public TestSingleInputRowSemanticsFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .rowSemantics()
                            .name("INPUT")
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("boolean_result", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }
    }

    public static class ConstantFunction
            extends AbstractConnectorTableFunction
    {
        public ConstantFunction()
        {
            super(
                    SCHEMA_NAME,
                    "constant",
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("VALUE")
                                    .type(INTEGER)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("N")
                                    .type(INTEGER)
                                    .defaultValue(1L)
                                    .build()),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("constant_column"),
                            ImmutableList.of(INTEGER))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument count = (ScalarArgument) arguments.get("N");
            requireNonNull(count.getValue(), "count value for function repeat() is null");
            checkArgument((long) count.getValue() > 0, "count value for function repeat() must be positive");

            return TableFunctionAnalysis.builder()
                    .handle(new ConstantFunctionHandle((Long) ((ScalarArgument) arguments.get("VALUE")).getValue(), (long) count.getValue()))
                    .build();
        }

        public static class ConstantFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final Long value;
            private final long count;

            @JsonCreator
            public ConstantFunctionHandle(@JsonProperty("value") Long value, @JsonProperty("count") long count)
            {
                this.value = value;
                this.count = count;
            }

            @JsonProperty
            public Long getValue()
            {
                return value;
            }

            @JsonProperty
            public long getCount()
            {
                return count;
            }
        }

        public static ConnectorSplitSource getConstantFunctionSplitSource(ConstantFunctionHandle handle)
        {
            long splitSize = DEFAULT_SPLIT_SIZE;
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (long i = 0; i < handle.getCount() / splitSize; i++) {
                splits.add(new ConstantFunctionSplit(splitSize));
            }
            long remainingSize = handle.getCount() % splitSize;
            if (remainingSize > 0) {
                splits.add(new ConstantFunctionSplit(remainingSize));
            }
            return new FixedSplitSource(splits.build());
        }

        public static final class ConstantFunctionSplit
                implements ConnectorSplit
        {
            private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(ConstantFunctionSplit.class).instanceSize());
            public static final int DEFAULT_SPLIT_SIZE = 5500;

            private final long count;

            @JsonCreator
            public ConstantFunctionSplit(@JsonProperty("count") long count)
            {
                this.count = count;
            }

            @JsonProperty
            public long getCount()
            {
                return count;
            }

            @Override
            public NodeSelectionStrategy getNodeSelectionStrategy()
            {
                return NO_PREFERENCE;
            }

            @Override
            public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
            {
                return Collections.emptyList();
            }

            @Override
            public Object getInfo()
            {
                return count;
            }
        }
    }

    public static class EmptySourceFunction
            extends AbstractConnectorTableFunction
    {
        public static final String FUNCTION_NAME = "empty_source";

        public EmptySourceFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    ImmutableList.of(),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("column", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new TestingTableFunctionHandle(new SchemaFunctionName(SCHEMA_NAME, FUNCTION_NAME)))
                    .build();
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
