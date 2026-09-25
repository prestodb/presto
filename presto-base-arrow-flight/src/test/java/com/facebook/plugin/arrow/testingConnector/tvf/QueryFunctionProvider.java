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
package com.facebook.plugin.arrow.testingConnector.tvf;

import com.facebook.plugin.arrow.ArrowColumnHandle;
import com.facebook.plugin.arrow.testingConnector.PrimitiveToPrestoTypeMappings;
import com.facebook.plugin.arrow.testingConnector.TestingQueryArrowTableHandle;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.table.GenericTableReturnTypeSpecification.GENERIC_TABLE;
import static java.util.Objects.requireNonNull;

public class QueryFunctionProvider
        implements Provider<ConnectorTableFunction>
{
    private static final String SYSTEM = "system";
    private static final String QUERY_FUNCTION = "query_function";
    private static final String QUERY = "QUERY";
    private static final String DATATYPES = "DATATYPES";

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction();
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        public QueryFunction()
        {
            super(
                    SYSTEM,
                    QUERY_FUNCTION,
                    Arrays.asList(
                            ScalarArgumentSpecification.builder()
                                    .name(QUERY)
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(DATATYPES)
                                    .type(VARCHAR)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments)
        {
            Slice dataTypes = (Slice) ((ScalarArgument) arguments.get(DATATYPES)).getValue();
            List<ArrowColumnHandle> columnHandles = ImmutableList.copyOf(extractColumnParameters(dataTypes.toStringUtf8()));

            // preparing descriptor from column handles
            Descriptor returnedType = new Descriptor(columnHandles.stream()
                    .map(column -> new Descriptor.Field(column.getColumnName(), Optional.of(column.getColumnType())))
                    .collect(Collectors.toList()));

            Slice query = (Slice) ((ScalarArgument) arguments.get(QUERY)).getValue();

            TestingQueryArrowTableHandle queryArrowTableHandle = new TestingQueryArrowTableHandle(query.toStringUtf8(), columnHandles);
            QueryFunctionHandle handle = new QueryFunctionHandle(queryArrowTableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class QueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final TestingQueryArrowTableHandle tableHandle;

        @JsonCreator
        public QueryFunctionHandle(@JsonProperty("tableHandle") TestingQueryArrowTableHandle tableHandle)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public TestingQueryArrowTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }

    private static List<ArrowColumnHandle> extractColumnParameters(String input)
    {
        String regex = "\\s*([\\w]+)\\s+([\\w ]+)(?:\\((\\d+)(?:,(\\d+))?\\))?\\s*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        List<ArrowColumnHandle> columnHandles = new ArrayList<>();

        while (matcher.find()) {
            //map columnType to presto type
            requireNonNull(matcher.group(2), "Column data type is null");
            Type prestoType = PrimitiveToPrestoTypeMappings.fromPrimitiveToPrestoType(matcher.group(2).toUpperCase());
            if (prestoType == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Unsupported data type: " + matcher.group(2));
            }
            columnHandles.add(new ArrowColumnHandle(matcher.group(1), prestoType));
        }

        return columnHandles;
    }
}
