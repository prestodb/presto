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
package com.facebook.presto.iceberg.tvf;

import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.iceberg.ColumnIdentity;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorTableFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;

@Description("Approximate Nearest Neighbors for input vector against a vector column")
public class ApproxNearestNeighborsFunction
        implements Provider<ConnectorTableFunction>
{
    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new QueryFunction(), getClass().getClassLoader());
    }

    public static final String NAME = "approx_nearest_neighbors";
    public static final String SCHEMA_NAME = "system";

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private static final String QUERY_VECTOR = "query_vector";
        private static final String COLUMN_NAME = "column_name";
        private static final String LIMIT = "limit";

        public QueryFunction()
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            ScalarArgumentSpecification.builder()
                                    .name(QUERY_VECTOR)
                                    .type(new ArrayType(REAL))
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(COLUMN_NAME)
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(LIMIT)
                                    .type(BIGINT)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments)
        {
            Descriptor returnedType = new Descriptor(ImmutableList.of(
                    new Descriptor.Field("row_id", Optional.of(BIGINT))));

            ScalarArgument queryVector = (ScalarArgument) arguments.get(QUERY_VECTOR);
            ScalarArgument limit = (ScalarArgument) arguments.get(LIMIT);
            ScalarArgument qualifiedColumnName = (ScalarArgument) arguments.get(COLUMN_NAME);

            String formattedColumnName = stripSurroundingSingleQuotes(((Slice) qualifiedColumnName.getValue()).toStringUtf8());
            String columnName = parseQualifiedName(formattedColumnName).getColumn();
            String schemaName = parseQualifiedName(formattedColumnName).getSchema();
            String tableName = parseQualifiedName(formattedColumnName).getTable();

            // --- build column handles ---
            List<ColumnHandle> columnHandles = ImmutableList.of(
                    new IcebergColumnHandle(
                            new ColumnIdentity(0, "row_id", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                            BIGINT,
                            Optional.empty(),
                            REGULAR,
                            ImmutableList.of()));

            IcebergAnnTableFunctionHandle handle =
                    new IcebergAnnTableFunctionHandle(schemaName, tableName, queryVector, limit, columnHandles);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }

        private static String stripSurroundingSingleQuotes(String input)
        {
            if (input != null && input.length() >= 2 &&
                    input.startsWith("'") && input.endsWith("'")) {
                return input.substring(1, input.length() - 1);
            }
            return input;
        }
        private static List<ColumnHandle> extractColumnParameters(String columnName)
        {
            List<ColumnHandle> columnHandles = new ArrayList<>();

            Type prestoType = createUnboundedVarcharType();
            columnHandles.add(new IcebergColumnHandle(new ColumnIdentity(ThreadLocalRandom.current().nextInt(), columnName, ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                    prestoType,
                    Optional.empty(),
                    REGULAR,
                    ImmutableList.of()));
            return columnHandles;
        }
    }
    public static class QualifiedNameParts
    {
        public final String schema;

        public String getSchema()
        {
            return schema;
        }

        public String getTable()
        {
            return table;
        }

        public String getColumn()
        {
            return column;
        }

        public final String table;
        public final String column;

        public QualifiedNameParts(String schema, String table, String column)
        {
            this.schema = schema;
            this.table = table;
            this.column = column;
        }
    }

    public static QualifiedNameParts parseQualifiedName(String qualifiedName)
    {
        String[] parts = qualifiedName.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Expected format: schema.table.column, but got: " + qualifiedName);
        }
        return new QualifiedNameParts(parts[0], parts[1], parts[2]);
    }

    public static class IcebergAnnTableHandle
            extends IcebergTableHandle
    {
        private final List<Float> queryVector;
        private final int limit;
        public IcebergAnnTableHandle(List<Float> queryVector, int limit, String schema, String table)
        {
            super(schema, IcebergTableName.from(table), false, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableList.of());
            this.queryVector = queryVector;
            this.limit = limit;
        }
        public List<Float> getInputVector()
        {
            return queryVector;
        }

        public int getLimit()
        {
            return limit;
        }
    }
    public static class IcebergAnnTableFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final List<Float> queryVector;
        private final int limit;
        private final ConnectorTableHandle tableHandle;
        private final List<ColumnHandle> columnHandles;

        public IcebergAnnTableFunctionHandle(String schema, String table, ScalarArgument inputVector, ScalarArgument limit, List<ColumnHandle> columnHandles)
        {
            this.queryVector = convertBlockToFloatList((IntArrayBlock) inputVector.getValue());
            this.limit = ((Long) limit.getValue()).intValue();
            this.tableHandle = new IcebergAnnTableHandle(queryVector, this.limit, schema, table);
            this.columnHandles = columnHandles;
        }

        public List<Float> getInputVector()
        {
            return queryVector;
        }

        public int getLimit()
        {
            return limit;
        }

        public ConnectorTableHandle getTableHandle()
        {
            return this.tableHandle;
        }
        public List<ColumnHandle> getColumnHandles()
        {
            return this.columnHandles;
        }
        private static List<Float> parseQueryVector(String valueExpression)
        {
            if (valueExpression == null || valueExpression.isEmpty()) {
                return List.of();
            }

            // Remove leading "ARRAY[" and trailing "]"
            String inner = valueExpression.trim();
            if (inner.startsWith("ARRAY[")) {
                inner = inner.substring("ARRAY[".length(), inner.length() - 1);
            }

            // Split by comma
            String[] parts = inner.split(",");

            List<Float> result = new ArrayList<>();
            for (String part : parts) {
                part = part.trim();

                // Strip DECIMAL '...'
                if (part.startsWith("DECIMAL")) {
                    int start = part.indexOf('\'');
                    int end = part.lastIndexOf('\'');
                    if (start > 0 && end > start) {
                        part = part.substring(start + 1, end);
                    }
                }
                // Convert to float
                result.add(Float.parseFloat(part));
            }
            return result;
        }
        private static List<Float> convertBlockToFloatList(IntArrayBlock intBlock)
        {
            int positionCount = intBlock.getPositionCount();
            List<Float> result = new ArrayList<>(positionCount);

            for (int position = 0; position < positionCount; position++) {
                if (intBlock.isNull(position)) {
                    result.add(null);
                }
                else {
                    // REAL type stores floats as int bits - must convert back
                    result.add(Float.intBitsToFloat(intBlock.getInt(position)));
                }
            }

            return result;
        }
    }
}
