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
package com.facebook.presto.lance;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.lance.LanceVectorSearch.DISTANCE_COLUMN_NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.table.GenericTableReturnTypeSpecification.GENERIC_TABLE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * {@code TABLE(lance.system.vector_search(schema_name => ..., table_name => ..., vector_col => ...,
 * query_vector => ARRAY[...], k => 10, metric => 'l2'))}
 *
 * <p>Resolves the target table at analysis time (Doris-style): validates every argument against
 * the pinned dataset schema and emits a {@link LanceTableHandle} carrying the complete
 * {@link LanceVectorSearch} request with {@code requiresGlobalTopK=true}. The engine turns the
 * invocation into a plain table scan ({@code TransformTableFunctionProcessorToTableScan});
 * {@link LanceVectorSearchTopNRewriter} then adds the global-merge TopN.
 *
 * <p>Output = all table columns plus a trailing {@code _distance REAL} column (Lance-native:
 * smaller is nearer for every metric; cosine emits 1 - similarity, dot emits the negated product).
 * A WHERE clause above the invocation is pushed into the scan layout and applied by Lance as a
 * post-ANN residual filter; a pre-filter argument is deliberately not part of v1.
 */
public class LanceVectorSearchTableFunction
        extends AbstractConnectorTableFunction
{
    public static final String SCHEMA_NAME = "system";
    public static final String FUNCTION_NAME = "vector_search";

    private static final String SCHEMA_ARG = "SCHEMA_NAME";
    private static final String TABLE_ARG = "TABLE_NAME";
    private static final String VECTOR_COL_ARG = "VECTOR_COL";
    private static final String QUERY_VECTOR_ARG = "QUERY_VECTOR";
    private static final String K_ARG = "K";
    private static final String METRIC_ARG = "METRIC";
    private static final String NPROBES_ARG = "NPROBES";
    private static final String EF_ARG = "EF";
    private static final String REFINE_FACTOR_ARG = "REFINE_FACTOR";

    private static final Set<String> SUPPORTED_METRICS = ImmutableSet.of(
            LanceVectorSearch.METRIC_L2, LanceVectorSearch.METRIC_COSINE, LanceVectorSearch.METRIC_DOT);

    private final LanceNamespaceHolder namespaceHolder;

    @Inject
    public LanceVectorSearchTableFunction(LanceNamespaceHolder namespaceHolder)
    {
        super(SCHEMA_NAME, FUNCTION_NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(SCHEMA_ARG).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(TABLE_ARG).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(VECTOR_COL_ARG).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(QUERY_VECTOR_ARG).type(new ArrayType(DOUBLE)).build(),
                        ScalarArgumentSpecification.builder().name(K_ARG).type(BIGINT).defaultValue(10L).build(),
                        ScalarArgumentSpecification.builder().name(METRIC_ARG).type(VARCHAR).defaultValue(utf8Slice(LanceVectorSearch.METRIC_L2)).build(),
                        ScalarArgumentSpecification.builder().name(NPROBES_ARG).type(BIGINT).defaultValue(null).build(),
                        ScalarArgumentSpecification.builder().name(EF_ARG).type(BIGINT).defaultValue(null).build(),
                        ScalarArgumentSpecification.builder().name(REFINE_FACTOR_ARG).type(BIGINT).defaultValue(null).build()),
                GENERIC_TABLE);
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        String schemaName = stringArgument(arguments, SCHEMA_ARG);
        String tableName = stringArgument(arguments, TABLE_ARG);
        String vectorColumn = stringArgument(arguments, VECTOR_COL_ARG);
        List<Double> queryVector = queryVectorArgument(arguments);
        int k = intArgument(arguments, K_ARG, 1);
        String metric = stringArgument(arguments, METRIC_ARG).toLowerCase(ENGLISH);
        Optional<Integer> nprobes = optionalIntArgument(arguments, NPROBES_ARG);
        Optional<Integer> ef = optionalIntArgument(arguments, EF_ARG);
        Optional<Integer> refineFactor = optionalIntArgument(arguments, REFINE_FACTOR_ARG);

        if (!SUPPORTED_METRICS.contains(metric)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    format("metric must be one of %s, got '%s'", SUPPORTED_METRICS, metric));
        }

        if (!namespaceHolder.schemaExists(schemaName)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Schema '%s' does not exist", schemaName));
        }
        String tablePath = namespaceHolder.getTablePath(schemaName, tableName);
        if (tablePath == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Table '%s.%s' does not exist", schemaName, tableName));
        }
        List<String> tableId = namespaceHolder.getTableId(schemaName, tableName);
        long datasetVersion = namespaceHolder.getLatestVersion(tablePath);
        Schema arrowSchema = namespaceHolder.describeTable(tablePath, Optional.of(datasetVersion));

        ImmutableList.Builder<LanceColumnHandle> columnsBuilder = ImmutableList.builder();
        Field vectorField = null;
        for (Field field : arrowSchema.getFields()) {
            if (DISTANCE_COLUMN_NAME.equals(field.getName())) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                        format("Table '%s.%s' already has a '%s' column; vector_search cannot add its distance output",
                                schemaName, tableName, DISTANCE_COLUMN_NAME));
            }
            if (field.getName().equals(vectorColumn)) {
                vectorField = field;
            }
            columnsBuilder.add(new LanceColumnHandle(field.getName(), LanceColumnHandle.toPrestoType(field), field.isNullable()));
        }
        if (vectorField == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    format("Column '%s' does not exist in table '%s.%s'", vectorColumn, schemaName, tableName));
        }
        validateVectorColumn(vectorField, queryVector.size());
        columnsBuilder.add(LanceColumnHandle.distanceColumn());
        List<LanceColumnHandle> columns = columnsBuilder.build();

        LanceVectorSearch vectorSearch = new LanceVectorSearch(
                vectorColumn, queryVector, k, metric, nprobes, ef, refineFactor, true);
        LanceTableHandle tableHandle = new LanceTableHandle(schemaName, tableName, tablePath, tableId, Optional.of(datasetVersion))
                .withVectorSearch(vectorSearch);

        Descriptor returnedType = new Descriptor(columns.stream()
                .map(column -> new Descriptor.Field(column.getColumnName(), Optional.of(column.getColumnType())))
                .collect(ImmutableList.toImmutableList()));

        return TableFunctionAnalysis.builder()
                .returnedType(returnedType)
                .handle(new LanceVectorSearchFunctionHandle(tableHandle, columns))
                .build();
    }

    private static void validateVectorColumn(Field field, int queryDimension)
    {
        Type prestoType = LanceColumnHandle.toPrestoType(field);
        boolean floatArray = prestoType instanceof ArrayType
                && (((ArrayType) prestoType).getElementType().equals(REAL) || ((ArrayType) prestoType).getElementType().equals(DOUBLE));
        if (!floatArray) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    format("Column '%s' is not a float vector column (type %s)", field.getName(), prestoType));
        }
        if (field.getType() instanceof ArrowType.FixedSizeList) {
            int dimension = ((ArrowType.FixedSizeList) field.getType()).getListSize();
            if (dimension != queryDimension) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                        format("query_vector has dimension %s but column '%s' has dimension %s", queryDimension, field.getName(), dimension));
            }
        }
    }

    private static String stringArgument(Map<String, Argument> arguments, String name)
    {
        Object value = ((ScalarArgument) arguments.get(name)).getValue();
        if (value == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s must not be null", name.toLowerCase(ENGLISH)));
        }
        return ((Slice) value).toStringUtf8();
    }

    private static int intArgument(Map<String, Argument> arguments, String name, int minimum)
    {
        Object value = ((ScalarArgument) arguments.get(name)).getValue();
        if (value == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s must not be null", name.toLowerCase(ENGLISH)));
        }
        long longValue = (Long) value;
        if (longValue < minimum || longValue > Integer.MAX_VALUE) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    format("%s must be between %s and %s, got %s", name.toLowerCase(ENGLISH), minimum, Integer.MAX_VALUE, longValue));
        }
        return (int) longValue;
    }

    private static Optional<Integer> optionalIntArgument(Map<String, Argument> arguments, String name)
    {
        Object value = ((ScalarArgument) arguments.get(name)).getValue();
        if (value == null) {
            return Optional.empty();
        }
        long longValue = (Long) value;
        if (longValue < 1 || longValue > Integer.MAX_VALUE) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    format("%s must be a positive integer, got %s", name.toLowerCase(ENGLISH), longValue));
        }
        return Optional.of((int) longValue);
    }

    private static List<Double> queryVectorArgument(Map<String, Argument> arguments)
    {
        Object value = ((ScalarArgument) arguments.get(QUERY_VECTOR_ARG)).getValue();
        if (value == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "query_vector must not be null");
        }
        Block block = (Block) value;
        if (block.getPositionCount() == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "query_vector must not be empty");
        }
        ImmutableList.Builder<Double> vector = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "query_vector must not contain nulls");
            }
            vector.add(DOUBLE.getDouble(block, i));
        }
        return vector.build();
    }
}
