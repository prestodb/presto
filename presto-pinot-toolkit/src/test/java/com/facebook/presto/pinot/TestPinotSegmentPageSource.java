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
package com.facebook.presto.pinot;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VariableWidthType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.connector.presto.PinotScatterGatherQueryClient;
import org.apache.pinot.connector.presto.grpc.Utils;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Test(singleThreaded = true)
public class TestPinotSegmentPageSource
        extends TestPinotQueryBase
{
    private static final Random RANDOM = new Random(1234);
    protected static final int NUM_ROWS = 100;
    private PinotScatterGatherQueryClient mockPinotQueryClient;

    private static final Set<DataSchema.ColumnDataType> UNSUPPORTED_TYPES = ImmutableSet.of(
            DataSchema.ColumnDataType.OBJECT, DataSchema.ColumnDataType.BYTES);
    protected static final List<DataSchema.ColumnDataType> ALL_TYPES = Arrays.stream(DataSchema.ColumnDataType.values())
            .filter(x -> !UNSUPPORTED_TYPES.contains(x)).collect(toImmutableList());
    private static final DataSchema.ColumnDataType[] ALL_TYPES_ARRAY = ALL_TYPES.toArray(new DataSchema.ColumnDataType[0]);

    private static String generateRandomStringWithLength(int length)
    {
        byte[] array = new byte[length];
        RANDOM.nextBytes(array);
        return new String(array, UTF_8);
    }

    protected List<PinotColumnHandle> createPinotColumnHandlesWithAllTypes()
    {
        DataSchema.ColumnDataType[] columnDataTypes = ALL_TYPES_ARRAY;
        int numColumns = columnDataTypes.length;
        ImmutableList.Builder<PinotColumnHandle> handles = ImmutableList.builder();
        for (int i = 0; i < numColumns; i++) {
            DataSchema.ColumnDataType columnDataType = columnDataTypes[i];
            String columnName = "column" + i;
            handles.add(new PinotColumnHandle(columnName, PinotColumnUtils.getPrestoTypeFromPinotType(getFieldSpec(columnName, columnDataType), false, false), PinotColumnHandle.PinotColumnType.REGULAR));
        }
        return handles.build();
    }

    protected FieldSpec getFieldSpec(String columnName, DataSchema.ColumnDataType columnDataType)
    {
        switch (columnDataType) {
            case BOOLEAN:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.BOOLEAN, true);
            case INT:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.INT, true);
            case LONG:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.LONG, true);
            case FLOAT:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.FLOAT, true);
            case DOUBLE:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.DOUBLE, true);
            case STRING:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.STRING, true);
            case BYTES:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.BYTES, true);
            case TIMESTAMP:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.TIMESTAMP, true);
            case JSON:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.JSON, true);
            case BOOLEAN_ARRAY:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.BOOLEAN, false);
            case INT_ARRAY:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.INT, false);
            case LONG_ARRAY:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.LONG, false);
            case FLOAT_ARRAY:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.FLOAT, false);
            case DOUBLE_ARRAY:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.DOUBLE, false);
            case STRING_ARRAY:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.STRING, false);
            case BYTES_ARRAY:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.BYTES, false);
            case TIMESTAMP_ARRAY:
                return new DimensionFieldSpec(columnName, FieldSpec.DataType.TIMESTAMP, false);
            default:
                throw new IllegalStateException("Unexpected column type " + columnDataType);
        }
    }

    protected static DataTable createDataTableWithAllTypes()
    {
        try {
            int numColumns = ALL_TYPES.size();
            String[] columnNames = new String[numColumns];
            for (int i = 0; i < numColumns; i++) {
                columnNames[i] = ALL_TYPES.get(i).name();
            }
            DataSchema.ColumnDataType[] columnDataTypes = ALL_TYPES_ARRAY;
            DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
            DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
            for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
                dataTableBuilder.startRow();
                for (int colId = 0; colId < numColumns; colId++) {
                    switch (columnDataTypes[colId]) {
                        case BOOLEAN:
                            dataTableBuilder.setColumn(colId, RANDOM.nextBoolean());
                            break;
                        case INT:
                            dataTableBuilder.setColumn(colId, RANDOM.nextInt());
                            break;
                        case LONG:
                        case TIMESTAMP:
                            dataTableBuilder.setColumn(colId, RANDOM.nextLong());
                            break;
                        case FLOAT:
                            dataTableBuilder.setColumn(colId, RANDOM.nextFloat());
                            break;
                        case DOUBLE:
                            dataTableBuilder.setColumn(colId, RANDOM.nextDouble());
                            break;
                        case STRING:
                            dataTableBuilder.setColumn(colId, generateRandomStringWithLength(RANDOM.nextInt(20)));
                            break;
                        case OBJECT:
                            dataTableBuilder.setColumn(colId, (Object) RANDOM.nextDouble());
                            break;
                        case BOOLEAN_ARRAY:
                            int length = RANDOM.nextInt(20);
                            int[] booleanArray = new int[length];
                            for (int i = 0; i < length; i++) {
                                booleanArray[i] = RANDOM.nextInt(2);
                            }
                            dataTableBuilder.setColumn(colId, booleanArray);
                            break;
                        case INT_ARRAY:
                            length = RANDOM.nextInt(20);
                            int[] intArray = new int[length];
                            for (int i = 0; i < length; i++) {
                                intArray[i] = RANDOM.nextInt();
                            }
                            dataTableBuilder.setColumn(colId, intArray);
                            break;
                        case LONG_ARRAY:
                        case TIMESTAMP_ARRAY:
                            length = RANDOM.nextInt(20);
                            long[] longArray = new long[length];
                            for (int i = 0; i < length; i++) {
                                longArray[i] = RANDOM.nextLong();
                            }
                            dataTableBuilder.setColumn(colId, longArray);
                            break;
                        case FLOAT_ARRAY:
                            length = RANDOM.nextInt(20);
                            float[] floatArray = new float[length];
                            for (int i = 0; i < length; i++) {
                                floatArray[i] = RANDOM.nextFloat();
                            }
                            dataTableBuilder.setColumn(colId, floatArray);
                            break;
                        case DOUBLE_ARRAY:
                            length = RANDOM.nextInt(20);
                            double[] doubleArray = new double[length];
                            for (int i = 0; i < length; i++) {
                                doubleArray[i] = RANDOM.nextDouble();
                            }
                            dataTableBuilder.setColumn(colId, doubleArray);
                            break;
                        case STRING_ARRAY:
                        case BYTES_ARRAY:
                            length = RANDOM.nextInt(20);
                            String[] stringArray = new String[length];
                            for (int i = 0; i < length; i++) {
                                stringArray[i] = generateRandomStringWithLength(RANDOM.nextInt(20));
                            }
                            dataTableBuilder.setColumn(colId, stringArray);
                            break;
                        case JSON:
                            dataTableBuilder.setColumn(colId,
                                    "{ " + generateRandomStringWithLength(RANDOM.nextInt(5)) + " : "
                                        + generateRandomStringWithLength(RANDOM.nextInt(10)) + " }");
                            break;
                        case BYTES:
                            try {
                                dataTableBuilder.setColumn(colId,
                                        Hex.decodeHex("0DE0B6B3A7640000".toCharArray())); // Hex of BigDecimal.ONE
                            }
                            catch (DecoderException e) {
                                throw new RuntimeException(e);
                            }
                            break;
                        default:
                            throw new RuntimeException("Unsupported type - " + columnDataTypes[colId]);
                    }
                }
                dataTableBuilder.finishRow();
            }
            return dataTableBuilder.build();
        }
        catch (IOException e) {
            Assert.fail("Failed to create Pinot DataTable with all types", e);
            throw new RuntimeException(e);
        }
    }

    private static final class MockPinotScatterGatherQueryClient
            extends PinotScatterGatherQueryClient
    {
        private ImmutableList<DataTable> dataTables;

        MockPinotScatterGatherQueryClient(Config pinotConfig)
        {
            super(pinotConfig);
        }

        public void setDataTables(List<DataTable> dataTables)
        {
            this.dataTables = ImmutableList.copyOf(dataTables);
        }

        @Override
        public Map<ServerInstance, DataTable> queryPinotServerForDataTable(String pql, String serverHost, List<String> segments, long connectionTimeoutInMillis, boolean ignoreEmptyResponses, int pinotRetryCount)
        {
            ImmutableMap.Builder<ServerInstance, DataTable> response = ImmutableMap.builder();
            for (int i = 0; i < dataTables.size(); ++i) {
                response.put(new ServerInstance(Utils.toInstanceConfig(String.format("Server_localhost_%d", i + 9000))), dataTables.get(i));
            }
            return response.build();
        }
    }

    PinotSegmentPageSource getPinotSegmentPageSource(
            ConnectorSession session,
            List<DataTable> dataTables,
            PinotSplit mockPinotSplit,
            List<PinotColumnHandle> pinotColumnHandles)
    {
        if (mockPinotQueryClient == null) {
            mockPinotQueryClient = new MockPinotScatterGatherQueryClient(new PinotScatterGatherQueryClient.Config(
                    pinotConfig.getIdleTimeout().toMillis(),
                    pinotConfig.getThreadPoolSize(),
                    pinotConfig.getMinConnectionsPerServer(),
                    pinotConfig.getMaxBacklogPerServer(),
                    pinotConfig.getMaxConnectionsPerServer()));
        }
        ((MockPinotScatterGatherQueryClient) mockPinotQueryClient).setDataTables(dataTables);
        PinotSegmentPageSource pinotSegmentPageSource = new PinotSegmentPageSource(session, pinotConfig, mockPinotQueryClient, mockPinotSplit, pinotColumnHandles);
        return pinotSegmentPageSource;
    }

    @Test
    public void testPrunedColumns()
    {
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(pinotConfig);
        ConnectorSession session = new TestingConnectorSession(pinotSessionProperties.getSessionProperties());
        List<DataTable> dataTables = IntStream.range(0, 3).mapToObj(i -> createDataTableWithAllTypes()).collect(toImmutableList());
        List<PinotColumnHandle> expectedColumnHandles = createPinotColumnHandlesWithAllTypes();
        PinotSplit mockPinotSplit = new PinotSplit(pinotConnectorId.toString(), PinotSplit.SplitType.SEGMENT, expectedColumnHandles, Optional.empty(), Optional.of("blah"), ImmutableList.of("seg"), Optional.of("host"), getGrpcPort());

        ImmutableList.Builder<Integer> columnsSurvivingBuilder = ImmutableList.builder();
        for (int i = expectedColumnHandles.size() - 1; i >= 0; i--) {
            if (i % 2 == 0) {
                columnsSurvivingBuilder.add(i);
            }
        }
        List<Integer> columnsSurviving = columnsSurvivingBuilder.build();
        List<PinotColumnHandle> handlesSurviving = columnsSurviving.stream().map(expectedColumnHandles::get).collect(toImmutableList());
        PinotSegmentPageSource pinotSegmentPageSource = getPinotSegmentPageSource(session, dataTables, mockPinotSplit, handlesSurviving);

        for (int i = 0; i < dataTables.size(); ++i) {
            Page page = requireNonNull(pinotSegmentPageSource.getNextPage(), "Expected a valid page");
            Assert.assertEquals(page.getChannelCount(), columnsSurviving.size());
            for (int j = 0; j < columnsSurviving.size(); ++j) {
                Block block = page.getBlock(j);
                int originalColumnIndex = columnsSurviving.get(j);
                Type type = PinotColumnUtils.getPrestoTypeFromPinotType(getFieldSpec("dontcare", ALL_TYPES.get(originalColumnIndex)), false, false);
                long maxHashCode = Long.MIN_VALUE;
                for (int k = 0; k < NUM_ROWS; k++) {
                    maxHashCode = Math.max(type.hash(block, k), maxHashCode);
                }
                Assert.assertTrue(maxHashCode != 0, "Not all column values can have hash code 0");
            }
        }
    }

    Optional<Integer> getGrpcPort()
    {
        return Optional.empty();
    }

    @Test
    public void testAllDataTypes()
    {
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(pinotConfig);
        ConnectorSession session = new TestingConnectorSession(pinotSessionProperties.getSessionProperties());
        List<DataTable> dataTables = IntStream.range(0, 3).mapToObj(i -> createDataTableWithAllTypes()).collect(toImmutableList());
        List<PinotColumnHandle> pinotColumnHandles = createPinotColumnHandlesWithAllTypes();
        PinotSplit mockPinotSplit = new PinotSplit(pinotConnectorId.toString(), PinotSplit.SplitType.SEGMENT, pinotColumnHandles, Optional.empty(), Optional.of("blah"), ImmutableList.of("seg"), Optional.of("host"), getGrpcPort());
        PinotSegmentPageSource pinotSegmentPageSource = getPinotSegmentPageSource(session, dataTables, mockPinotSplit, pinotColumnHandles);

        for (int i = 0; i < dataTables.size(); ++i) {
            Page page = requireNonNull(pinotSegmentPageSource.getNextPage(), "Expected a valid page");
            for (int j = 0; j < ALL_TYPES.size(); ++j) {
                Block block = page.getBlock(j);
                Type type = PinotColumnUtils.getPrestoTypeFromPinotType(getFieldSpec("dontcare", ALL_TYPES.get(j)), false, false);
                long maxHashCode = Long.MIN_VALUE;
                for (int k = 0; k < NUM_ROWS; ++k) {
                    maxHashCode = Math.max(type.hash(block, k), maxHashCode);
                }
                Assert.assertTrue(maxHashCode != 0, "Not all column values can have hash code 0");
            }
        }
    }

    @Test
    public void testMultivaluedType()
            throws IOException
    {
        String[] columnNames = {"col1", "col2"};
        DataSchema.ColumnDataType[] columnDataTypes = {DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY};
        DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
        String[] stringArray = {"stringVal1", "stringVal2"};
        int[] intArray = {10, 34, 67};
        DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
        dataTableBuilder.startRow();
        dataTableBuilder.setColumn(0, intArray);
        dataTableBuilder.setColumn(1, stringArray);
        dataTableBuilder.finishRow();
        DataTable dataTable = dataTableBuilder.build();

        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(pinotConfig);
        ConnectorSession session = new TestingConnectorSession(pinotSessionProperties.getSessionProperties());
        List<PinotColumnHandle> pinotColumnHandles = ImmutableList.of(
                new PinotColumnHandle(columnNames[0], PinotColumnUtils.getPrestoTypeFromPinotType(getFieldSpec(columnNames[0], columnDataTypes[0]), false, false), PinotColumnHandle.PinotColumnType.REGULAR),
                new PinotColumnHandle(columnNames[1], PinotColumnUtils.getPrestoTypeFromPinotType(getFieldSpec(columnNames[1], columnDataTypes[1]), false, false), PinotColumnHandle.PinotColumnType.REGULAR));
        PinotSplit mockPinotSplit = new PinotSplit(pinotConnectorId.toString(), PinotSplit.SplitType.SEGMENT, pinotColumnHandles, Optional.empty(), Optional.of("blah"), ImmutableList.of("seg"), Optional.of("host"), getGrpcPort());
        PinotSegmentPageSource pinotSegmentPageSource = getPinotSegmentPageSource(session, ImmutableList.of(dataTable), mockPinotSplit, pinotColumnHandles);

        Page page = requireNonNull(pinotSegmentPageSource.getNextPage(), "Expected a valid page");

        for (int i = 0; i < columnDataTypes.length; i++) {
            Block block = page.getBlock(i);
            Type type = PinotColumnUtils.getPrestoTypeFromPinotType(getFieldSpec(columnNames[i], columnDataTypes[i]), false, false);
            Assert.assertTrue(type instanceof ArrayType, "presto type should be array");
            if (((ArrayType) type).getElementType() instanceof IntegerType) {
                Assert.assertTrue(block.getBlock(0).getInt(0) == 10, "Array element not matching");
                Assert.assertTrue(block.getBlock(0).getInt(1) == 34, "Array element not matching");
                Assert.assertTrue(block.getBlock(0).getInt(2) == 67, "Array element not matching");
            }
            else if (((ArrayType) type).getElementType() instanceof VariableWidthType) {
                Type type1 = ((ArrayType) type).getElementType();
                Assert.assertTrue(block.getBlock(0) instanceof VariableWidthBlock);
                VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block.getBlock(0);
                Assert.assertTrue("stringVal1".equals(new String(variableWidthBlock.getSlice(0, 0, variableWidthBlock.getSliceLength(0)).getBytes())), "Array element not matching");
                Assert.assertTrue("stringVal2".equals(new String(variableWidthBlock.getSlice(1, 0, variableWidthBlock.getSliceLength(1)).getBytes())), "Array element not matching");
            }
        }
    }
}
