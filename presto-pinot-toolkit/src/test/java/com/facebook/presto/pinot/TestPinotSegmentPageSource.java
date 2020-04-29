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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.testng.annotations.Test;

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

public class TestPinotSegmentPageSource
        extends TestPinotQueryBase
{
    private static final Random RANDOM = new Random(1234);
    private static final int NUM_ROWS = 100;

    private static final Set<DataSchema.ColumnDataType> UNSUPPORTED_TYPES = ImmutableSet.of(
            DataSchema.ColumnDataType.OBJECT, DataSchema.ColumnDataType.BYTES);
    private static final List<DataSchema.ColumnDataType> ALL_TYPES = Arrays.stream(DataSchema.ColumnDataType.values())
            .filter(x -> !UNSUPPORTED_TYPES.contains(x)).collect(toImmutableList());
    private static final DataSchema.ColumnDataType[] ALL_TYPES_ARRAY = ALL_TYPES.toArray(new DataSchema.ColumnDataType[0]);

    private static String generateRandomStringWithLength(int length)
    {
        byte[] array = new byte[length];
        RANDOM.nextBytes(array);
        return new String(array, UTF_8);
    }

    private List<PinotColumnHandle> createPinotColumnHandlesWithAllTypes()
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

    private FieldSpec getFieldSpec(String columnName, DataSchema.ColumnDataType columnDataType)
    {
        switch (columnDataType) {
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
            default:
                throw new IllegalStateException("Unexpected column type " + columnDataType);
        }
    }

    private static final class SimpleDataTable
            implements DataTable
    {
        private final DataSchema dataSchema;
        private final int numRows;
        private final Object[][] data;

        public SimpleDataTable(int numRows, DataSchema dataSchema)
        {
            this.numRows = numRows;
            this.dataSchema = dataSchema;
            this.data = new Object[numRows][];
            for (int i = 0; i < numRows; ++i) {
                this.data[i] = new Object[dataSchema.size()];
            }
        }

        @Override
        public void addException(ProcessingException e)
        {
            throw new UnsupportedOperationException("Unsupported", e);
        }

        @Override
        public byte[] toBytes()
        {
            throw new UnsupportedOperationException("Unsupported");
        }

        @Override
        public Map<String, String> getMetadata()
        {
            return ImmutableMap.of();
        }

        @Override
        public DataSchema getDataSchema()
        {
            return dataSchema;
        }

        @Override
        public int getNumberOfRows()
        {
            return numRows;
        }

        private Object get(int rowIndex, int columnIndex)
        {
            return this.data[rowIndex][columnIndex];
        }

        private void set(int rowIndex, int columnIndex, Object o)
        {
            this.data[rowIndex][columnIndex] = o;
        }

        @Override
        public int getInt(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public long getLong(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public float getFloat(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public double getDouble(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public String getString(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public <T> T getObject(int rowIndex, int columnIndex)
        {
            return (T) get(rowIndex, columnIndex);
        }

        @Override
        public int[] getIntArray(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public long[] getLongArray(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public float[] getFloatArray(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public double[] getDoubleArray(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }

        @Override
        public String[] getStringArray(int rowIndex, int colIndex)
        {
            return getObject(rowIndex, colIndex);
        }
    }

    private DataTable createDataTableWithAllTypes()
    {
        int numColumns = ALL_TYPES.size();
        String[] columnNames = new String[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columnNames[i] = ALL_TYPES.get(i).name();
        }
        DataSchema.ColumnDataType[] columnDataTypes = ALL_TYPES_ARRAY;
        DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
        SimpleDataTable dataTable = new SimpleDataTable(NUM_ROWS, dataSchema);
        for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
            for (int colId = 0; colId < numColumns; colId++) {
                switch (columnDataTypes[colId]) {
                    case INT:
                        dataTable.set(rowId, colId, RANDOM.nextInt());
                        break;
                    case LONG:
                        dataTable.set(rowId, colId, RANDOM.nextLong());
                        break;
                    case FLOAT:
                        dataTable.set(rowId, colId, RANDOM.nextFloat());
                        break;
                    case DOUBLE:
                        dataTable.set(rowId, colId, RANDOM.nextDouble());
                        break;
                    case STRING:
                        dataTable.set(rowId, colId, generateRandomStringWithLength(RANDOM.nextInt(20)));
                        break;
                    case OBJECT:
                        dataTable.set(rowId, colId, (Object) RANDOM.nextDouble());
                        break;
                    case INT_ARRAY:
                        int length = RANDOM.nextInt(20);
                        int[] intArray = new int[length];
                        for (int i = 0; i < length; i++) {
                            intArray[i] = RANDOM.nextInt();
                        }
                        dataTable.set(rowId, colId, intArray);
                        break;
                    case LONG_ARRAY:
                        length = RANDOM.nextInt(20);
                        long[] longArray = new long[length];
                        for (int i = 0; i < length; i++) {
                            longArray[i] = RANDOM.nextLong();
                        }
                        dataTable.set(rowId, colId, longArray);
                        break;
                    case FLOAT_ARRAY:
                        length = RANDOM.nextInt(20);
                        float[] floatArray = new float[length];
                        for (int i = 0; i < length; i++) {
                            floatArray[i] = RANDOM.nextFloat();
                        }
                        dataTable.set(rowId, colId, floatArray);
                        break;
                    case DOUBLE_ARRAY:
                        length = RANDOM.nextInt(20);
                        double[] doubleArray = new double[length];
                        for (int i = 0; i < length; i++) {
                            doubleArray[i] = RANDOM.nextDouble();
                        }
                        dataTable.set(rowId, colId, doubleArray);
                        break;
                    case STRING_ARRAY:
                        length = RANDOM.nextInt(20);
                        String[] stringArray = new String[length];
                        for (int i = 0; i < length; i++) {
                            stringArray[i] = generateRandomStringWithLength(RANDOM.nextInt(20));
                        }
                        dataTable.set(rowId, colId, stringArray);
                        break;
                }
            }
        }
        return dataTable;
    }

    private static final class MockPinotScatterGatherQueryClient
            extends PinotScatterGatherQueryClient
    {
        private final ImmutableList<DataTable> dataTables;

        MockPinotScatterGatherQueryClient(Config pinotConfig, List<DataTable> dataTables)
        {
            super(pinotConfig);
            this.dataTables = ImmutableList.copyOf(dataTables);
        }

        @Override
        public Map<ServerInstance, DataTable> queryPinotServerForDataTable(String pql, String serverHost, List<String> segments, long connectionTimeoutInMillis, boolean ignoreEmptyResponses, int pinotRetryCount)
        {
            ImmutableMap.Builder<ServerInstance, DataTable> response = ImmutableMap.builder();
            for (int i = 0; i < dataTables.size(); ++i) {
                response.put(new ServerInstance(String.format("localhost:%d", i + 9000)), dataTables.get(i));
            }
            return response.build();
        }
    }

    @Test
    public void testPrunedColumns()
    {
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(pinotConfig);
        ConnectorSession session = new TestingConnectorSession(pinotSessionProperties.getSessionProperties());
        List<DataTable> dataTables = IntStream.range(0, 3).mapToObj(i -> createDataTableWithAllTypes()).collect(toImmutableList());
        PinotScatterGatherQueryClient mockPinotQueryClient = new MockPinotScatterGatherQueryClient(new PinotScatterGatherQueryClient.Config(
                pinotConfig.getIdleTimeout().toMillis(),
                pinotConfig.getThreadPoolSize(),
                pinotConfig.getMinConnectionsPerServer(),
                pinotConfig.getMaxBacklogPerServer(),
                pinotConfig.getMaxConnectionsPerServer()), dataTables);
        List<PinotColumnHandle> expectedColumnHandles = createPinotColumnHandlesWithAllTypes();
        PinotSplit mockPinotSplit = new PinotSplit(pinotConnectorId.toString(), PinotSplit.SplitType.SEGMENT, expectedColumnHandles, Optional.empty(), Optional.of("blah"), ImmutableList.of("seg"), Optional.of("host"));

        ImmutableList.Builder<Integer> columnsSurvivingBuilder = ImmutableList.builder();
        for (int i = expectedColumnHandles.size() - 1; i >= 0; i--) {
            if (i % 2 == 0) {
                columnsSurvivingBuilder.add(i);
            }
        }
        List<Integer> columnsSurviving = columnsSurvivingBuilder.build();
        List<PinotColumnHandle> handlesSurviving = columnsSurviving.stream().map(expectedColumnHandles::get).collect(toImmutableList());
        PinotSegmentPageSource pinotSegmentPageSource = new PinotSegmentPageSource(session, pinotConfig, mockPinotQueryClient, mockPinotSplit, handlesSurviving);

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

    @Test
    public void testAllDataTypes()
    {
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(pinotConfig);
        ConnectorSession session = new TestingConnectorSession(pinotSessionProperties.getSessionProperties());
        List<DataTable> dataTables = IntStream.range(0, 3).mapToObj(i -> createDataTableWithAllTypes()).collect(toImmutableList());
        PinotScatterGatherQueryClient mockPinotQueryClient = new MockPinotScatterGatherQueryClient(new PinotScatterGatherQueryClient.Config(
                pinotConfig.getIdleTimeout().toMillis(),
                pinotConfig.getThreadPoolSize(),
                pinotConfig.getMinConnectionsPerServer(),
                pinotConfig.getMaxBacklogPerServer(),
                pinotConfig.getMaxConnectionsPerServer()), dataTables);
        List<PinotColumnHandle> pinotColumnHandles = createPinotColumnHandlesWithAllTypes();
        PinotSplit mockPinotSplit = new PinotSplit(pinotConnectorId.toString(), PinotSplit.SplitType.SEGMENT, pinotColumnHandles, Optional.empty(), Optional.of("blah"), ImmutableList.of("seg"), Optional.of("host"));
        PinotSegmentPageSource pinotSegmentPageSource = new PinotSegmentPageSource(session, pinotConfig, mockPinotQueryClient, mockPinotSplit, pinotColumnHandles);

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
}
