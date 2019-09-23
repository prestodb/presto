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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;

import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.facebook.presto.pinot.PinotQueryBuilder.getPinotQuery;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

/**
 * This class retrieves Pinot data from a Pinot client and re-constructs the data into Presto Pages.
 */

public class PinotPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(PinotPageSource.class);

    private static List<PinotColumnHandle> columnHandles;
    private static List<Type> columnTypes;

    private PinotConfig pinotConfig;
    private PinotSplit split;
    private PinotScatterGatherQueryClient pinotQueryClient;

    // dataTableList stores the dataTable returned from each server. Each dataTable is constructed to a Page, and then destroyed to save memory.
    private Deque<PinotDataTableWithSize> dataTableList = new LinkedList<>();
    private long completedBytes;
    private long readTimeNanos;
    private long estimatedMemoryUsageInBytes;
    private PinotDataTableWithSize currentDataTable;

    private boolean closed;
    private boolean isPinotDataFetched;
    // Stores the mapping between pinot column name and the column index
    Map<String, Integer> pinotColumnNameIndexMap = new HashMap<>();

    public PinotPageSource(PinotConfig pinotConfig, PinotScatterGatherQueryClient pinotQueryClient, PinotSplit split, List<PinotColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
        this.split = requireNonNull(split, "split is null");
        this.pinotQueryClient = requireNonNull(pinotQueryClient, "pinotQueryClient is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return estimatedMemoryUsageInBytes;
    }

    /**
     * @return true if is closed or all Pinot data have been processed.
     */
    @Override
    public boolean isFinished()
    {
        return closed || (isPinotDataFetched && dataTableList.isEmpty());
    }

    /**
     * Iterate through each Pinot {@link org.apache.pinot.common.utils.DataTable}, and construct a {@link com.facebook.presto.spi.Page} out of it.
     *
     * @return constructed page for pinot data.
     */
    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            close();
            return null;
        }
        if (!isPinotDataFetched) {
            fetchPinotData();
        }
        // To reduce memory usage, remove dataTable from dataTableList once it's processed.
        if (currentDataTable != null) {
            estimatedMemoryUsageInBytes -= currentDataTable.getEstimatedSizeInBytes();
        }
        if (dataTableList.size() == 0) {
            close();
            return null;
        }
        currentDataTable = dataTableList.pop();

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        // Note that declared positions in the Page should be the same with number of rows in each Block
        pageBuilder.declarePositions(currentDataTable.getDataTable().getNumberOfRows());
        for (int columnHandleIdx = 0; columnHandleIdx < columnHandles.size(); columnHandleIdx++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnHandleIdx);
            Type columnType = columnTypes.get(columnHandleIdx);
            writeBlock(blockBuilder, columnType, pinotColumnNameIndexMap.get(columnHandles.get(columnHandleIdx).getColumnName()));
        }
        return pageBuilder.build();
    }

    /**
     * Fetch data from Pinot for the current split and store the {@link org.apache.pinot.common.utils.DataTable} returned from each Pinto server.
     */
    private void fetchPinotData()
    {
        log.debug("Fetching data from Pinot for table %s, segment %s", split.getTableName(), split.getSegment());
        long startTimeNanos = System.nanoTime();
        if (pinotColumnNameIndexMap.isEmpty()) {
            int idx = 0;
            for (PinotColumnHandle columnHandle : columnHandles) {
                pinotColumnNameIndexMap.put(columnHandle.getColumnName(), idx++);
            }
        }
        String pinotQuery = getPinotQuery(pinotConfig, columnHandles, split.getPinotFilter(), split.getTimeFilter(), split.getTableName(), split.getLimit());
        Map<ServerInstance, DataTable> dataTableMap = pinotQueryClient.queryPinotServerForDataTable(pinotQuery, split.getHost(), split.getSegment());
        dataTableMap.values().stream()
                // ignore empty tables and tables with 0 rows
                .filter(table -> table != null && table.getNumberOfRows() > 0)
                .forEach(dataTable ->
                {
                    // Store each dataTable which will later be constructed into Pages.
                    // Also update estimatedMemoryUsage, mostly represented by the size of all dataTables, using numberOfRows and fieldTypes combined as an estimate
                    int estimatedTableSizeInBytes = IntStream.rangeClosed(0, dataTable.getDataSchema().size() - 1)
                            .map(i -> getEstimatedColumnSizeInBytes(dataTable.getDataSchema().getColumnDataType(i)) * dataTable.getNumberOfRows())
                            .reduce(0, Integer::sum);
                    dataTableList.add(new PinotDataTableWithSize(dataTable, estimatedTableSizeInBytes));
                    estimatedMemoryUsageInBytes += estimatedTableSizeInBytes;
                });
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        columnHandles.stream()
                .map(PinotPageSource::getTypeForBlock)
                .forEach(types::add);
        columnTypes = types.build();
        readTimeNanos += System.nanoTime() - startTimeNanos;
        isPinotDataFetched = true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
    }

    /**
     * Generates the {@link com.facebook.presto.spi.block.Block} for the specific column from the {@link #currentDataTable}.
     *
     * <p>Based on the original Pinot column types, write as Presto-supported values to {@link com.facebook.presto.spi.block.BlockBuilder}, e.g.
     * FLOAT -> Double, INT -> Long, String -> Slice.
     *
     * @param blockBuilder blockBuilder for the current column
     * @param columnType type of the column
     * @param columnIdx column index
     */

    private void writeBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        Class<?> javaType = columnType.getJavaType();
        DataSchema.ColumnDataType pinotColumnType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIdx);
        if (javaType.equals(boolean.class)) {
            writeBooleanBlock(blockBuilder, columnType, columnIdx);
        }
        else if (javaType.equals(long.class)) {
            writeLongBlock(blockBuilder, columnType, columnIdx);
        }
        else if (javaType.equals(double.class)) {
            writeDoubleBlock(blockBuilder, columnType, columnIdx);
        }
        else if (javaType.equals(Slice.class)) {
            writeSliceBlock(blockBuilder, columnType, columnIdx);
        }
        else {
            throw new PrestoException(
                    PINOT_UNSUPPORTED_COLUMN_TYPE,
                    String.format(
                            "Failed to write column %s. pinotColumnType %s, javaType %s",
                            columnHandles.get(columnIdx).getColumnName(), pinotColumnType, javaType));
        }
    }

    private void writeBooleanBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        IntStream.rangeClosed(0, currentDataTable.getDataTable().getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeBoolean(blockBuilder, getBoolean(i, columnIdx));
            completedBytes++;
        });
    }

    private void writeLongBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        IntStream.rangeClosed(0, currentDataTable.getDataTable().getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeLong(blockBuilder, getLong(i, columnIdx));
            completedBytes += Long.BYTES;
        });
    }

    private void writeDoubleBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        IntStream.rangeClosed(0, currentDataTable.getDataTable().getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeDouble(blockBuilder, getDouble(i, columnIdx));
            completedBytes += Double.BYTES;
        });
    }

    private void writeSliceBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        IntStream.rangeClosed(0, currentDataTable.getDataTable().getNumberOfRows() - 1).forEach(i ->
        {
            Slice slice = getSlice(i, columnIdx);
            columnType.writeSlice(blockBuilder, slice, 0, slice.length());
            completedBytes += slice.getBytes().length;
        });
    }

    private static Type getType(int columnIndex)
    {
        checkArgument(columnIndex < columnHandles.size(), "Invalid field index");
        return columnHandles.get(columnIndex).getColumnType();
    }

    private boolean getBoolean(int rowIndex, int columnIndex)
    {
        return Boolean.getBoolean(currentDataTable.getDataTable().getString(rowIndex, columnIndex));
    }

    private long getLong(int rowIndex, int columnIndex)
    {
        DataSchema.ColumnDataType dataType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        // Note columnType in the dataTable could be different from the original columnType in the columnHandle.
        // e.g. when original column type is int/long and aggregation value is requested, the returned dataType from Pinot would be double.
        // So need to cast it back to the original columnType.
        if (dataType.equals(DataSchema.ColumnDataType.DOUBLE)) {
            return (long) currentDataTable.getDataTable().getDouble(rowIndex, columnIndex);
        }
        if (dataType.equals(DataSchema.ColumnDataType.INT)) {
            return (long) currentDataTable.getDataTable().getInt(rowIndex, columnIndex);
        }
        return currentDataTable.getDataTable().getLong(rowIndex, columnIndex);
    }

    private double getDouble(int rowIndex, int columnIndex)
    {
        DataSchema.ColumnDataType dataType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        if (dataType.equals(DataType.FLOAT)) {
            return currentDataTable.getDataTable().getFloat(rowIndex, columnIndex);
        }
        return currentDataTable.getDataTable().getDouble(rowIndex, columnIndex);
    }

    private Slice getSlice(int rowIndex, int columnIndex)
    {
        checkColumnType(columnIndex, VARCHAR);
        DataSchema.ColumnDataType columnType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        switch (columnType) {
            case INT_ARRAY:
                int[] intArray = currentDataTable.getDataTable().getIntArray(rowIndex, columnIndex);
                return utf8Slice(Arrays.toString(intArray));
            case LONG_ARRAY:
                long[] longArray = currentDataTable.getDataTable().getLongArray(rowIndex, columnIndex);
                return utf8Slice(Arrays.toString(longArray));
            case FLOAT_ARRAY:
                float[] floatArray = currentDataTable.getDataTable().getFloatArray(rowIndex, columnIndex);
                return utf8Slice(Arrays.toString(floatArray));
            case DOUBLE_ARRAY:
                double[] doubleArray = currentDataTable.getDataTable().getDoubleArray(rowIndex, columnIndex);
                return utf8Slice(Arrays.toString(doubleArray));
            case STRING_ARRAY:
                String[] stringArray = currentDataTable.getDataTable().getStringArray(rowIndex, columnIndex);
                return utf8Slice(Arrays.toString(stringArray));
            case STRING:
                String field = currentDataTable.getDataTable().getString(rowIndex, columnIndex);
                if (field == null || field.isEmpty()) {
                    return Slices.EMPTY_SLICE;
                }
                return Slices.utf8Slice(field);
        }
        return Slices.EMPTY_SLICE;
    }

    /**
     * Get estimated size in bytes for the Pinot column.
     * Deterministic for numeric fields; use estimate for other types to save calculation.
     *
     * @param dataType FieldSpec.dataType for Pinot column.
     * @return estimated size in bytes.
     */
    private int getEstimatedColumnSizeInBytes(DataSchema.ColumnDataType dataType)
    {
        if (dataType.isNumber()) {
            switch (dataType) {
                case LONG:
                    return Long.BYTES;
                case FLOAT:
                    return Float.BYTES;
                case DOUBLE:
                    return Double.BYTES;
                case INT:
                default:
                    return Integer.BYTES;
            }
        }
        return pinotConfig.getEstimatedSizeInBytesForNonNumericColumn();
    }

    private static void checkColumnType(int columnIndex, Type expected)
    {
        Type actual = getType(columnIndex);
        checkArgument(actual.equals(expected), "Expected column %s to be type %s but is %s", columnIndex, expected, actual);
    }

    private static Type getTypeForBlock(PinotColumnHandle pinotColumnHandle)
    {
        if (pinotColumnHandle.getColumnType().equals(INTEGER)) {
            return BIGINT;
        }
        return pinotColumnHandle.getColumnType();
    }

    private static class PinotDataTableWithSize
    {
        final DataTable dataTable;
        int estimatedSizeInBytes;

        PinotDataTableWithSize(DataTable dataTable, int estimatedSizeInBytes)
        {
            this.dataTable = requireNonNull(dataTable);
            this.estimatedSizeInBytes = estimatedSizeInBytes;
        }

        public DataTable getDataTable()
        {
            return dataTable;
        }

        public int getEstimatedSizeInBytes()
        {
            return estimatedSizeInBytes;
        }
    }
}
