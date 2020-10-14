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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.pinot.PinotScatterGatherQueryClient.ErrorCode;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_DATA_FETCH_EXCEPTION;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_INVALID_PQL_GENERATED;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNCLASSIFIED_ERROR;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

/**
 * This class retrieves Pinot data from a Pinot client, and re-constructs the data into Presto Pages.
 */

public class PinotSegmentPageSource
        implements ConnectorPageSource
{
    private static final Map<ErrorCode, PinotErrorCode> PINOT_ERROR_CODE_MAP = ImmutableMap.of(
            ErrorCode.PINOT_UNCLASSIFIED_ERROR, PINOT_UNCLASSIFIED_ERROR,
            ErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE, PINOT_INSUFFICIENT_SERVER_RESPONSE,
            ErrorCode.PINOT_INVALID_PQL_GENERATED, PINOT_INVALID_PQL_GENERATED);

    protected final List<PinotColumnHandle> columnHandles;
    protected final List<Type> columnTypes;
    protected final PinotConfig pinotConfig;
    protected final PinotSplit split;
    private final PinotScatterGatherQueryClient pinotQueryClient;
    protected final ConnectorSession session;

    // dataTableList stores the dataTable returned from each server. Each dataTable is constructed to a Page, and then destroyed to save memory.
    private final LinkedList<PinotDataTableWithSize> dataTableList = new LinkedList<>();
    protected long completedBytes;
    protected long readTimeNanos;
    protected long estimatedMemoryUsageInBytes;
    protected PinotDataTableWithSize currentDataTable;
    protected boolean closed;
    private boolean isPinotDataFetched;

    public PinotSegmentPageSource(
            ConnectorSession session,
            PinotConfig pinotConfig,
            PinotScatterGatherQueryClient pinotQueryClient,
            PinotSplit split,
            List<PinotColumnHandle> columnHandles)
    {
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
        this.split = requireNonNull(split, "split is null");
        this.pinotQueryClient = pinotQueryClient;
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.session = requireNonNull(session, "session is null");
        this.columnTypes = columnHandles.stream()
                .map(PinotSegmentPageSource::getTypeForBlock)
                .collect(Collectors.toList());
    }

    public static void checkExceptions(DataTable dataTable, PinotSplit split, boolean markDataFetchExceptionsAsRetriable)
    {
        Map<String, String> metadata = dataTable.getMetadata();
        List<String> exceptions = new ArrayList<>();
        metadata.forEach((k, v) -> {
            if (k.startsWith(DataTable.EXCEPTION_METADATA_KEY)) {
                exceptions.add(v);
            }
        });
        if (!exceptions.isEmpty()) {
            throw new PinotException(
                markDataFetchExceptionsAsRetriable ? PINOT_DATA_FETCH_EXCEPTION : PINOT_EXCEPTION,
                split.getSegmentPinotQuery(),
                String.format("Encountered %d pinot exceptions for split %s: %s", exceptions.size(), split, exceptions));
        }
        int numColumnsExpected = split.getExpectedColumnHandles().size();
        int numColumnsActual = dataTable.getDataSchema().size();
        if (numColumnsActual != numColumnsExpected) {
            throw new PinotException(
                    PINOT_EXCEPTION,
                    split.getSegmentPinotQuery(),
                    String.format("Expected pinot to contain %d columns but got %d: %s", numColumnsExpected, numColumnsActual, dataTable.getDataSchema()));
        }
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

        return fillNextPage();
    }

    protected Page fillNextPage()
    {
        // This is the list of handles we came up with when generating the PQL
        // This could be a superset/permutation of the handles being requested in this scan
        List<PinotColumnHandle> expectedColumnHandles = split.getExpectedColumnHandles();
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        // Note that declared positions in the Page should be the same with number of rows in each Block
        pageBuilder.declarePositions(currentDataTable.getDataTable().getNumberOfRows());
        for (int columnHandleIndex = 0; columnHandleIndex < columnHandles.size(); columnHandleIndex++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnHandleIndex);
            Type columnType = columnTypes.get(columnHandleIndex);
            // Write a block for each column in the original order.
            PinotColumnHandle handle = columnHandles.get(columnHandleIndex);

            // map the handle needed by the scan to its index corresponding to the generated PQL
            // All handles requested by the scan should be a subset of the expected handles
            // ie., the expected column handles (corresponding to the generated PQL) can contain
            // extra columns that we drop.
            int indexReturnedByPinot = expectedColumnHandles.indexOf(handle);
            if (indexReturnedByPinot < 0) {
                throw new PinotException(
                        PINOT_INVALID_PQL_GENERATED,
                        split.getSegmentPinotQuery(),
                        String.format("Expected column handle %s to be present in the handles %s corresponding to the segment PQL", handle, expectedColumnHandles));
            }
            writeBlock(blockBuilder, columnType, indexReturnedByPinot);
        }

        return pageBuilder.build();
    }

    /**
     * Fetch data from Pinot for the current split and store the data returned from each Pinot server.
     */
    private void fetchPinotData()
    {
        long startTimeNanos = System.nanoTime();
        try {
            Map<ServerInstance, DataTable> dataTableMap = queryPinot(session, split);
            dataTableMap.values().stream()
                    // ignore empty tables and tables with 0 rows
                    .filter(table -> table != null && table.getNumberOfRows() > 0)
                    .forEach(dataTable ->
                    {
                        checkExceptions(dataTable, split, PinotSessionProperties.isMarkDataFetchExceptionsAsRetriable(session));
                        // Store each dataTable which will later be constructed into Pages.
                        // Also update estimatedMemoryUsage, mostly represented by the size of all dataTables, using numberOfRows and fieldTypes combined as an estimate
                        int estimatedTableSizeInBytes = IntStream.rangeClosed(0, dataTable.getDataSchema().size() - 1)
                                .map(i -> getEstimatedColumnSizeInBytes(dataTable.getDataSchema().getColumnDataType(i)) * dataTable.getNumberOfRows())
                                .reduce(0, Integer::sum);
                        dataTableList.add(new PinotDataTableWithSize(dataTable, estimatedTableSizeInBytes));
                        estimatedMemoryUsageInBytes += estimatedTableSizeInBytes;
                    });

            isPinotDataFetched = true;
        }
        finally {
            readTimeNanos += System.nanoTime() - startTimeNanos;
        }
    }

    private Map<ServerInstance, DataTable> queryPinot(ConnectorSession session, PinotSplit split)
    {
        String pql = split.getSegmentPinotQuery().orElseThrow(() -> new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), "Expected the segment split to contain the pql"));
        String host = split.getSegmentHost().orElseThrow(() -> new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), "Expected the segment split to contain the host"));
        try {
            return ImmutableMap.copyOf(
                    pinotQueryClient.queryPinotServerForDataTable(
                            pql,
                            host,
                            split.getSegments(),
                            PinotSessionProperties.getConnectionTimeout(session).toMillis(),
                            PinotSessionProperties.isIgnoreEmptyResponses(session),
                            PinotSessionProperties.getPinotRetryCount(session)));
        }
        catch (PinotScatterGatherQueryClient.PinotException pe) {
            throw new PinotException(PINOT_ERROR_CODE_MAP.getOrDefault(pe.getErrorCode(), PINOT_UNCLASSIFIED_ERROR), Optional.of(pql), String.format("Error when hitting host %s", host), pe);
        }
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
     * Generates the {@link com.facebook.presto.common.block.Block} for the specific column from the {@link #currentDataTable}.
     *
     * <p>Based on the original Pinot column types, write as Presto-supported values to {@link com.facebook.presto.common.block.BlockBuilder}, e.g.
     * FLOAT -> Double, INT -> Long, String -> Slice.
     *
     * @param blockBuilder blockBuilder for the current column
     * @param columnType type of the column
     * @param columnIndex column index
     */

    private void writeBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        Class<?> javaType = columnType.getJavaType();
        DataSchema.ColumnDataType pinotColumnType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        if (columnType instanceof ArrayType) {
            writeArrayBlock(blockBuilder, columnType, columnIndex);
        }
        else if (javaType.equals(boolean.class)) {
            writeBooleanBlock(blockBuilder, columnType, columnIndex);
        }
        else if (javaType.equals(long.class)) {
            writeLongBlock(blockBuilder, columnType, columnIndex);
        }
        else if (javaType.equals(double.class)) {
            writeDoubleBlock(blockBuilder, columnType, columnIndex);
        }
        else if (javaType.equals(Slice.class)) {
            writeSliceBlock(blockBuilder, columnType, columnIndex);
        }
        else {
            throw new PrestoException(
                    PINOT_UNSUPPORTED_COLUMN_TYPE,
                    String.format(
                            "Failed to write column %s. pinotColumnType %s, javaType %s",
                            split.getExpectedColumnHandles().get(columnIndex).getColumnName(),
                            pinotColumnType,
                            javaType));
        }
    }

    private void writeArrayBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int rowIndex = 0; rowIndex < currentDataTable.getDataTable().getNumberOfRows(); rowIndex++) {
            DataSchema.ColumnDataType columnPinotType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
            Type columnPrestoType = ((ArrayType) columnType).getElementType();
            BlockBuilder childBuilder = blockBuilder.beginBlockEntry();
            switch (columnPinotType) {
                case INT_ARRAY:
                    int[] intArray = currentDataTable.getDataTable().getIntArray(rowIndex, columnIndex);
                    for (int i = 0; i < intArray.length; i++) {
                        // Both the numeric types implement a writeLong method which write if the bounds for
                        // the type allows else throw exception.
                        columnPrestoType.writeLong(childBuilder, intArray[i]);
                        completedBytes += Long.BYTES;
                    }
                    break;
                case LONG_ARRAY:
                    long[] longArray = currentDataTable.getDataTable().getLongArray(rowIndex, columnIndex);
                    for (int i = 0; i < longArray.length; i++) {
                        columnPrestoType.writeLong(childBuilder, longArray[i]);
                        completedBytes += Long.BYTES;
                    }
                    break;
                case FLOAT_ARRAY:
                    float[] floatArray = currentDataTable.getDataTable().getFloatArray(rowIndex, columnIndex);
                    if (columnPrestoType.getJavaType().equals(long.class)) {
                        for (int i = 0; i < floatArray.length; i++) {
                            columnPrestoType.writeLong(childBuilder, (long) floatArray[i]);
                            completedBytes += Long.BYTES;
                        }
                    }
                    else {
                        for (int i = 0; i < floatArray.length; i++) {
                            columnPrestoType.writeDouble(childBuilder, floatArray[i]);
                            completedBytes += Double.BYTES;
                        }
                    }
                    break;
                case DOUBLE_ARRAY:
                    double[] doubleArray = currentDataTable.getDataTable().getDoubleArray(rowIndex, columnIndex);
                    if (columnPrestoType.getJavaType().equals(long.class)) {
                        for (int i = 0; i < doubleArray.length; i++) {
                            columnPrestoType.writeLong(childBuilder, (long) doubleArray[i]);
                            completedBytes += Long.BYTES;
                        }
                    }
                    else {
                        for (int i = 0; i < doubleArray.length; i++) {
                            columnPrestoType.writeDouble(childBuilder, doubleArray[i]);
                            completedBytes += Double.BYTES;
                        }
                    }
                    break;
                case STRING_ARRAY:
                    String[] stringArray = currentDataTable.getDataTable().getStringArray(rowIndex, columnIndex);
                    for (int i = 0; i < stringArray.length; i++) {
                        Slice slice = Slices.utf8Slice(stringArray[i]);
                        childBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
                        completedBytes += slice.getBytes().length;
                    }
                    break;
                default:
                    throw new PrestoException(
                            PINOT_UNSUPPORTED_COLUMN_TYPE,
                            String.format(
                                    "Failed to write column %s. pinotColumnType %s, prestoType %s",
                                    split.getExpectedColumnHandles().get(columnIndex).getColumnName(),
                                    columnPinotType,
                                    columnPrestoType));
            }
            blockBuilder.closeEntry();
        }
    }

    private void writeBooleanBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            columnType.writeBoolean(blockBuilder, getBoolean(i, columnIndex));
            completedBytes++;
        }
    }

    private void writeLongBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            columnType.writeLong(blockBuilder, getLong(i, columnIndex));
            completedBytes += Long.BYTES;
        }
    }

    private void writeDoubleBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            columnType.writeDouble(blockBuilder, getDouble(i, columnIndex));
            completedBytes += Double.BYTES;
        }
    }

    private void writeSliceBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            Slice slice = getSlice(i, columnIndex);
            columnType.writeSlice(blockBuilder, slice, 0, slice.length());
            completedBytes += slice.getBytes().length;
        }
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
        else {
            return currentDataTable.getDataTable().getLong(rowIndex, columnIndex);
        }
    }

    private double getDouble(int rowIndex, int columnIndex)
    {
        DataSchema.ColumnDataType dataType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        if (dataType.equals(DataSchema.ColumnDataType.FLOAT)) {
            return currentDataTable.getDataTable().getFloat(rowIndex, columnIndex);
        }
        else {
            return currentDataTable.getDataTable().getDouble(rowIndex, columnIndex);
        }
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

    private void checkColumnType(int columnIndex, Type expected)
    {
        checkArgument(columnIndex < split.getExpectedColumnHandles().size(), "Invalid field index");
        Type actual = split.getExpectedColumnHandles().get(columnIndex).getDataType();
        checkArgument(actual.equals(expected), "Expected column %s to be type %s but is %s", columnIndex, expected, actual);
    }

    protected static Type getTypeForBlock(PinotColumnHandle pinotColumnHandle)
    {
        if (pinotColumnHandle.getDataType().equals(INTEGER)) {
            return BIGINT;
        }
        return pinotColumnHandle.getDataType();
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    protected static class PinotDataTableWithSize
    {
        DataTable dataTable;
        int estimatedSizeInBytes;

        PinotDataTableWithSize(DataTable dataTable, int estimatedSizeInBytes)
        {
            this.dataTable = dataTable;
            this.estimatedSizeInBytes = estimatedSizeInBytes;
        }

        DataTable getDataTable()
        {
            return dataTable;
        }

        int getEstimatedSizeInBytes()
        {
            return estimatedSizeInBytes;
        }
    }
}
