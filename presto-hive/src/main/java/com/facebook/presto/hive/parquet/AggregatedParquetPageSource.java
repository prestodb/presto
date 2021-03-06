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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.common.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.parquet.ParquetTimestampUtils.getTimestampMillis;
import static com.facebook.presto.parquet.ParquetTypeUtils.getShortDecimalValue;
import static com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class AggregatedParquetPageSource
        implements ConnectorPageSource
{
    private final List<HiveColumnHandle> columnHandles;
    private final ParquetMetadata parquetMetadata;
    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;

    // Prepare the one required record by looking at the aggregations and stats in metadata
    private static final int batchSize = 1;

    private boolean completed;
    private long readTimeNanos;
    private long completedBytes;

    public AggregatedParquetPageSource(List<HiveColumnHandle> columnHandles, ParquetMetadata parquetMetadata, TypeManager typeManager, StandardFunctionResolution functionResolution)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.parquetMetadata = requireNonNull(parquetMetadata, "fileMetadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return completed;
    }

    @Override
    public Page getNextPage()
    {
        if (completed) {
            return null;
        }

        long start = System.nanoTime();
        Block[] blocks = new Block[columnHandles.size()];
        for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
            HiveColumnHandle columnHandle = columnHandles.get(fieldId);
            Aggregation aggregation = columnHandle.getPartialAggregation().get();
            Type type = typeManager.getType(columnHandle.getTypeSignature());
            BlockBuilder blockBuilder = type.createBlockBuilder(null, batchSize, 0);
            int columnIndex = columnHandle.getHiveColumnIndex();
            FunctionHandle functionHandle = aggregation.getFunctionHandle();

            if (functionResolution.isCountFunction(functionHandle)) {
                long rowCount = getRowCountFromParquetMetadata(parquetMetadata);
                if (!aggregation.getArguments().isEmpty()) {
                    rowCount -= getNumNulls(parquetMetadata, columnIndex);
                }
                blockBuilder = blockBuilder.writeLong(rowCount);
            }
            else if (functionResolution.isMaxFunction(functionHandle)) {
                writeMinMax(parquetMetadata, columnIndex, blockBuilder, type, columnHandle.getHiveType(), false);
            }
            else if (functionResolution.isMinFunction(functionHandle)) {
                writeMinMax(parquetMetadata, columnIndex, blockBuilder, type, columnHandle.getHiveType(), true);
            }
            else {
                throw new UnsupportedOperationException(aggregation.getFunctionHandle().toString() + " is not supported");
            }
            blocks[fieldId] = blockBuilder.build();
        }

        completed = true;
        readTimeNanos += System.nanoTime() - start;
        return new Page(batchSize, blocks);
    }

    private long getRowCountFromParquetMetadata(ParquetMetadata parquetMetadata)
    {
        long rowCount = 0;
        for (BlockMetaData blockMetaData : parquetMetadata.getBlocks()) {
            rowCount += blockMetaData.getRowCount();
        }
        completedBytes += INTEGER.getFixedSize();
        return rowCount;
    }

    private long getNumNulls(ParquetMetadata parquetMetadata, int columnIndex)
    {
        long numNulls = 0;
        for (BlockMetaData blockMetaData : parquetMetadata.getBlocks()) {
            Statistics statistics = blockMetaData.getColumns().get(columnIndex).getStatistics();
            if (!statistics.isNumNullsSet()) {
                throw new UnsupportedOperationException("Number of nulls not set for parquet file. Set session property hive.pushdown_partial_aggregations_into_scan=false and execute query again");
            }
            numNulls += statistics.getNumNulls();
        }
        completedBytes += INTEGER.getFixedSize();
        return numNulls;
    }

    private void writeMinMax(ParquetMetadata parquetMetadata, int columnIndex, BlockBuilder blockBuilder, Type type, HiveType hiveType, boolean isMin)
    {
        org.apache.parquet.schema.Type parquetType = parquetMetadata.getFileMetaData().getSchema().getType(columnIndex);
        if (parquetType instanceof GroupType) {
            throw new IllegalArgumentException("Unsupported type : " + parquetType.toString());
        }

        Object value = null;
        for (BlockMetaData blockMetaData : parquetMetadata.getBlocks()) {
            Statistics statistics = blockMetaData.getColumns().get(columnIndex).getStatistics();
            if (!statistics.hasNonNullValue()) {
                throw new UnsupportedOperationException("No min/max found for parquet file. Set session property hive.pushdown_partial_aggregations_into_scan=false and execute query again");
            }
            if (isMin) {
                Object currentValue = statistics.genericGetMin();
                if (currentValue != null && (value == null || ((Comparable) currentValue).compareTo(value) < 0)) {
                    value = currentValue;
                }
            }
            else {
                Object currentValue = statistics.genericGetMax();
                if (currentValue != null && (value == null || ((Comparable) currentValue).compareTo(value) > 0)) {
                    value = currentValue;
                }
            }
        }

        if (type instanceof FixedWidthType) {
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }

        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        PrimitiveType.PrimitiveTypeName parquetTypeName = parquetType.asPrimitiveType().getPrimitiveTypeName();
        switch (parquetTypeName) {
            case INT32: {
                blockBuilder.writeLong(Long.valueOf((Integer) value));
                break;
            }
            case INT64: {
                blockBuilder.writeLong((Long) value);
                break;
            }
            case INT96: {
                blockBuilder.writeLong(getTimestampMillis(((Binary) value).getBytes(), 0));
                break;
            }
            case FLOAT: {
                blockBuilder.writeLong(floatToRawIntBits((Float) value));
                break;
            }
            case DOUBLE: {
                type.writeDouble(blockBuilder, (Double) value);
                break;
            }
            case FIXED_LEN_BYTE_ARRAY: {
                byte[] valBytes = ((Binary) value).getBytes();
                DecimalType decimalType = (DecimalType) hiveType.getType(typeManager);
                if (decimalType.isShort()) {
                    blockBuilder.writeLong(getShortDecimalValue(valBytes));
                }
                else {
                    BigInteger bigIntValue = new BigInteger(valBytes);
                    type.writeSlice(blockBuilder, encodeUnscaledValue(bigIntValue));
                }
                break;
            }
            case BINARY: {
                Slice slice = Slices.wrappedBuffer(((Binary) value).getBytes());
                blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
                completedBytes += slice.length();
                break;
            }
            case BOOLEAN:
            default:
                throw new IllegalArgumentException("Unexpected parquet type name: " + parquetTypeName);
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        // no-op
    }
}
