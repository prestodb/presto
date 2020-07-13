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
package com.facebook.presto.hive.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class AggregatedOrcPageSource
        implements ConnectorPageSource
{
    private final List<HiveColumnHandle> columnHandles;
    private final Footer footer;
    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;

    // Prepare the one required record by looking at the aggregations and stats in footer
    private static final int batchSize = 1;

    private boolean completed;
    private long readTimeNanos;
    private long completedBytes;

    public AggregatedOrcPageSource(List<HiveColumnHandle> columnHandles, Footer footer, TypeManager typeManager, StandardFunctionResolution functionResolution)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.footer = requireNonNull(footer, "footer is null");
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
            AggregationNode.Aggregation aggregation = columnHandle.getPartialAggregation().get();
            int columnIndex = columnHandle.getHiveColumnIndex();
            Type type = typeManager.getType(columnHandle.getTypeSignature());

            BlockBuilder blockBuilder = type.createBlockBuilder(null, batchSize, 0);

            FunctionHandle functionHandle = aggregation.getFunctionHandle();

            if (functionResolution.isCountFunction(functionHandle)) {
                if (aggregation.getArguments().isEmpty()) {
                    blockBuilder = blockBuilder.writeLong(footer.getNumberOfRows());
                }
                else {
                    writeNonNullCount(columnIndex, blockBuilder);
                }
                completedBytes += INTEGER.getFixedSize();
            }
            else if (functionResolution.isMaxFunction(functionHandle)) {
                writeMinMax(columnIndex, type, columnHandle.getHiveType(), blockBuilder, false);
            }
            else if (functionResolution.isMinFunction(functionHandle)) {
                writeMinMax(columnIndex, type, columnHandle.getHiveType(), blockBuilder, true);
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

    private void writeMinMax(int columnIndex, Type type, HiveType hiveType, BlockBuilder blockBuilder, boolean isMin)
    {
        ColumnStatistics columnStatistics = footer.getFileStats().get(columnIndex + 1);
        OrcType orcType = footer.getTypes().get(columnIndex + 1);

        if (type instanceof FixedWidthType) {
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }

        switch (orcType.getOrcTypeKind()) {
            case SHORT:
            case INT:
            case LONG: {
                Long val = isMin ? columnStatistics.getIntegerStatistics().getMin() : columnStatistics.getIntegerStatistics().getMax();
                if (val == null) {
                    throw new UnsupportedOperationException("No min/max found for orc file. Set session property pushdown_partial_aggregations_into_scan=false and execute query again");
                }
                else {
                    blockBuilder.writeLong(val);
                }
                break;
            }

            case TIMESTAMP:
            case DATE: {
                Integer val = isMin ? columnStatistics.getDateStatistics().getMin() : columnStatistics.getDateStatistics().getMax();
                if (val == null) {
                    throw new UnsupportedOperationException("No min/max found for orc file. Set session property pushdown_partial_aggregations_into_scan=false and execute query again");
                }
                else {
                    blockBuilder.writeLong(Long.valueOf(val));
                }
                break;
            }

            case VARCHAR:
            case CHAR:
            case STRING: {
                Slice val = isMin ? columnStatistics.getStringStatistics().getMin() : columnStatistics.getStringStatistics().getMax();
                if (val == null) {
                    throw new UnsupportedOperationException("No min/max found for orc file. Set session property pushdown_partial_aggregations_into_scan=false and execute query again");
                }
                else {
                    blockBuilder.writeBytes(val, 0, val.length()).closeEntry();
                    completedBytes += val.length();
                }
                break;
            }

            case FLOAT: {
                Double val = isMin ? columnStatistics.getDoubleStatistics().getMin() : columnStatistics.getDoubleStatistics().getMax();
                if (val == null) {
                    throw new UnsupportedOperationException("No min/max found for orc file. Set session property pushdown_partial_aggregations_into_scan=false and execute query again");
                }
                else {
                    blockBuilder.writeLong(floatToRawIntBits(val.floatValue()));
                }
                break;
            }

            case DOUBLE: {
                Double val = isMin ? columnStatistics.getDoubleStatistics().getMin() : columnStatistics.getDoubleStatistics().getMax();
                if (val == null) {
                    throw new UnsupportedOperationException("No min/max found for orc file. Set session property pushdown_partial_aggregations_into_scan=false and execute query again");
                }
                else {
                    type.writeDouble(blockBuilder, val);
                }
                break;
            }

            case DECIMAL:
                BigDecimal val = isMin ? columnStatistics.getDecimalStatistics().getMin() : columnStatistics.getDecimalStatistics().getMax();
                if (val == null) {
                    throw new UnsupportedOperationException("No min/max found for orc file. Set session property pushdown_partial_aggregations_into_scan=false and execute query again");
                }
                else {
                    Type definedType = hiveType.getType(typeManager);
                    if (Decimals.isShortDecimal(definedType)) {
                        blockBuilder.writeLong(val.unscaledValue().longValue());
                    }
                    else {
                        type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(val.unscaledValue()));
                    }
                }
                break;

            case BYTE:
            case BOOLEAN:
            case BINARY:
            case UNION:
            case LIST:
            case STRUCT:
            case MAP:
            default:
                throw new IllegalArgumentException("Unsupported type: " + orcType.getOrcTypeKind());
        }
    }

    private void writeNonNullCount(int columnIndex, BlockBuilder blockBuilder)
    {
        ColumnStatistics columnStatistics = footer.getFileStats().get(columnIndex + 1);
        if (!columnStatistics.hasNumberOfValues()) {
            throw new UnsupportedOperationException("Number of values not set for orc file. Set session property pushdown_partial_aggregations_into_scan=false and execute query again");
        }
        blockBuilder.writeLong(columnStatistics.getNumberOfValues());
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
