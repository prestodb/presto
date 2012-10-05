package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.aggregation.AggregationFunction;
import com.facebook.presto.block.AbstractBlockIterator;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterator;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class FinalHashAggregationOperator
        implements TupleStream, BlockIterable<UncompressedBlock>
{
    private final TupleStream groupBySource;
    private final Provider<AggregationFunction> functionProvider;
    private final TupleInfo info;
    private final TupleInfo groupByTypeInfo;

    public FinalHashAggregationOperator(TupleStream source,
            Provider<AggregationFunction> functionProvider)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(functionProvider, "functionProvider is null");

        this.groupBySource = source;
        this.functionProvider = functionProvider;

        // todo assumes aggregate intermediate is only one column
        groupByTypeInfo = new TupleInfo(source.getTupleInfo().getTypes().subList(0, source.getTupleInfo().getFieldCount() - 1));
        info = new TupleInfo(ImmutableList.<Type>builder()
                .addAll(groupByTypeInfo.getTypes())
                .addAll(functionProvider.get().getTupleInfo().getTypes())
                .build());

    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor()
    {
        return new UncompressedCursor(getTupleInfo(), iterator());
    }

    @Override
    public BlockIterator<UncompressedBlock> iterator()
    {
        final Cursor cursor = groupBySource.cursor();

        return new AbstractBlockIterator<UncompressedBlock>()
        {
            private final Map<Tuple, AggregationFunction> aggregationMap = new HashMap<>();
            private Iterator<Entry<Tuple, AggregationFunction>> aggregations;
            private long position;

            @Override
            protected UncompressedBlock computeNext()
            {
                // process all data ahead of time
                if (aggregations == null) {
                    while (!cursor.isFinished()) {
                        AdvanceResult result = cursor.advanceNextValue();
                        if (result != AdvanceResult.SUCCESS) {
                            if (result == MUST_YIELD) {
                                return setMustYield();
                            }
                            else if (result == AdvanceResult.FINISHED) {
                                break;
                            }
                        }

                        Tuple key = getKey();
                        AggregationFunction aggregation = aggregationMap.get(key);
                        if (aggregation == null) {
                            aggregation = functionProvider.get();
                            aggregationMap.put(key, aggregation);
                        }
                        aggregation.addCurrentPosition(cursor);
                    }

                    this.aggregations = aggregationMap.entrySet().iterator();
                }

                // if no more data, return null
                if (!aggregations.hasNext()) {
                    endOfData();
                    return null;
                }

                BlockBuilder blockBuilder = new BlockBuilder(position, info);
                while (!blockBuilder.isFull() && aggregations.hasNext()) {
                    // get next aggregation
                    Entry<Tuple, AggregationFunction> aggregation = aggregations.next();

                    // calculate final value for this group
                    Tuple key = aggregation.getKey();
                    Tuple value = aggregation.getValue().evaluate();
                    blockBuilder.append(key);
                    blockBuilder.append(value);
                }

                // build output block
                UncompressedBlock block = blockBuilder.build();

                // update position
                position += block.getCount();

                return block;
            }

            private Tuple getKey()
            {
                TupleInfo.Builder keyBuilder = groupByTypeInfo.builder();
                for (int field = 0; field < groupByTypeInfo.getFieldCount(); field++) {
                    Type type = groupByTypeInfo.getTypes().get(field);
                    switch (type) {
                        case FIXED_INT_64:
                            keyBuilder.append(cursor.getLong(field));
                            break;
                        case DOUBLE:
                            keyBuilder.append(cursor.getDouble(field));
                            break;
                        case VARIABLE_BINARY:
                            Slice data = cursor.getSlice(field);
                            keyBuilder.append(data);
                            break;
                        default:
                            throw new IllegalStateException("Type not yet supported: " + type);
                    }
                }
                return keyBuilder.build();
            }
        };
    }
}
