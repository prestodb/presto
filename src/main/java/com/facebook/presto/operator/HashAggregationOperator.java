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
import com.facebook.presto.block.BlockIterators;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
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
public class HashAggregationOperator
        implements TupleStream, BlockIterable<UncompressedBlock>
{
    private final TupleStream groupBySource;
    private final TupleStream aggregationSource;
    private final Provider<AggregationFunction> functionProvider;
    private final TupleInfo info;

    public HashAggregationOperator(TupleStream groupBySource,
            TupleStream aggregationSource,
            Provider<AggregationFunction> functionProvider)
    {
        Preconditions.checkNotNull(groupBySource, "groupBySource is null");
        Preconditions.checkNotNull(aggregationSource, "aggregationSource is null");
        Preconditions.checkNotNull(functionProvider, "functionProvider is null");

        this.groupBySource = groupBySource;
        this.aggregationSource = aggregationSource;
        this.functionProvider = functionProvider;

        info = new TupleInfo(ImmutableList.<Type>builder()
                .addAll(groupBySource.getTupleInfo().getTypes())
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
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new UncompressedCursor(getTupleInfo(), iterator(session));
    }

    @Override
    public BlockIterator<UncompressedBlock> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        final Cursor groupByCursor = groupBySource.cursor(session);
        final Cursor aggregationCursor = aggregationSource.cursor(session);
        if (!Cursors.advanceNextPositionNoYield(groupByCursor)) {
            return BlockIterators.emptyIterator();
        }
        if (!Cursors.advanceNextPositionNoYield(aggregationCursor)) {
            return BlockIterators.emptyIterator();
        }

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
                    while (!groupByCursor.isFinished() && !aggregationCursor.isFinished()) {
                        // advance the group by one value, if group has been completely processed
                        long groupEndPosition = groupByCursor.getCurrentValueEndPosition();
                        if (groupEndPosition <= aggregationCursor.getPosition()) {
                            AdvanceResult result = groupByCursor.advanceNextValue();
                            if (result != AdvanceResult.SUCCESS) {
                                if (result == MUST_YIELD) {
                                    return setMustYield();
                                }
                                else if (result == AdvanceResult.FINISHED) {
                                    if (aggregationCursor.advanceToPosition(Long.MAX_VALUE) == MUST_YIELD) {
                                        return setMustYield();
                                    }
                                    break;
                                }
                            }
                            groupEndPosition = groupByCursor.getCurrentValueEndPosition();
                        }

                        // advance aggregate cursor to start of group, if necessary
                        if (aggregationCursor.getPosition() < groupEndPosition) {
                            AdvanceResult result = aggregationCursor.advanceToPosition(groupByCursor.getPosition());
                            if (result == MUST_YIELD) {
                                return setMustYield();
                            }
                            else if (result == AdvanceResult.FINISHED) {
                                if (groupByCursor.advanceToPosition(Long.MAX_VALUE) == MUST_YIELD) {
                                    return setMustYield();
                                }
                                break;
                            }
                        }

                        Tuple key = groupByCursor.getTuple();
                        AggregationFunction aggregation = aggregationMap.get(key);
                        if (aggregation == null) {
                            aggregation = functionProvider.get();
                            aggregationMap.put(key, aggregation);
                        }
                        aggregation.add(aggregationCursor, groupEndPosition);
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
        };
    }
}
