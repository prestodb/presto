package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.aggregation.AggregationFunction;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
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

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class PipelinedAggregationOperator
        implements TupleStream, YieldingIterable<UncompressedBlock>
{
    private final TupleStream groupBySource;
    private final TupleStream aggregationSource;
    private final Provider<AggregationFunction> functionProvider;
    private final TupleInfo info;

    public PipelinedAggregationOperator(TupleStream groupBySource,
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
    public YieldingIterator<UncompressedBlock> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        final Cursor groupByCursor = groupBySource.cursor(session);
        final Cursor aggregationCursor = aggregationSource.cursor(session);
        aggregationCursor.advanceNextPosition();

        // todo add code to advance to watermark position
        return new AbstractYieldingIterator<UncompressedBlock>()
        {
            private long position;

            @Override
            protected UncompressedBlock computeNext()
            {
                BlockBuilder builder = new BlockBuilder(position, info);
                while (!builder.isFull()) {
                    // advance the group by one value
                    AdvanceResult result = groupByCursor.advanceNextValue();
                    if (result != AdvanceResult.SUCCESS) {
                        if (!builder.isEmpty()) {
                            // output current block
                            break;
                        } else if (result == AdvanceResult.MUST_YIELD) {
                            // todo produce partial result
                            return setMustYield();
                        }
                        else if (result == AdvanceResult.FINISHED) {
                            return endOfData();
                        }
                    }

                    // process the group - pipeline can not yield
                    long groupEndPosition = groupByCursor.getCurrentValueEndPosition();
                    if (Cursors.advanceToPositionNoYield(aggregationCursor, groupByCursor.getPosition()) && aggregationCursor.getPosition() <= groupEndPosition) {
                        // create a new aggregate for this group
                        AggregationFunction aggregation = functionProvider.get();

                        // process data
                        aggregation.add(aggregationCursor, groupEndPosition);

                        // calculate final value for this group
                        Tuple value = aggregation.evaluate();

                        builder.append(groupByCursor.getTuple());
                        builder.append(value);
                    }
                }

                // build an output block
                UncompressedBlock block = builder.build();
                position += block.getCount();
                return block;
            }
        };
    }
}
