package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.aggregation.AggregationFunction;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.dictionary.Dictionary;
import com.facebook.presto.block.dictionary.DictionaryEncodedCursor;
import com.facebook.presto.block.dictionary.DictionaryEncodedTupleStream;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.Iterator;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class DictionaryAggregationOperator
    implements TupleStream, Iterable<UncompressedBlock>
{
    private final DictionaryEncodedTupleStream groupBySource;
    private final TupleStream aggregationSource;
    private final Provider<AggregationFunction> functionProvider;
    private final TupleInfo info;
    private final Dictionary dictionary;

    public DictionaryAggregationOperator(DictionaryEncodedTupleStream groupBySource,
            TupleStream aggregationSource,
            Provider<AggregationFunction> functionProvider)
    {
        Preconditions.checkNotNull(groupBySource, "groupBySource is null");
        Preconditions.checkNotNull(aggregationSource, "aggregationSource is null");
        Preconditions.checkNotNull(functionProvider, "functionProvider is null");

        this.groupBySource = groupBySource;
        this.dictionary = groupBySource.getDictionary();
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
    public Cursor cursor()
    {
        return new UncompressedCursor(getTupleInfo(), iterator());
    }

    @Override
    public Iterator<UncompressedBlock> iterator()
    {
        final DictionaryEncodedCursor groupByCursor = groupBySource.cursor();
        final Cursor aggregationCursor = aggregationSource.cursor();
        aggregationCursor.advanceNextPosition();

        return new AbstractIterator<UncompressedBlock>()
        {
            private final AggregationFunction[] aggregationMap = new AggregationFunction[dictionary.size()];
            private int index = -1;
            private int position = 0;

            @Override
            protected UncompressedBlock computeNext()
            {
                // process all data ahead of time
                if (index < 0) {
                    while (groupByCursor.advanceNextValue()) {
                        int key = groupByCursor.getDictionaryKey();
                        long groupEndPosition = groupByCursor.getCurrentValueEndPosition();
                        if (aggregationCursor.advanceToPosition(groupByCursor.getPosition()) && aggregationCursor.getPosition() <= groupEndPosition) {
                            AggregationFunction aggregation = aggregationMap[key];
                            if (aggregation == null) {
                                aggregation = functionProvider.get();
                                aggregationMap[key] = aggregation;
                            }
                            aggregation.add(aggregationCursor, groupEndPosition);
                        }
                    }
                    index = 0;
                }

                // if no more data, return null
                if (index >= aggregationMap.length) {
                    endOfData();
                    return null;
                }

                BlockBuilder blockBuilder = new BlockBuilder(position, info);
                while (!blockBuilder.isFull() && index < aggregationMap.length) {
                    // calculate final value for this group
                    if (aggregationMap[index] != null) {
                        Tuple key = dictionary.getTuple(index);
                        Tuple value = aggregationMap[index].evaluate();
                        blockBuilder.append(key);
                        blockBuilder.append(value);
                    }
                    index++;
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
