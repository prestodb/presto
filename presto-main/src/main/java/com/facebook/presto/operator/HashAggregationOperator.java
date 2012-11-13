package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class HashAggregationOperator
        implements Operator
{
    private final Operator source;
    private final int groupByChannel;
    private final List<Provider<AggregationFunction>> functionProviders;
    private final List<ProjectionFunction> projections;
    private final List<TupleInfo> tupleInfos;

    public HashAggregationOperator(Operator source, int groupByChannel, List<Provider<AggregationFunction>> functionProviders, List<ProjectionFunction> projections)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkArgument(groupByChannel >= 0, "groupByChannel is negative");
        Preconditions.checkNotNull(functionProviders, "functionProviders is null");
        Preconditions.checkArgument(!functionProviders.isEmpty(), "functionProviders is empty");
        Preconditions.checkNotNull(projections, "projections is null");
        Preconditions.checkArgument(!projections.isEmpty(), "projections is empty");

        this.source = source;
        this.groupByChannel = groupByChannel;
        this.functionProviders = functionProviders;
        this.projections = projections;

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
    }

    @Override
    public int getChannelCount()
    {
        return projections.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new AbstractIterator<Page>()
        {
            private final Map<Tuple, AggregationFunction[]> aggregationMap = new HashMap<>();
            private Iterator<Entry<Tuple, AggregationFunction[]>> aggregations;

            @Override
            protected Page computeNext()
            {
                // process all data ahead of time
                if (aggregations == null) {
                    aggregate();
                }

                // if no more data, return null
                if (!aggregations.hasNext()) {
                    endOfData();
                    return null;
                }

                BlockBuilder[] outputs = new BlockBuilder[projections.size()];
                for (int i = 0; i < outputs.length; i++) {
                    outputs[i] = new BlockBuilder(projections.get(i).getTupleInfo());
                }

                while (!isFull(outputs) && aggregations.hasNext()) {
                    // get next aggregation
                    Entry<Tuple, AggregationFunction[]> aggregation = aggregations.next();

                    // get result tuples
                    Tuple[] results = new Tuple[functionProviders.size() + 1];
                    results[0] = aggregation.getKey();
                    AggregationFunction[] aggregations = aggregation.getValue();
                    for (int i = 1; i < results.length; i++) {
                        results[i] = aggregations[i - 1].evaluate();
                    }

                    // project results into output blocks
                    for (int i = 0; i < projections.size(); i++) {
                        projections.get(i).project(results, outputs[i]);
                    }
                }

                if (outputs[0].isEmpty()) {
                    return endOfData();
                }

                Block[] blocks = new Block[projections.size()];
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = outputs[i].build();
                }

                Page page = new Page(blocks);
                return page;
            }

            private boolean isFull(BlockBuilder... outputs)
            {
                for (BlockBuilder output : outputs) {
                    if (output.isFull()) {
                        return true;
                    }
                }
                return false;
            }

            private void aggregate()
            {
                BlockCursor[] cursors = new BlockCursor[source.getChannelCount()];
                for (Page page : source) {
                    Block[] blocks = page.getBlocks();
                    for (int i = 0; i < blocks.length; i++) {
                        cursors[i] = blocks[i].cursor();
                    }

                    int rows = page.getPositionCount();
                    for (int position = 0; position < rows; position++) {
                        for (BlockCursor cursor : cursors) {
                            checkState(cursor.advanceNextPosition());
                        }

                        Tuple key = cursors[groupByChannel].getTuple();
                        AggregationFunction[] functions = aggregationMap.get(key);
                        if (functions == null) {
                            functions = new AggregationFunction[functionProviders.size()];
                            for (int i = 0; i < functions.length; i++) {
                                functions[i]= functionProviders.get(i).get();
                            }
                            aggregationMap.put(key, functions);
                        }

                        for (AggregationFunction function : functions) {
                            function.add(cursors);
                        }
                    }

                    for (BlockCursor cursor : cursors) {
                        checkState(!cursor.advanceNextPosition());
                    }
                }

                this.aggregations = aggregationMap.entrySet().iterator();
            }
        };
    }
}
