package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.aggregation.AggregationFunctionStep;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;

import javax.inject.Provider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.operator.ProjectionFunctions.toTupleInfos;
import static com.google.common.base.Preconditions.checkState;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class HashAggregationOperator
        implements Operator
{
    private final Operator source;
    private final int groupByChannel;
    private final List<Provider<AggregationFunctionStep>> functionProviders;
    private final List<ProjectionFunction> projections;
    private final List<TupleInfo> tupleInfos;
    private final int maxNumberOfGroups;

    public HashAggregationOperator(Operator source,
            int groupByChannel,
            List<Provider<AggregationFunctionStep>> functionProviders,
            List<ProjectionFunction> projections,
            int maxNumberOfGroups)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkArgument(groupByChannel >= 0, "groupByChannel is negative");
        Preconditions.checkNotNull(functionProviders, "functionProviders is null");
        Preconditions.checkNotNull(projections, "projections is null");
        Preconditions.checkArgument(!projections.isEmpty(), "projections is empty");

        this.source = source;
        this.groupByChannel = groupByChannel;
        this.functionProviders = functionProviders;
        this.projections = projections;

        this.tupleInfos = toTupleInfos(projections);
        this.maxNumberOfGroups = maxNumberOfGroups;
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
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new HashAggregationIterator(source, groupByChannel, functionProviders, projections, operatorStats, maxNumberOfGroups);
    }

    private static class HashAggregationIterator
            extends AbstractPageIterator
    {
        private final List<ProjectionFunction> projections;
        private final OperatorStats operatorStats;

        private final Iterator<Entry<Tuple, AggregationFunctionStep[]>> aggregations;
        private final int aggregationFunctionCount;

        private HashAggregationIterator(Operator source,
                int groupByChannel,
                List<Provider<AggregationFunctionStep>> functionProviders,
                List<ProjectionFunction> projections,
                OperatorStats operatorStats,
                int maxNumberOfGroups)
        {
            super(toTupleInfos(projections));
            this.projections = projections;
            this.operatorStats = operatorStats;
            Map<Tuple, AggregationFunctionStep[]> aggregate = aggregate(source, groupByChannel, functionProviders, maxNumberOfGroups);
            this.aggregations = aggregate.entrySet().iterator();
            this.aggregationFunctionCount = functionProviders.size();
        }

        @Override
        protected Page computeNext()
        {
            // if no more data, return null
            if (!aggregations.hasNext()) {
                endOfData();
                return null;
            }

            // todo convert to page builder
            BlockBuilder[] outputs = new BlockBuilder[projections.size()];
            for (int i = 0; i < outputs.length; i++) {
                outputs[i] = new BlockBuilder(projections.get(i).getTupleInfo());
            }

            while (!isFull(outputs) && aggregations.hasNext()) {
                // get next aggregation
                Entry<Tuple, AggregationFunctionStep[]> aggregation = aggregations.next();

                // get result tuples
                Tuple[] results = new Tuple[aggregationFunctionCount + 1];
                results[0] = aggregation.getKey();
                AggregationFunctionStep[] aggregations = aggregation.getValue();
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

        @Override
        protected void doClose()
        {
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

        private Map<Tuple, AggregationFunctionStep[]> aggregate(Operator source,
                int groupByChannel,
                List<Provider<AggregationFunctionStep>> functionProviders,
                int maxNumberOfGroups)
        {
            Map<Tuple, AggregationFunctionStep[]> aggregationMap = new HashMap<>();

            BlockCursor[] cursors = new BlockCursor[source.getChannelCount()];
            PageIterator iterator = source.iterator(operatorStats);
            while (iterator.hasNext()) {
                Page page = iterator.next();
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
                    AggregationFunctionStep[] functions = aggregationMap.get(key);
                    if (functions == null) {
                        Preconditions.checkState(aggregationMap.size() < maxNumberOfGroups, "Query exceeded max number of aggregation groups %s", maxNumberOfGroups);
                        functions = new AggregationFunctionStep[functionProviders.size()];
                        for (int i = 0; i < functions.length; i++) {
                            functions[i]= functionProviders.get(i).get();
                        }
                        aggregationMap.put(key, functions);
                    }

                    for (AggregationFunctionStep function : functions) {
                        function.add(cursors);
                    }
                }

                for (BlockCursor cursor : cursors) {
                    checkState(!cursor.advanceNextPosition());
                }
            }

            return aggregationMap;
        }
    }
}
