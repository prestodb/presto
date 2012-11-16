package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.AggregationFunctionStep;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import javax.inject.Provider;
import java.util.Iterator;
import java.util.List;

public class AggregationOperator
        implements Operator
{
    private final Operator source;
    private final List<Provider<AggregationFunctionStep>> functionProviders;
    private final List<ProjectionFunction> projections;
    private final List<TupleInfo> tupleInfos;

    public AggregationOperator(Operator source, List<Provider<AggregationFunctionStep>> functionProviders, List<ProjectionFunction> projections)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(functionProviders, "functionProviders is null");
        Preconditions.checkArgument(!functionProviders.isEmpty(), "functionProviders is empty");
        Preconditions.checkNotNull(projections, "projections is null");
        Preconditions.checkArgument(!projections.isEmpty(), "projections is empty");

        this.source = source;
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
        // create the aggregation functions
        AggregationFunctionStep[] functions = new AggregationFunctionStep[functionProviders.size()];
        for (int i = 0; i < functions.length; i++) {
            functions[i] = functionProviders.get(i).get();
        }

        // process all rows
        for (Page page : source) {
            for (AggregationFunctionStep function : functions) {
                function.add(page);
            }
        }

        // get result tuples
        Tuple[] results = new Tuple[functions.length];
        for (int i = 0; i < functions.length; i++) {
            results[i] = functions[i].evaluate();
        }

        // project results into output blocks
        Block[] blocks = new Block[projections.size()];
        for (int i = 0; i < blocks.length; i++) {
            BlockBuilder output = new BlockBuilder(projections.get(i).getTupleInfo());
            projections.get(i).project(results, output);
            blocks[i] = output.build();
        }

        return Iterators.singletonIterator(new Page(blocks));
    }
}
