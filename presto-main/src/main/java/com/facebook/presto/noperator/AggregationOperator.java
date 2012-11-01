package com.facebook.presto.noperator;

import com.facebook.presto.Tuple;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.noperator.aggregation.AggregationFunction;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import javax.inject.Provider;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

public class AggregationOperator
        implements Operator
{
    private final Operator source;
    private final List<Provider<AggregationFunction>> functionProviders;
    private final List<ProjectionFunction> projections;

    public AggregationOperator(Operator source, List<Provider<AggregationFunction>> functionProviders, List<ProjectionFunction> projections)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(functionProviders, "functionProviders is null");
        Preconditions.checkArgument(!functionProviders.isEmpty(), "functionProviders is empty");
        Preconditions.checkNotNull(projections, "projections is null");
        Preconditions.checkArgument(!projections.isEmpty(), "projections is empty");

        this.source = source;
        this.functionProviders = functionProviders;
        this.projections = projections;
    }

    @Override
    public int getChannelCount()
    {
        return projections.size();
    }

    @Override
    public Iterator<Page> iterator()
    {
        // create the aggregation functions
        AggregationFunction[] functions = new AggregationFunction[functionProviders.size()];
        for (int i = 0; i < functions.length; i++) {
            functions[i]= functionProviders.get(i).get();
        }

        // process all rows
        for (Page page : source) {
            Block[] blocks = page.getBlocks();

            int rows = (int) blocks[0].getRange().length();

            BlockCursor[] cursors = new BlockCursor[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                cursors[i] = blocks[i].cursor();
            }

            for (int position = 0; position < rows; position++) {
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceNextPosition());
                }

                for (AggregationFunction function : functions) {
                    function.add(cursors);
                }
            }

            for (BlockCursor cursor : cursors) {
                checkState(!cursor.advanceNextPosition());
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
            BlockBuilder output = new BlockBuilder(0, projections.get(i).getTupleInfo());
            projections.get(i).project(results, output);
            blocks[i] = output.build();
        }

        return Iterators.singletonIterator(new Page(blocks));
    }
}
