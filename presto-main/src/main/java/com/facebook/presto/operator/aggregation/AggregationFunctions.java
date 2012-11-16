/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

import javax.inject.Provider;

public final class AggregationFunctions
{
    private AggregationFunctions()
    {
    }

    public static AggregationFunctionStep singleNodeAggregation(AggregationFunction function)
    {
        return new SingleNodeAggregationAdapter(function);
    }

    public static Provider<AggregationFunctionStep> singleNodeAggregation(Provider<? extends AggregationFunction> provider)
    {
        return new SingleNodeAggregationProvider(provider);
    }

    private static class SingleNodeAggregationProvider
            implements Provider<AggregationFunctionStep>
    {
        private final Provider<? extends AggregationFunction> provider;

        public SingleNodeAggregationProvider(Provider<? extends AggregationFunction> provider)
        {
            this.provider = provider;
        }

        @Override
        public AggregationFunctionStep get()
        {
            return new SingleNodeAggregationAdapter(provider.get());
        }
    }

    private static class SingleNodeAggregationAdapter
            implements AggregationFunctionStep
    {
        private final AggregationFunction aggregationFunction;

        public SingleNodeAggregationAdapter(AggregationFunction aggregationFunction)
        {
            this.aggregationFunction = aggregationFunction;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return aggregationFunction.getFinalTupleInfo();
        }

        @Override
        public void add(Page page)
        {
            aggregationFunction.addInput(page);
        }

        @Override
        public void add(BlockCursor... cursors)
        {
            aggregationFunction.addInput(cursors);
        }

        @Override
        public Tuple evaluate()
        {
            return aggregationFunction.evaluateFinal();
        }
    }

    public static AggregationFunctionStep partialAggregation(AggregationFunction function)
    {
        return new PartialAggregationAdapter(function);
    }

    public static Provider<AggregationFunctionStep> partialAggregation(Provider<? extends AggregationFunction> provider)
    {
        return new PartialAggregationProvider(provider);
    }

    private static class PartialAggregationProvider
            implements Provider<AggregationFunctionStep>
    {
        private final Provider<? extends AggregationFunction> provider;

        public PartialAggregationProvider(Provider<? extends AggregationFunction> provider)
        {
            this.provider = provider;
        }

        @Override
        public AggregationFunctionStep get()
        {
            return new PartialAggregationAdapter(provider.get());
        }
    }

    private static class PartialAggregationAdapter
            implements AggregationFunctionStep
    {
        private final AggregationFunction aggregationFunction;

        public PartialAggregationAdapter(AggregationFunction aggregationFunction)
        {
            this.aggregationFunction = aggregationFunction;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return aggregationFunction.getIntermediateTupleInfo();
        }

        @Override
        public void add(Page page)
        {
            aggregationFunction.addInput(page);
        }

        @Override
        public void add(BlockCursor... cursors)
        {
            aggregationFunction.addInput(cursors);
        }

        @Override
        public Tuple evaluate()
        {
            return aggregationFunction.evaluateIntermediate();
        }
    }

    public static AggregationFunctionStep combinerAggregation(AggregationFunction function)
    {
        return new CombinerAggregationAdapter(function);
    }

    public static Provider<AggregationFunctionStep> combinerAggregation(Provider<? extends AggregationFunction> provider)
    {
        return new CombinerAggregationProvider(provider);
    }

    private static class CombinerAggregationProvider
            implements Provider<AggregationFunctionStep>
    {
        private final Provider<? extends AggregationFunction> provider;

        public CombinerAggregationProvider(Provider<? extends AggregationFunction> provider)
        {
            this.provider = provider;
        }

        @Override
        public AggregationFunctionStep get()
        {
            return new CombinerAggregationAdapter(provider.get());
        }
    }

    private static class CombinerAggregationAdapter
            implements AggregationFunctionStep
    {
        private final AggregationFunction aggregationFunction;

        public CombinerAggregationAdapter(AggregationFunction aggregationFunction)
        {
            this.aggregationFunction = aggregationFunction;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return aggregationFunction.getIntermediateTupleInfo();
        }

        @Override
        public void add(Page page)
        {
            aggregationFunction.addIntermediate(page);
        }

        @Override
        public void add(BlockCursor... cursors)
        {
            aggregationFunction.addIntermediate(cursors);
        }

        @Override
        public Tuple evaluate()
        {
            return aggregationFunction.evaluateIntermediate();
        }
    }

    public static AggregationFunctionStep finalAggregation(AggregationFunction function)
    {
        return new FinalAggregationAdapter(function);
    }

    public static Provider<AggregationFunctionStep> finalAggregation(Provider<? extends AggregationFunction> provider)
    {
        return new FinalAggregationProvider(provider);
    }

    private static class FinalAggregationProvider
            implements Provider<AggregationFunctionStep>
    {
        private final Provider<? extends AggregationFunction> provider;

        public FinalAggregationProvider(Provider<? extends AggregationFunction> provider)
        {
            this.provider = provider;
        }

        @Override
        public AggregationFunctionStep get()
        {
            return new FinalAggregationAdapter(provider.get());
        }
    }

    private static class FinalAggregationAdapter
            implements AggregationFunctionStep
    {
        private final AggregationFunction aggregationFunction;

        public FinalAggregationAdapter(AggregationFunction aggregationFunction)
        {
            this.aggregationFunction = aggregationFunction;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return aggregationFunction.getFinalTupleInfo();
        }

        @Override
        public void add(Page page)
        {
            aggregationFunction.addIntermediate(page);
        }

        @Override
        public void add(BlockCursor... cursors)
        {
            aggregationFunction.addIntermediate(cursors);
        }

        @Override
        public Tuple evaluate()
        {
            return aggregationFunction.evaluateFinal();
        }
    }
}
