/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

import javax.inject.Provider;

public final class AggregationFunctions
{
    private AggregationFunctions()
    {
    }

    public static AggregationFunction singleNodeAggregation(FullAggregationFunction function)
    {
        return new SingleNodeAggregationAdapter(function);
    }

    public static Provider<AggregationFunction> singleNodeAggregation(Provider<? extends FullAggregationFunction> provider)
    {
        return new SingleNodeAggregationProvider(provider);
    }

    private static class SingleNodeAggregationProvider
            implements Provider<AggregationFunction>
    {
        private final Provider<? extends FullAggregationFunction> provider;

        public SingleNodeAggregationProvider(Provider<? extends FullAggregationFunction> provider)
        {
            this.provider = provider;
        }

        @Override
        public AggregationFunction get()
        {
            return new SingleNodeAggregationAdapter(provider.get());
        }
    }

    private static class SingleNodeAggregationAdapter
            implements AggregationFunction
    {
        private final FullAggregationFunction fullAggregationFunction;

        public SingleNodeAggregationAdapter(FullAggregationFunction fullAggregationFunction)
        {
            this.fullAggregationFunction = fullAggregationFunction;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return fullAggregationFunction.getFinalTupleInfo();
        }

        @Override
        public void add(BlockCursor... cursors)
        {
            fullAggregationFunction.addInput(cursors);
        }

        @Override
        public Tuple evaluate()
        {
            return fullAggregationFunction.evaluateFinal();
        }
    }

    public static AggregationFunction partialAggregation(FullAggregationFunction function)
    {
        return new PartialAggregationAdapter(function);
    }

    public static Provider<AggregationFunction> partialAggregation(Provider<? extends FullAggregationFunction> provider)
    {
        return new PartialAggregationProvider(provider);
    }

    private static class PartialAggregationProvider
            implements Provider<AggregationFunction>
    {
        private final Provider<? extends FullAggregationFunction> provider;

        public PartialAggregationProvider(Provider<? extends FullAggregationFunction> provider)
        {
            this.provider = provider;
        }

        @Override
        public AggregationFunction get()
        {
            return new PartialAggregationAdapter(provider.get());
        }
    }

    private static class PartialAggregationAdapter
            implements AggregationFunction
    {
        private final FullAggregationFunction fullAggregationFunction;

        public PartialAggregationAdapter(FullAggregationFunction fullAggregationFunction)
        {
            this.fullAggregationFunction = fullAggregationFunction;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return fullAggregationFunction.getIntermediateTupleInfo();
        }

        @Override
        public void add(BlockCursor... cursors)
        {
            fullAggregationFunction.addInput(cursors);
        }

        @Override
        public Tuple evaluate()
        {
            return fullAggregationFunction.evaluateIntermediate();
        }
    }

    public static AggregationFunction combinerAggregation(FullAggregationFunction function)
    {
        return new CombinerAggregationAdapter(function);
    }

    public static Provider<AggregationFunction> combinerAggregation(Provider<? extends FullAggregationFunction> provider)
    {
        return new CombinerAggregationProvider(provider);
    }

    private static class CombinerAggregationProvider
            implements Provider<AggregationFunction>
    {
        private final Provider<? extends FullAggregationFunction> provider;

        public CombinerAggregationProvider(Provider<? extends FullAggregationFunction> provider)
        {
            this.provider = provider;
        }

        @Override
        public AggregationFunction get()
        {
            return new CombinerAggregationAdapter(provider.get());
        }
    }

    private static class CombinerAggregationAdapter
            implements AggregationFunction
    {
        private final FullAggregationFunction fullAggregationFunction;

        public CombinerAggregationAdapter(FullAggregationFunction fullAggregationFunction)
        {
            this.fullAggregationFunction = fullAggregationFunction;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return fullAggregationFunction.getIntermediateTupleInfo();
        }

        @Override
        public void add(BlockCursor... cursors)
        {
            fullAggregationFunction.addIntermediate(cursors);
        }

        @Override
        public Tuple evaluate()
        {
            return fullAggregationFunction.evaluateIntermediate();
        }
    }

    public static AggregationFunction finalAggregation(FullAggregationFunction function)
    {
        return new FinalAggregationAdapter(function);
    }

    public static Provider<AggregationFunction> finalAggregation(Provider<? extends FullAggregationFunction> provider)
    {
        return new FinalAggregationProvider(provider);
    }

    private static class FinalAggregationProvider
            implements Provider<AggregationFunction>
    {
        private final Provider<? extends FullAggregationFunction> provider;

        public FinalAggregationProvider(Provider<? extends FullAggregationFunction> provider)
        {
            this.provider = provider;
        }

        @Override
        public AggregationFunction get()
        {
            return new FinalAggregationAdapter(provider.get());
        }
    }

    private static class FinalAggregationAdapter
            implements AggregationFunction
    {
        private final FullAggregationFunction fullAggregationFunction;

        public FinalAggregationAdapter(FullAggregationFunction fullAggregationFunction)
        {
            this.fullAggregationFunction = fullAggregationFunction;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return fullAggregationFunction.getFinalTupleInfo();
        }

        @Override
        public void add(BlockCursor... cursors)
        {
            fullAggregationFunction.addIntermediate(cursors);
        }

        @Override
        public Tuple evaluate()
        {
            return fullAggregationFunction.evaluateFinal();
        }
    }
}
