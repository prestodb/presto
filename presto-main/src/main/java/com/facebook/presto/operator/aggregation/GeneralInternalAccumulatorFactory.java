/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GeneralInternalAccumulatorFactory
        implements InternalAccumulatorFactory
{
    private final AccumulatorFactory accumulatorFactory;
    private final Type intermediateType;
    private final Type finalType;

    public GeneralInternalAccumulatorFactory(AccumulatorFactory accumulatorFactory)
    {
        this.accumulatorFactory = requireNonNull(accumulatorFactory, "accumulatorFactory is null");
        Accumulator accumulator = accumulatorFactory.createAccumulator();
        this.intermediateType = accumulator.getIntermediateType();
        this.finalType = accumulator.getFinalType();
    }

    @Override
    public Type getIntermediateType()
    {
        return intermediateType;
    }

    @Override
    public Type getFinalType()
    {
        return finalType;
    }

    @Override
    public boolean isDistinct()
    {
        return false;
    }

    @Override
    public InternalSingleAccumulator createSingleAccumulator()
    {
        return new GeneralInternalAccumulator(accumulatorFactory.createAccumulator());
    }

    @Override
    public InternalPartialAccumulator createPartialAccumulator()
    {
        return new GeneralInternalIntermediateAccumulator(accumulatorFactory.createAccumulator());
    }

    @Override
    public InternalIntermediateAccumulator createIntermediateAccumulator()
    {
        return new GeneralInternalIntermediateAccumulator(accumulatorFactory.createIntermediateAccumulator());
    }

    @Override
    public InternalFinalAccumulator createFinalAccumulator()
    {
        return new GeneralInternalAccumulator(accumulatorFactory.createIntermediateAccumulator());
    }

    public static class GeneralInternalAccumulator
            implements InternalSingleAccumulator, InternalFinalAccumulator
    {
        private final Accumulator accumulator;

        public GeneralInternalAccumulator(Accumulator accumulator)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
        }

        @Override
        public long getEstimatedSize()
        {
            return accumulator.getEstimatedSize();
        }

        @Override
        public Type getIntermediateType()
        {
            return accumulator.getIntermediateType();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public boolean isDistinct()
        {
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            accumulator.addInput(page);
        }

        @Override
        public void addIntermediate(Block block)
        {
            accumulator.addIntermediate(block);
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    public static class GeneralInternalIntermediateAccumulator
            implements InternalPartialAccumulator, InternalIntermediateAccumulator
    {
        private final Accumulator accumulator;

        public GeneralInternalIntermediateAccumulator(Accumulator accumulator)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
        }

        @Override
        public long getEstimatedSize()
        {
            return accumulator.getEstimatedSize();
        }

        @Override
        public Type getIntermediateType()
        {
            return accumulator.getIntermediateType();
        }

        @Override
        public boolean isDistinct()
        {
            return false;
        }

        @Override
        public Optional<Block> addInput(Page page)
        {
            accumulator.addInput(page);
            return Optional.empty();
        }

        @Override
        public void addIntermediate(Block block)
        {
            accumulator.addIntermediate(block);
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            accumulator.evaluateIntermediate(blockBuilder);
        }
    }
}
