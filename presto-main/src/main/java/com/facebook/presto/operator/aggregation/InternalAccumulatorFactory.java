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

public interface InternalAccumulatorFactory
{
    Type getIntermediateType();

    Type getFinalType();

    boolean isDistinct();

    InternalSingleAccumulator createSingleAccumulator();

    InternalPartialAccumulator createPartialAccumulator();

    InternalIntermediateAccumulator createIntermediateAccumulator();

    InternalFinalAccumulator createFinalAccumulator();

    interface InternalSingleAccumulator
    {
        long getEstimatedSize();

        Type getFinalType();

        boolean isDistinct();

        void addInput(Page page);

        void evaluateFinal(BlockBuilder blockBuilder);
    }

    interface InternalPartialAccumulator
    {
        long getEstimatedSize();

        Type getIntermediateType();

        boolean isDistinct();

        Optional<Block> addInput(Page page);

        void evaluateIntermediate(BlockBuilder blockBuilder);
    }

    interface InternalIntermediateAccumulator
    {
        long getEstimatedSize();

        Type getIntermediateType();

        boolean isDistinct();

        Optional<Block> addInput(Page page);

        void addIntermediate(Block block);

        void evaluateIntermediate(BlockBuilder blockBuilder);
    }

    interface InternalFinalAccumulator
    {
        long getEstimatedSize();

        Type getIntermediateType();

        Type getFinalType();

        boolean isDistinct();

        void addInput(Page page);

        void addIntermediate(Block block);

        void evaluateFinal(BlockBuilder blockBuilder);
    }
}
