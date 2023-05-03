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
package com.facebook.presto.spi.function.aggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.WindowIndex;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;

public interface Accumulator
{
    long getEstimatedSize();

    Type getFinalType();

    Type getIntermediateType();

    void addInput(Page page);

    default void addBlockInput(Page page)
    {
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, "addBlockInput is not implemented");
    }

    void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition);

    void addIntermediate(Block block);

    void evaluateIntermediate(BlockBuilder blockBuilder);

    void evaluateFinal(BlockBuilder blockBuilder);

    default boolean hasAddBlockInput()
    {
        return false;
    }
}
