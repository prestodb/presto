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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.FilterAndProjectOperator.FilterAndProjectOperatorFactory;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DynamicTupleFilterFactory
{
    private final int filterOperatorId;
    private final int[] tupleFilterChannels;
    private final int[] outputFilterChannels;
    private final List<Type> outputTypes;

    public DynamicTupleFilterFactory(int filterOperatorId, int[] tupleFilterChannels, int[] outputFilterChannels, List<Type> outputTypes)
    {
        requireNonNull(tupleFilterChannels, "tupleFilterChannels is null");
        checkArgument(tupleFilterChannels.length > 0, "Must have at least one tupleFilterChannel");
        requireNonNull(outputFilterChannels, "outputFilterChannels is null");
        checkArgument(outputFilterChannels.length == tupleFilterChannels.length, "outputFilterChannels must have same length as tupleFilterChannels");
        requireNonNull(outputTypes, "outputTypes is null");
        checkArgument(outputTypes.size() >= outputFilterChannels.length, "Must have at least as many output channels as those used for filtering");

        this.filterOperatorId = filterOperatorId;
        this.tupleFilterChannels = tupleFilterChannels.clone();
        this.outputFilterChannels = outputFilterChannels.clone();
        this.outputTypes = ImmutableList.copyOf(outputTypes);
    }

    public OperatorFactory filterWithTuple(Page tuplePage)
    {
        Page normalizedTuplePage = normalizeTuplePage(tuplePage);
        TupleFilterProcessor processor = new TupleFilterProcessor(normalizedTuplePage, outputTypes, outputFilterChannels);
        return new FilterAndProjectOperatorFactory(filterOperatorId, processor, outputTypes);
    }

    private Page normalizeTuplePage(Page tuplePage)
    {
        Block[] normalizedBlocks = new Block[tupleFilterChannels.length];
        for (int i = 0; i < tupleFilterChannels.length; i++) {
            normalizedBlocks[i] = tuplePage.getBlock(tupleFilterChannels[i]);
        }
        return new Page(normalizedBlocks);
    }
}
