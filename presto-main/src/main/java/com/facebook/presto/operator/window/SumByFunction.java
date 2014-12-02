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
package com.facebook.presto.operator.window;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.collect.Iterables.getOnlyElement;

import java.util.List;

import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

public class SumByFunction implements WindowFunction
{
    public static class DoubleSumByFunction extends SumByFunction
    {
        public DoubleSumByFunction(List<Integer> argumentChannels)
        {
            super(DOUBLE, argumentChannels);
        }
    }

    public static class BigintSumByFunction extends SumByFunction
    {
        public BigintSumByFunction(List<Integer> argumentChannels)
        {
            super(BIGINT, argumentChannels);
        }
    }

    private final Type type;
    private final int argumentChannel;

    private int currentPosition;
    private PagesIndex pagesIndex;
    private long sumValue;

    protected SumByFunction(Type type, List<Integer> argumentChannels)
    {
        this.type = type;
        this.argumentChannel = getOnlyElement(argumentChannels);
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void reset(int partitionStartPosition, int partitionRowCount,
            PagesIndex pagesIndex)
    {
        this.currentPosition = partitionStartPosition;
        this.pagesIndex = pagesIndex;
        this.sumValue = 0L;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup,
            int peerGroupCount)
    {
        Type curType = pagesIndex.getType(argumentChannel);
        long curValue = pagesIndex.getLong(argumentChannel, currentPosition);
        sumValue += curValue;
        curType.writeLong(output, sumValue);
        currentPosition++;
    }
}
