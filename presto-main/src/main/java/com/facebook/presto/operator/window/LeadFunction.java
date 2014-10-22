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

import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.checkCondition;

public class LeadFunction
        implements WindowFunction
{
    public static class BigintLeadFunction
            extends LeadFunction
    {
        public BigintLeadFunction(List<Integer> argumentChannels)
        {
            super(BIGINT, argumentChannels);
        }
    }

    public static class BooleanLeadFunction
            extends LeadFunction
    {
        public BooleanLeadFunction(List<Integer> argumentChannels)
        {
            super(BOOLEAN, argumentChannels);
        }
    }

    public static class DoubleLeadFunction
            extends LeadFunction
    {
        public DoubleLeadFunction(List<Integer> argumentChannels)
        {
            super(DOUBLE, argumentChannels);
        }
    }

    public static class VarcharLeadFunction
            extends LeadFunction
    {
        public VarcharLeadFunction(List<Integer> argumentChannels)
        {
            super(VARCHAR, argumentChannels);
        }
    }

    private final Type type;
    private final int valueChannel;
    private final int offsetChannel;
    private final int defaultChannel;

    private int partitionStartPosition;
    private int currentPosition;
    private int partitionRowCount;
    private PagesIndex pagesIndex;

    protected LeadFunction(Type type, List<Integer> argumentChannels)
    {
        this.type = type;
        this.valueChannel = argumentChannels.get(0);
        if (argumentChannels.size() > 1) {
            this.offsetChannel = argumentChannels.get(1);
        }
        else {
            this.offsetChannel = -1;
        }
        if (argumentChannels.size() > 2) {
            this.defaultChannel = argumentChannels.get(2);
        }
        else {
            this.defaultChannel = -1;
        }
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void reset(int partitionStartPosition, int partitionRowCount, PagesIndex pagesIndex)
    {
        this.partitionStartPosition = partitionStartPosition;
        this.currentPosition = partitionStartPosition;
        this.partitionRowCount = partitionRowCount;
        this.pagesIndex = pagesIndex;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount)
    {
        int offset = offsetChannel < 0 ? 1 : Ints.checkedCast(pagesIndex.getLong(offsetChannel, currentPosition));
        checkCondition(offset >= 0, INVALID_FUNCTION_ARGUMENT, "Offset must be at least 0");

        int valuePosition = currentPosition + offset;

        if (valuePosition < partitionStartPosition + partitionRowCount) {
            pagesIndex.appendTo(valueChannel, valuePosition, output);
        }
        else {
            if (defaultChannel < 0) {
                output.appendNull();
            }
            else {
                pagesIndex.appendTo(defaultChannel, currentPosition, output);
            }
        }

        currentPosition++;
    }
}
