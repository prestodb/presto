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
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class LastValueFunction
        implements WindowFunction
{
    public static class BigintLastValueFunction
            extends LastValueFunction
    {
        public BigintLastValueFunction()
        {
            super(BIGINT);
        }
    }
    public static class BooleanLastValueFunction
            extends LastValueFunction
    {
        public BooleanLastValueFunction()
        {
            super(BOOLEAN);
        }
    }
    public static class DoubleLastValueFunction
            extends LastValueFunction
    {
        public DoubleLastValueFunction()
        {
            super(DOUBLE);
        }
    }
    public static class VarcharLastValueFunction
            extends LastValueFunction
    {
        public VarcharLastValueFunction()
        {
            super(VARCHAR);
        }
    }

    private final Type type;
    private int rowCount;
    protected RandomAccessBlock valueBlock;

    protected LastValueFunction(Type type)
    {
        this.type = type;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void reset(int partitionRowCount, PagesIndex pagesIndex, int... argumentChannels)
    {
        rowCount += partitionRowCount;
        valueBlock = pagesIndex.getSingleValueBlock(argumentChannels[0], rowCount - 1);
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount)
    {
        valueBlock.appendTo(0, output);
    }
}
