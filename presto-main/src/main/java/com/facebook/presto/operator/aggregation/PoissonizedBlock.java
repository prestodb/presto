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

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import org.apache.commons.math3.random.RandomDataGenerator;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

class PoissonizedBlock
        implements Block
{
    private final Block delegate;
    private final long seed;

    public PoissonizedBlock(Block delegate, long seed)
    {
        this.delegate = delegate;
        this.seed = seed;
    }

    @Override
    public Type getType()
    {
        return BIGINT;
    }

    @Override
    public int getPositionCount()
    {
        return delegate.getPositionCount();
    }

    @Override
    public int getSizeInBytes()
    {
        return delegate.getSizeInBytes();
    }

    @Override
    public BlockCursor cursor()
    {
        return new PoissonizedBlockCursor(delegate.cursor(), seed);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        throw new UnsupportedOperationException("Poissonized blocks cannot be serialized");
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException("getRegion for poissonized block is not supported");
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        throw new UnsupportedOperationException("Random access to poissonized block is not supported");
    }

    public static class PoissonizedBlockCursor
            implements BlockCursor
    {
        private final RandomDataGenerator rand = new RandomDataGenerator();
        private final BlockCursor delegate;
        private long currentValue;

        private PoissonizedBlockCursor(BlockCursor delegate, long seed)
        {
            checkArgument(delegate.getType().equals(BIGINT), "delegate must be a cursor of longs");
            this.delegate = delegate;
            rand.reSeed(seed);
        }

        @Override
        public BlockCursor duplicate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type getType()
        {
            return BIGINT;
        }

        @Override
        public int getRemainingPositions()
        {
            return delegate.getRemainingPositions();
        }

        @Override
        public boolean isValid()
        {
            return delegate.isValid();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public boolean advanceNextPosition()
        {
            boolean advanced = delegate.advanceNextPosition();
            if (advanced) {
                currentValue = rand.nextPoisson(delegate.getLong());
            }
            return advanced;
        }

        @Override
        public boolean advanceToPosition(int position)
        {
            // We can't just delegate this method, because for this to be consistent with calling advanceNextPosition() position times, we need to advance the random number generator
            boolean advanced = false;
            while (getPosition() < position) {
                advanced = advanceNextPosition();
                if (!advanced) {
                    break;
                }
            }
            return advanced;
        }

        @Override
        public Block getRegionAndAdvance(int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RandomAccessBlock getSingleValueBlock()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getObjectValue(Session session)
        {
            return currentValue;
        }

        @Override
        public boolean getBoolean()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong()
        {
            return currentValue;
        }

        @Override
        public double getDouble()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice getSlice()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull()
        {
            checkState(!delegate.isNull(), "delegate to poissonized cursor returned a null row");
            return false;
        }

        @Override
        public int compareTo(Slice slice, int offset)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPosition()
        {
            return delegate.getPosition();
        }

        @Override
        public int calculateHashCode()
        {
            return Longs.hashCode(currentValue);
        }

        @Override
        public void appendTo(BlockBuilder blockBuilder)
        {
            blockBuilder.append(currentValue);
        }
    }
}
