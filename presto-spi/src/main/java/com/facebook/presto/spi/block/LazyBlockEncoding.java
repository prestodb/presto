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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class LazyBlockEncoding
        implements BlockEncoding
{
    private final BlockEncoding delegate;

    public LazyBlockEncoding(BlockEncoding delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public String getName()
    {
        return delegate.getName();
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        delegate.writeBlock(sliceOutput, ((LazyBlock) block).getBlock());
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        // We write the actual underlying block, so we will never need to read a lazy block
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return new LazyBlockEncodingFactory(delegate.getFactory());
    }

    public static class LazyBlockEncodingFactory
            implements BlockEncodingFactory<LazyBlockEncoding>
    {
        private final BlockEncodingFactory delegate;

        public LazyBlockEncodingFactory(BlockEncodingFactory delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public String getName()
        {
            return delegate.getName();
        }

        @Override
        public LazyBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, LazyBlockEncoding blockEncoding)
        {
            delegate.writeEncoding(serde, output, blockEncoding.delegate);
        }
    }
}
