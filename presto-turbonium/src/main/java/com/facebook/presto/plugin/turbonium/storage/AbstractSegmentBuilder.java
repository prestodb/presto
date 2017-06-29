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
package com.facebook.presto.plugin.turbonium.storage;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

abstract class AbstractSegmentBuilder
        implements SegmentBuilder
{
    public static final int DEFAULT_SEGMENT_SIZE = 0x8000;
    private final int channel;
    private final Type type;
    private final int capacity;
    private final boolean disableEncoding;

    AbstractSegmentBuilder(int channel, Type type, boolean disableEncoding)
    {
        this.channel = channel;
        this.type = type;
        this.capacity = DEFAULT_SEGMENT_SIZE;
        this.disableEncoding = disableEncoding;
    }

    public abstract void append(Block block, int position);

    protected boolean getDisableEncoding()
    {
        return disableEncoding;
    }

    @Override
    public boolean isFull()
    {
        return size() == capacity;
    }

    @Override
    public boolean isEmpty()
    {
        return size() == 0;
    }

    @Override
    public int getChannel()
    {
        return channel;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public int capacity()
    {
        return capacity;
    }

    @Override
    public int remaining()
    {
        return capacity - size();
    }
}
