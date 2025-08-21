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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.UncheckedBlock;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractType
        implements Type
{
    private final Class<?> javaType;

    protected AbstractType(Class<?> javaType)
    {
        this.javaType = javaType;
    }

    @Override
    public final Class<?> getJavaType()
    {
        return javaType;
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return Collections.unmodifiableList(new ArrayList<>());
    }

    @Override
    public boolean isComparable()
    {
        return false;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public long hash(Block block, int position)
    {
        throw new UnsupportedOperationException(getTypeSignature() + " type is not comparable");
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException(getTypeSignature() + " type is not comparable");
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException(getTypeSignature() + " type is not orderable");
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean getBooleanUnchecked(UncheckedBlock block, int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public byte getByte(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public long getLong(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public long getLongUnchecked(UncheckedBlock block, int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public double getDouble(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public double getDoubleUnchecked(UncheckedBlock block, int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Slice getSliceUnchecked(Block block, int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Object getObject(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Block getBlockUnchecked(Block block, int internalPosition)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public String toString()
    {
        return getTypeSignature().toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return this.getTypeSignature().equals(((Type) o).getTypeSignature());
    }

    @Override
    public int hashCode()
    {
        return getTypeSignature().hashCode();
    }

    @Override
    public String getDisplayName()
    {
        return getTypeSignature().toString();
    }
}
