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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.UncheckedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TypeWithName
        implements Type
{
    private final QualifiedObjectName name;
    private final Type type;
    private final TypeSignature typeSignature;

    public TypeWithName(QualifiedObjectName name, Type type)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.typeSignature = new TypeSignature(new UserDefinedType(name, type.getTypeSignature()));
    }

    @Override
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    @Override
    public String getDisplayName()
    {
        return name.toString();
    }

    public QualifiedObjectName getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return typeSignature.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TypeWithName other = (TypeWithName) obj;

        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    // All aspect related to execution are delegated to type
    @Override
    public boolean isComparable()
    {
        return type.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return type.isOrderable();
    }

    @Override
    public Class<?> getJavaType()
    {
        return type.getJavaType();
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return type.getTypeParameters();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return type.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return type.createBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        return type.getObjectValue(properties, block, position);
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        return type.getBoolean(block, position);
    }

    @Override
    public boolean getBooleanUnchecked(UncheckedBlock block, int internalPosition)
    {
        return type.getBooleanUnchecked(block, internalPosition);
    }

    @Override
    public byte getByte(Block block, int position)
    {
        return type.getByte(block, position);
    }

    @Override
    public long getLong(Block block, int position)
    {
        return type.getLong(block, position);
    }

    @Override
    public long getLongUnchecked(UncheckedBlock block, int internalPosition)
    {
        return type.getLongUnchecked(block, internalPosition);
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return type.getDouble(block, position);
    }

    @Override
    public double getDoubleUnchecked(UncheckedBlock block, int internalPosition)
    {
        return type.getDoubleUnchecked(block, internalPosition);
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return type.getSlice(block, position);
    }

    @Override
    public Slice getSliceUnchecked(Block block, int internalPosition)
    {
        return type.getSliceUnchecked(block, internalPosition);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return type.getObject(block, position);
    }

    @Override
    public Block getBlockUnchecked(Block block, int internalPosition)
    {
        return type.getBlockUnchecked(block, internalPosition);
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        type.writeBoolean(blockBuilder, value);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        type.writeLong(blockBuilder, value);
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        type.writeDouble(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        type.writeSlice(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        type.writeSlice(blockBuilder, value, offset, length);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        type.writeObject(blockBuilder, value);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        type.appendTo(block, position, blockBuilder);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public long hash(Block block, int position)
    {
        return type.hash(block, position);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return type.compareTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }
}
