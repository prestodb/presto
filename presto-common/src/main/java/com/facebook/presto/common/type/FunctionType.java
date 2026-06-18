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
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.UncheckedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class FunctionType
        implements Type
{
    public static final String NAME = "function";

    private final TypeSignature signature;
    private final Type returnType;
    private final List<Type> argumentTypes;

    public FunctionType(List<Type> argumentTypes, Type returnType)
    {
        this.signature = new TypeSignature(NAME, typeParameters(argumentTypes, returnType));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = unmodifiableList(new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null")));
    }

    private static List<TypeSignatureParameter> typeParameters(List<Type> argumentTypes, Type returnType)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        List<TypeSignatureParameter> parameters = new ArrayList<>(argumentTypes.size() + 1);
        argumentTypes.stream()
                .map(Type::getTypeSignature)
                .map(TypeSignatureParameter::of)
                .forEach(parameters::add);
        parameters.add(TypeSignatureParameter.of(returnType.getTypeSignature()));
        return unmodifiableList(parameters);
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    @Override
    public List<Type> getTypeParameters()
    {
        List<Type> parameters = new ArrayList<>(argumentTypes.size() + 1);
        parameters.addAll(argumentTypes);
        parameters.add(returnType);
        return unmodifiableList(parameters);
    }

    @Override
    public final TypeSignature getTypeSignature()
    {
        return signature;
    }

    @Override
    public String getDisplayName()
    {
        List<String> names = getTypeParameters().stream()
                .map(Type::getDisplayName)
                .collect(toList());
        return "function<" + String.join(",", names) + ">";
    }

    @Override
    public final Class<?> getJavaType()
    {
        throw new UnsupportedOperationException(getTypeSignature() + " type does not have Java type");
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
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        throw new UnsupportedOperationException();
    }
}
