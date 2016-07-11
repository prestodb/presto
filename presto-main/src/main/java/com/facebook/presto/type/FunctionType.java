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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class FunctionType
        extends AbstractType
{
    public static final String NAME = "function";

    private final Type returnType;
    private final List<Type> argumentTypes;

    public FunctionType(List<Type> argumentTypes, Type returnType)
    {
        super(new TypeSignature(NAME, typeParameters(argumentTypes, returnType)), MethodHandle.class);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    private static List<TypeSignatureParameter> typeParameters(List<Type> argumentTypes, Type returnType)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        ImmutableList.Builder<TypeSignatureParameter> builder = ImmutableList.builder();
        argumentTypes.stream()
                .map(Type::getTypeSignature)
                .map(TypeSignatureParameter::of)
                .forEach(builder::add);
        builder.add(TypeSignatureParameter.of(returnType.getTypeSignature()));
        return builder.build();
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
    public Object getObjectValue(ConnectorSession session, Block block, int position)
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

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.<Type>builder().addAll(argumentTypes).add(returnType).build();
    }

    @Override
    public String getDisplayName()
    {
        ImmutableList<String> names = getTypeParameters().stream()
                .map(Type::getDisplayName)
                .collect(toImmutableList());
        return "function<" + Joiner.on(",").join(names) + ">";
    }
}
