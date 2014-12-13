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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.appendToBlockBuilder;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;

public class TestSimpleRowType
        extends AbstractTestType
{
    private static final Type TYPE = new TypeRegistry().getType(parseTypeSignature("row<bigint,varchar>('a','b')"));

    public TestSimpleRowType()
    {
        super(TYPE, List.class, createTestBlock());
    }

    private static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TYPE.createBlockBuilder(new BlockBuilderStatus());
        List<Type> parameterTypes = ImmutableList.of(BIGINT, VARCHAR);
        TYPE.writeSlice(blockBuilder, sliceOf(parameterTypes, 1, "cat"));
        TYPE.writeSlice(blockBuilder, sliceOf(parameterTypes, 2, "cats"));
        TYPE.writeSlice(blockBuilder, sliceOf(parameterTypes, 3, "dog"));
        return blockBuilder.build();
    }

    private static Slice sliceOf(List<Type> parameterTypes, Object... values)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        for (int i = 0; i < values.length; i++) {
            appendToBlockBuilder(parameterTypes.get(i), values[i], blockBuilder);
        }
        return buildStructuralSlice(blockBuilder);
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }
}
