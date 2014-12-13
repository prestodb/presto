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
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;

public class TestBigintArrayType
        extends AbstractTestType
{
    public TestBigintArrayType()
    {
        super(new TypeRegistry().getType(parseTypeSignature("array<bigint>")), List.class, createTestBlock(new TypeRegistry().getType(parseTypeSignature("array<bigint>"))));
    }

    public static Block createTestBlock(Type arrayType)
    {
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(new BlockBuilderStatus());
        arrayType.writeSlice(blockBuilder, toStackRepresentation(ImmutableList.of(1, 2), BIGINT));
        arrayType.writeSlice(blockBuilder, toStackRepresentation(ImmutableList.of(1, 2, 3), BIGINT));
        arrayType.writeSlice(blockBuilder, toStackRepresentation(ImmutableList.of(1, 2, 3), BIGINT));
        arrayType.writeSlice(blockBuilder, toStackRepresentation(ImmutableList.of(100, 200, 300), BIGINT));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Block block = readStructuralBlock(((Slice) value));
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), block.getSizeInBytes() + 8);
        for (int i = 0; i < block.getPositionCount(); i++) {
            BIGINT.appendTo(block, i, blockBuilder);
        }
        BIGINT.writeLong(blockBuilder, 1L);

        return buildStructuralSlice(blockBuilder);
    }
}
