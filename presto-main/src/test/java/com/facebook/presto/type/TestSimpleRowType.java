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

import com.facebook.presto.spi.block.ArrayBlockBuilder;
import com.facebook.presto.spi.block.ArrayElementBlockWriter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;

public class TestSimpleRowType
        extends AbstractTestType
{
    private static final Type TYPE = new TypeRegistry().getType(parseTypeSignature("row(a bigint,b varchar)"));

    public TestSimpleRowType()
    {
        super(TYPE, List.class, createTestBlock());
    }

    private static Block createTestBlock()
    {
        ArrayBlockBuilder blockBuilder = (ArrayBlockBuilder) TYPE.createBlockBuilder(new BlockBuilderStatus(), 3);

        ArrayElementBlockWriter arrayElementBlockWriter;

        arrayElementBlockWriter = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(arrayElementBlockWriter, 1);
        VARCHAR.writeSlice(arrayElementBlockWriter, utf8Slice("cat"));
        blockBuilder.closeEntry();

        arrayElementBlockWriter = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(arrayElementBlockWriter, 2);
        VARCHAR.writeSlice(arrayElementBlockWriter, utf8Slice("cats"));
        blockBuilder.closeEntry();

        arrayElementBlockWriter = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(arrayElementBlockWriter, 3);
        VARCHAR.writeSlice(arrayElementBlockWriter, utf8Slice("dog"));
        blockBuilder.closeEntry();

        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        ArrayBlockBuilder blockBuilder = (ArrayBlockBuilder) TYPE.createBlockBuilder(new BlockBuilderStatus(), 1);
        ArrayElementBlockWriter arrayElementBlockWriter;

        Block block = (Block) value;
        arrayElementBlockWriter = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(arrayElementBlockWriter, block.getSingleValueBlock(0).getLong(0, 0) + 1);
        VARCHAR.writeSlice(arrayElementBlockWriter, block.getSingleValueBlock(1).getSlice(0, 0, 1));
        blockBuilder.closeEntry();

        return TYPE.getObject(blockBuilder.build(), 0);
    }
}
