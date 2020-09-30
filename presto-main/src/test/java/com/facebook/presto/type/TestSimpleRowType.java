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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.SingleRowBlockWriter;
import com.facebook.presto.common.type.Type;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static io.airlift.slice.Slices.utf8Slice;

public class TestSimpleRowType
        extends AbstractTestType
{
    private static final Type TYPE = createTestFunctionAndTypeManager().getType(parseTypeSignature("row(a bigint,b varchar)"));

    public TestSimpleRowType()
    {
        super(TYPE, List.class, createTestBlock());
    }

    private static Block createTestBlock()
    {
        RowBlockBuilder blockBuilder = (RowBlockBuilder) TYPE.createBlockBuilder(null, 3);

        SingleRowBlockWriter singleRowBlockWriter;

        singleRowBlockWriter = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(singleRowBlockWriter, 1);
        VARCHAR.writeSlice(singleRowBlockWriter, utf8Slice("cat"));
        blockBuilder.closeEntry();

        singleRowBlockWriter = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(singleRowBlockWriter, 2);
        VARCHAR.writeSlice(singleRowBlockWriter, utf8Slice("cats"));
        blockBuilder.closeEntry();

        singleRowBlockWriter = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(singleRowBlockWriter, 3);
        VARCHAR.writeSlice(singleRowBlockWriter, utf8Slice("dog"));
        blockBuilder.closeEntry();

        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        RowBlockBuilder blockBuilder = (RowBlockBuilder) TYPE.createBlockBuilder(null, 1);
        SingleRowBlockWriter singleRowBlockWriter;

        Block block = (Block) value;
        singleRowBlockWriter = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(singleRowBlockWriter, block.getSingleValueBlock(0).getLong(0) + 1);
        VARCHAR.writeSlice(singleRowBlockWriter, block.getSingleValueBlock(1).getSlice(0, 0, 1));
        blockBuilder.closeEntry();

        return TYPE.getObject(blockBuilder.build(), 0);
    }
}
