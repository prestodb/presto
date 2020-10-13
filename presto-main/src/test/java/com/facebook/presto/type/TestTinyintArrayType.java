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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;

import java.util.List;

import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;

public class TestTinyintArrayType
        extends AbstractTestType
{
    public TestTinyintArrayType()
    {
        super(functionAndTypeManager.getType(parseTypeSignature("array(tinyint)")), List.class, createTestBlock(functionAndTypeManager.getType(parseTypeSignature("array(tinyint)"))));
    }

    public static Block createTestBlock(Type arrayType)
    {
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 4);
        arrayType.writeObject(blockBuilder, arrayBlockOf(TINYINT, 1, 2));
        arrayType.writeObject(blockBuilder, arrayBlockOf(TINYINT, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(TINYINT, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(TINYINT, 100, 110, 127));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Block block = (Block) value;
        BlockBuilder blockBuilder = TINYINT.createBlockBuilder(null, block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            TINYINT.appendTo(block, i, blockBuilder);
        }
        TINYINT.writeLong(blockBuilder, 1L);

        return blockBuilder.build();
    }
}
