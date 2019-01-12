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
package io.prestosql.type;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import java.util.List;

import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.StructuralTestUtil.arrayBlockOf;

public class TestIntegerArrayType
        extends AbstractTestType
{
    public TestIntegerArrayType()
    {
        super(new TypeRegistry().getType(parseTypeSignature("array(integer)")), List.class, createTestBlock(new TypeRegistry().getType(parseTypeSignature("array(integer)"))));
    }

    public static Block createTestBlock(Type arrayType)
    {
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 4);
        arrayType.writeObject(blockBuilder, arrayBlockOf(INTEGER, 1, 2));
        arrayType.writeObject(blockBuilder, arrayBlockOf(INTEGER, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(INTEGER, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(INTEGER, 100, 200, 300));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Block block = (Block) value;
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            INTEGER.appendTo(block, i, blockBuilder);
        }
        INTEGER.writeLong(blockBuilder, 1L);

        return blockBuilder.build();
    }
}
