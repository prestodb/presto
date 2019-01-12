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

import static io.prestosql.operator.scalar.ColorFunctions.rgb;
import static io.prestosql.type.ColorType.COLOR;

public class TestColorType
        extends AbstractTestType
{
    public TestColorType()
    {
        super(COLOR, String.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = COLOR.createBlockBuilder(null, 15);
        COLOR.writeLong(blockBuilder, rgb(1, 1, 1));
        COLOR.writeLong(blockBuilder, rgb(1, 1, 1));
        COLOR.writeLong(blockBuilder, rgb(1, 1, 1));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(3, 3, 3));
        COLOR.writeLong(blockBuilder, rgb(3, 3, 3));
        COLOR.writeLong(blockBuilder, rgb(4, 4, 4));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }
}
