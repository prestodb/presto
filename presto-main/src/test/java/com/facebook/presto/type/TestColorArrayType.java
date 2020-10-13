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

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;

public class TestColorArrayType
        extends AbstractTestType
{
    public TestColorArrayType()
    {
        super(functionAndTypeManager.getType(parseTypeSignature("array(color)")), List.class, createTestBlock(functionAndTypeManager.getType(parseTypeSignature("array(color)"))));
    }

    public static Block createTestBlock(Type arrayType)
    {
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 4);
        arrayType.writeObject(blockBuilder, arrayBlockOf(COLOR, 1, 2));
        arrayType.writeObject(blockBuilder, arrayBlockOf(COLOR, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(COLOR, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(COLOR, 100, 200, 300));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }
}
