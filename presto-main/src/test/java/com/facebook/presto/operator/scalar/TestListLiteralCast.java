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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.operator.scalar.ListLiteralCast.castArrayOfArraysToListLiteral;
import static com.facebook.presto.operator.scalar.ListLiteralCast.castArrayToListLiteral;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;

public class TestListLiteralCast
{
    @Test
    public void testArrayToListCast()
    {
        List<Integer> expectedValues = ImmutableList.of(1, 2, 3, 4);

        assertEquals(expectedValues, castArrayToListLiteral(buildIntArrayBlockWithIntValues(expectedValues)));
    }

    @Test
    public void testArrayOfArraysToListLiteral()
    {
        List<List<Integer>> expectedValues = ImmutableList.of(
                ImmutableList.of(-1, 726, -44, 0),
                ImmutableList.of(4),
                ImmutableList.of(7911, 30076, 432, 111));

        Type integerArrayType = new TypeRegistry().getType(parseTypeSignature("array(integer)"));
        BlockBuilder blockBuilder = integerArrayType.createBlockBuilder(new BlockBuilderStatus(), 3);
        for (List<Integer> values : expectedValues) {
            blockBuilder.writeObject(buildIntArrayBlockWithIntValues(values)).closeEntry();
        }

        assertEquals(expectedValues, castArrayOfArraysToListLiteral(blockBuilder.build()));
    }

    private static Block buildIntArrayBlockWithIntValues(List<Integer> values)
    {
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(new BlockBuilderStatus(), values.size());
        for (Integer value : values) {
            INTEGER.writeLong(blockBuilder, value);
        }

        return blockBuilder.build();
    }
}
