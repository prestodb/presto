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
package com.facebook.presto.block;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.MappedBlock;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

public class TestMappedBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] underlyingValues = createExpectedValues(100);
        assertValues(underlyingValues);
        assertValues((Slice[]) alternatingNullValues(underlyingValues));
    }

    private void assertValues(Slice[] underlyingValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(underlyingValues);
        Block block = blockBuilder.build();

        int[] mapping = new int[underlyingValues.length / 2];
        Slice[] expectedValues = new Slice[mapping.length];

        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = i * 2;
            expectedValues[i] = underlyingValues[i * 2];
        }

        Block maskedBlock = new MappedBlock(block, mapping);
        assertBlock(maskedBlock, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), expectedValues.length, 32);
        for (Slice expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
            }
        }
        return blockBuilder;
    }
}
