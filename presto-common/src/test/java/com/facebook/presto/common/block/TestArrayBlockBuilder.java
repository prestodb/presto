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
package com.facebook.presto.common.block;

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestArrayBlockBuilder
{
    // ArrayBlockBuilder: isNull, offset, 3 * value (FixedWidthBlockBuilder: isNull, value)
    private static final int THREE_INTS_ENTRY_SIZE = Byte.BYTES + Integer.BYTES + 3 * (Byte.BYTES + Long.BYTES);
    private static final int EXPECTED_ENTRY_COUNT = 100;

    @Test
    public void testArrayBlockIsFull()
    {
        testIsFull(new PageBuilderStatus(THREE_INTS_ENTRY_SIZE * EXPECTED_ENTRY_COUNT));
    }

    private void testIsFull(PageBuilderStatus pageBuilderStatus)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, pageBuilderStatus.createBlockBuilderStatus(), EXPECTED_ENTRY_COUNT);
        assertTrue(pageBuilderStatus.isEmpty());
        while (!pageBuilderStatus.isFull()) {
            BlockBuilder elementBuilder = blockBuilder.beginBlockEntry();
            BIGINT.writeLong(elementBuilder, 12);
            elementBuilder.appendNull();
            BIGINT.writeLong(elementBuilder, 34);
            blockBuilder.closeEntry();
        }
        assertEquals(blockBuilder.getPositionCount(), EXPECTED_ENTRY_COUNT);
        assertEquals(pageBuilderStatus.isFull(), true);
    }

    //TODO we should systematically test Block::getRetainedSizeInBytes()
    @Test
    public void testRetainedSizeInBytes()
    {
        int expectedEntries = 1000;
        BlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(BIGINT, null, expectedEntries);
        long initialRetainedSize = arrayBlockBuilder.getRetainedSizeInBytes();
        for (int i = 0; i < expectedEntries; i++) {
            BlockBuilder arrayElementBuilder = arrayBlockBuilder.beginBlockEntry();
            BIGINT.writeLong(arrayElementBuilder, i);
            arrayBlockBuilder.closeEntry();
        }
        assertTrue(arrayBlockBuilder.getRetainedSizeInBytes() >= (expectedEntries * Long.BYTES + ClassLayout.parseClass(LongArrayBlockBuilder.class).instanceSize() + initialRetainedSize));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Expected current entry to be closed but was opened")
    public void testConcurrentWriting()
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, EXPECTED_ENTRY_COUNT);
        BlockBuilder elementBlockWriter = blockBuilder.beginBlockEntry();
        elementBlockWriter.writeLong(45).closeEntry();
        blockBuilder.appendStructure(new LongArrayBlockBuilder(null, 1).writeLong(123).closeEntry().build());
    }
}
