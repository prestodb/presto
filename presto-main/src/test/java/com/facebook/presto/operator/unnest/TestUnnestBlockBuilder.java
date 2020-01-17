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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.PageBuilderStatus;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.block.ColumnarTestUtils.assertBlock;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.createSimpleBlock;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.toSlices;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestUnnestBlockBuilder
{
    @Test
    public void testWithNullElements()
    {
        testUnnestBlockBuilderMethods(new String[]{"a", "b", "c", null, "d", "e"});
    }

    @Test
    public void testWithoutNullElements()
    {
        testUnnestBlockBuilderMethods(new String[]{"a", "b", "c", "d", "e"});
    }

    @Test
    public void testCapacityIncrease()
    {
        Slice[] values = new Slice[100];
        for (int i = 0; i < values.length; i++) {
            values[i] = utf8Slice("a");
        }

        UnnestBlockBuilder unnestBlockBuilder = new UnnestBlockBuilder(VARCHAR);
        Block valuesBlock = createSimpleBlock(values);
        unnestBlockBuilder.resetInputBlock(valuesBlock);

        unnestBlockBuilder.startNewOutput(new PageBuilderStatus(), 20);
        unnestBlockBuilder.appendRange(0, values.length);
        assertBlock(unnestBlockBuilder.buildOutputAndFlush(), values);

        unnestBlockBuilder.clearCurrentOutput();
    }

    @Test
    public void testEmptyOutput()
    {
        Slice[] values = toSlices(new String[]{"a", "b"});
        UnnestBlockBuilder unnestBlockBuilder = new UnnestBlockBuilder(VARCHAR);
        Block valuesBlock = createSimpleBlock(values);
        unnestBlockBuilder.resetInputBlock(valuesBlock);
        unnestBlockBuilder.startNewOutput(new PageBuilderStatus(), 10);

        // Nothing added to the output
        Block block = unnestBlockBuilder.buildOutputAndFlush();
        assertTrue(block instanceof DictionaryBlock);
        assertBlock(block, new Slice[]{});
    }

    private static void testUnnestBlockBuilderMethods(String[] stringValues)
    {
        Slice[] values = toSlices(stringValues);
        UnnestBlockBuilder unnestBlockBuilder = new UnnestBlockBuilder(VARCHAR);
        Block valuesBlock = createSimpleBlock(values);
        unnestBlockBuilder.resetInputBlock(valuesBlock);

        testAppendSingleElement(unnestBlockBuilder, values);
        testAppendRange(unnestBlockBuilder, values);
        testAppendNull(unnestBlockBuilder, values);
    }

    private static void testAppendSingleElement(UnnestBlockBuilder unnestBlockBuilder, Slice[] values)
    {
        // Test unnestBlockBuilder output over the course of building output
        for (int testCount = 1; testCount <= values.length; testCount++) {
            unnestBlockBuilder.startNewOutput(new PageBuilderStatus(), 10);

            for (int i = 0; i < testCount; i++) {
                unnestBlockBuilder.appendElement(i);
            }

            Block block = unnestBlockBuilder.buildOutputAndFlush();
            assertTrue(block instanceof DictionaryBlock);
            assertBlock(block, Arrays.copyOf(values, testCount));
        }
    }

    private static void testAppendRange(UnnestBlockBuilder unnestBlockBuilder, Slice[] values)
    {
        unnestBlockBuilder.startNewOutput(new PageBuilderStatus(), 10);
        assertTrue(values.length >= 3, "test requires at least 3 elements in values");
        int startIndex = 1;
        int length = values.length - 2;
        unnestBlockBuilder.appendRange(startIndex, length);
        Block block = unnestBlockBuilder.buildOutputAndFlush();
        assertTrue(block instanceof DictionaryBlock);
        assertBlock(block, Arrays.copyOfRange(values, startIndex, startIndex + length));
    }

    private static void testAppendNull(UnnestBlockBuilder unnestBlockBuilder, Slice[] values)
    {
        assertTrue(values.length >= 1, "values should have at least one element");

        int nullIndex = -1;
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                nullIndex = i;
                break;
            }
        }

        // Test-1: appending a non-null element
        unnestBlockBuilder.startNewOutput(new PageBuilderStatus(), 10);
        unnestBlockBuilder.appendElement(0);
        Block block = unnestBlockBuilder.buildOutputAndFlush();
        assertTrue(block instanceof DictionaryBlock);
        assertBlock(block, new Slice[]{values[0]});

        // Test-2: appending a non-null element and a null.
        unnestBlockBuilder.startNewOutput(new PageBuilderStatus(), 10);
        unnestBlockBuilder.appendElement(0);
        unnestBlockBuilder.appendNull();
        block = unnestBlockBuilder.buildOutputAndFlush();
        assertTrue((block instanceof DictionaryBlock) || (nullIndex == -1));
        assertFalse((block instanceof DictionaryBlock) && (nullIndex == -1));
        assertBlock(block, new Slice[]{values[0], null});

        // Test-3: appending a non-null element, a null, and another non-null element.
        unnestBlockBuilder.startNewOutput(new PageBuilderStatus(), 10);
        unnestBlockBuilder.appendElement(0);
        unnestBlockBuilder.appendNull();
        unnestBlockBuilder.appendElement(0);
        block = unnestBlockBuilder.buildOutputAndFlush();
        assertTrue((block instanceof DictionaryBlock) || (nullIndex == -1));
        assertFalse((block instanceof DictionaryBlock) && (nullIndex == -1));
        assertBlock(block, new Slice[]{values[0], null, values[0]});
    }
}
