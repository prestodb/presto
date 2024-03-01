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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createEmptyBlock;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.fail;

public class TestOptimizedTypedSet
{
    private static final String FUNCTION_NAME = "optimized_typed_set_test";
    private static final int POSITIONS_PER_PAGE = 100;

    @Test
    public void testConstructor()
    {
        for (int i = -2; i <= -1; i++) {
            try {
                //noinspection ResultOfObjectAllocationIgnored
                new OptimizedTypedSet(BIGINT, 2, i);
                fail("Should throw exception if expectedSize < 0");
            }
            catch (IllegalArgumentException e) {
                // ignored
            }
        }

        try {
            //noinspection ResultOfObjectAllocationIgnored
            new OptimizedTypedSet(null, -1, 1);
            fail("Should throw exception if expectedBlockCount is negative");
        }
        catch (NullPointerException | IllegalArgumentException e) {
            // ignored
        }

        try {
            //noinspection ResultOfObjectAllocationIgnored
            new OptimizedTypedSet(null, 2, 1);
            fail("Should throw exception if type is null");
        }
        catch (NullPointerException | IllegalArgumentException e) {
            // ignored
        }
    }

    @Test
    public void testUnionWithDistinctValues()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE + 1);

        Block block = createLongSequenceBlock(0, POSITIONS_PER_PAGE / 2);
        testUnion(typedSet, block, block);

        block = createLongSequenceBlock(0, POSITIONS_PER_PAGE);
        testUnion(typedSet, createLongSequenceBlock(POSITIONS_PER_PAGE / 2, POSITIONS_PER_PAGE), block);

        // Test adding a block with null
        Block blockWithNull = block.appendNull();
        testUnion(typedSet, blockWithNull, blockWithNull);
    }

    @Test
    public void testUnionWithRepeatingValues()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE);

        Block block = createLongRepeatBlock(0, POSITIONS_PER_PAGE);
        Block expectedBlock = createLongRepeatBlock(0, 1);
        testUnion(typedSet, block, expectedBlock);

        // Test adding a block with null
        Block blockWithNull = block.appendNull();
        expectedBlock = expectedBlock.appendNull();
        testUnion(typedSet, blockWithNull, expectedBlock);
    }

    @Test
    public void testIntersectWithEmptySet()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE);
        testIntersect(typedSet, createLongSequenceBlock(0, POSITIONS_PER_PAGE - 1).appendNull(), createEmptyBlock(BIGINT));
    }

    @Test
    public void testIntersectWithDistinctValues()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE);

        Block block = createLongSequenceBlock(0, POSITIONS_PER_PAGE - 1).appendNull();
        typedSet.union(block);

        testIntersect(typedSet, block, block);

        block = createLongSequenceBlock(0, POSITIONS_PER_PAGE / 2 - 1).appendNull();
        testIntersect(typedSet, block, block);

        block = createLongSequenceBlock(0, 1).appendNull();
        testIntersect(typedSet, block, block);
    }

    @Test
    public void testIntersectWithNonDistinctValues()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE);

        Block block = createLongSequenceBlock(0, POSITIONS_PER_PAGE - 1).appendNull();
        typedSet.union(block);

        block = createLongRepeatBlock(0, POSITIONS_PER_PAGE - 1).appendNull();
        testIntersect(typedSet, block, createLongSequenceBlock(0, 1).appendNull());

        block = createLongSequenceBlock(0, 0).appendNull();
        testIntersect(typedSet, block, block);

        block = createLongSequenceBlock(0, 0);
        testIntersect(typedSet, block, block);
    }

    @Test
    public void testExceptWithDistinctValues()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE);

        Block block = createLongSequenceBlock(0, POSITIONS_PER_PAGE - 1).appendNull();
        typedSet.union(block);

        testExcept(typedSet, block, createEmptyBlock(BIGINT));
        testExcept(typedSet, block, block);
    }

    @Test
    public void testExceptWithRepeatingValues()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE);

        Block block = createLongRepeatBlock(0, POSITIONS_PER_PAGE - 1).appendNull();
        testExcept(typedSet, block, createLongSequenceBlock(0, 1).appendNull());
    }

    @Test
    public void testMultipleOperations()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE + 1);

        Block block = createLongSequenceBlock(0, POSITIONS_PER_PAGE / 2).appendNull();

        testUnion(typedSet, block, block);
        testIntersect(typedSet, block, block);

        block = createLongSequenceBlock(POSITIONS_PER_PAGE / 2, POSITIONS_PER_PAGE);
        testExcept(typedSet, block.appendNull(), block);
        testExcept(typedSet, createLongSequenceBlock(0, POSITIONS_PER_PAGE), createLongSequenceBlock(0, POSITIONS_PER_PAGE / 2));

        testUnion(typedSet, block, createLongSequenceBlock(0, POSITIONS_PER_PAGE));
        testIntersect(typedSet, createEmptyBlock(BIGINT).appendNull(), createEmptyBlock(BIGINT));
    }

    @Test
    public void testNulls()
    {
        OptimizedTypedSet typedSet = new OptimizedTypedSet(BIGINT, POSITIONS_PER_PAGE + 1);

        // Empty block
        Block emptyBlock = createLongSequenceBlock(0, 0);

        testUnion(typedSet, emptyBlock, emptyBlock);

        // Block with a single null
        Block singleNullBlock = emptyBlock.appendNull();

        testUnion(typedSet, singleNullBlock, singleNullBlock);
        testIntersect(typedSet, singleNullBlock, singleNullBlock);
        testIntersect(typedSet, emptyBlock, emptyBlock);
        testExcept(typedSet, singleNullBlock, singleNullBlock);
        testIntersect(typedSet, emptyBlock, emptyBlock);

        // Block with a 0, and block with a 0 and a null
        Block zero = createLongSequenceBlock(0, 0);
        Block zeroAndNull = zero.appendNull();

        testUnion(typedSet, zero, zero);
        testUnion(typedSet, singleNullBlock, zeroAndNull);
        testIntersect(typedSet, zero, zero);
        testExcept(typedSet, singleNullBlock, singleNullBlock);
        testUnion(typedSet, zero, zeroAndNull);
    }

    private void testUnion(OptimizedTypedSet typedSet, Block block, Block expectedBlock)
    {
        typedSet.union(block);
        Block resultBlock = typedSet.getBlock();
        assertBlockEquals(BIGINT, resultBlock, expectedBlock);
    }

    private void testIntersect(OptimizedTypedSet typedSet, Block block, Block expectedBlock)
    {
        typedSet.intersect(block);
        Block resultBlock = typedSet.getBlock();
        assertBlockEquals(BIGINT, resultBlock, expectedBlock);
    }

    private void testExcept(OptimizedTypedSet typedSet, Block block, Block expectedBlock)
    {
        typedSet.except(block);
        Block resultBlock = typedSet.getBlock();
        assertBlockEquals(BIGINT, resultBlock, expectedBlock);
    }
}
