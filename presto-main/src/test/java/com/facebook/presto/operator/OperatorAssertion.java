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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public final class OperatorAssertion
{
    private OperatorAssertion()
    {
    }

    public static List<Page> appendSampleWeight(List<Page> input, int sampleWeight)
    {
        return input.stream()
                .map(page -> {
                    BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        BIGINT.writeLong(builder, sampleWeight);
                    }
                    Block[] blocks = new Block[page.getChannelCount() + 1];
                    System.arraycopy(page.getBlocks(), 0, blocks, 0, page.getChannelCount());
                    blocks[blocks.length - 1] = builder.build();
                    return new Page(blocks);
                })
                .collect(toImmutableList());
    }

    public static List<Page> toPages(Operator operator, Iterator<Page> input)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;

        while (operator.needsInput() && input.hasNext()) {
            operator.addInput(input.next());
        }

        for (int loops = 0; !operator.isFinished() && loops < 10_000; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    operator.addInput(inputPage);
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        assertFalse(operator.needsInput());
        assertTrue(operator.isBlocked().isDone());
        assertTrue(operator.isFinished());

        return outputPages.build();
    }

    public static List<Page> toPages(Operator operator, List<Page> input)
    {
        // verify initial state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), true);
        assertEquals(operator.getOutput(), null);

        return toPages(operator, input.iterator());
    }

    public static List<Page> toPages(Operator operator)
    {
        // operator does not have input so should never require input
        assertEquals(operator.needsInput(), false);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        addRemainingOutputPages(operator, outputPages);
        return outputPages.build();
    }

    private static void addRemainingOutputPages(Operator operator, ImmutableList.Builder<Page> outputPages)
    {
        // pull remaining output pages
        while (!operator.isFinished()) {
            // at this point the operator should not need more input
            assertEquals(operator.needsInput(), false);

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        // verify final state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);
    }

    public static MaterializedResult toMaterializedResult(Session session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            resultBuilder.page(outputPage);
        }
        return resultBuilder.build();
    }

    public static void assertOperatorEquals(Operator operator, List<Page> expected)
    {
        List<Page> actual = toPages(operator);
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(operator.getTypes(), actual.get(i), expected.get(i));
        }
    }

    public static void assertOperatorEquals(Operator operator, List<Page> input, List<Page> expected)
    {
        List<Page> actual = toPages(operator, input);
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(operator.getTypes(), actual.get(i), expected.get(i));
        }
    }

    public static void assertOperatorEquals(Operator operator, MaterializedResult expected)
    {
        List<Page> pages = toPages(operator);
        MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), operator.getTypes(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEquals(Operator operator, List<Page> input, MaterializedResult expected)
    {
        assertOperatorEquals(operator, input, expected, false, ImmutableList.<Integer>of());
    }

    public static void assertOperatorEquals(Operator operator, List<Page> input, MaterializedResult expected, boolean hashEnabled, List<Integer> hashChannels)
    {
        List<Page> pages = toPages(operator, input);
        MaterializedResult actual;
        if (hashEnabled && !hashChannels.isEmpty()) {
            // Drop the hashChannel for all pages
            List<Page> actualPages = dropChannel(pages, hashChannels);
            List<Type> expectedTypes = without(operator.getTypes(), hashChannels);
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), expectedTypes, actualPages);
        }
        else {
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), operator.getTypes(), pages);
        }
        assertEquals(actual, expected);
    }

    public static void assertOperatorEqualsIgnoreOrder(Operator operator, List<Page> input, MaterializedResult expected)
    {
        assertOperatorEqualsIgnoreOrder(operator, input, expected, false, Optional.empty());
    }

    public static void assertOperatorEqualsIgnoreOrder(Operator operator, List<Page> input, MaterializedResult expected, boolean hashEnabled, Optional<Integer> hashChannel)
    {
        List<Page> pages = toPages(operator, input);
        MaterializedResult actual;
        if (hashEnabled && hashChannel.isPresent()) {
            // Drop the hashChannel for all pages
            List<Page> actualPages = dropChannel(pages, ImmutableList.of(hashChannel.get()));
            List<Type> expectedTypes = without(operator.getTypes(), ImmutableList.of(hashChannel.get()));
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), expectedTypes, actualPages);
        }
        else {
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), operator.getTypes(), pages);
        }

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    static <T> List<T> without(List<T> types, List<Integer> channels)
    {
        types = new ArrayList<>(types);
        int removed = 0;
        for (int hashChannel : channels) {
            types.remove(hashChannel - removed);
            removed++;
        }
        return ImmutableList.copyOf(types);
    }

    static List<Page> dropChannel(List<Page> pages, List<Integer> channels)
    {
        List<Page> actualPages = new ArrayList<>();
        for (Page page : pages) {
            int channel = 0;
            Block[] blocks = new Block[page.getChannelCount() - channels.size()];
            for (int i = 0; i < page.getChannelCount(); i++) {
                if (channels.contains(i)) {
                    continue;
                }
                blocks[channel++] = page.getBlock(i);
            }
            actualPages.add(new Page(blocks));
        }
        return actualPages;
    }
}
