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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.JoinProbe.JoinProbeFactory;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLookupJoinPageBuilder
{
    @Test
    public void testPageBuilder()
    {
        int entries = 10_000;
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block, block);

        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(new int[] {0, 1}, ImmutableList.of(0, 1), OptionalInt.empty());
        JoinProbe probe = joinProbeFactory.createJoinProbe(page);
        LookupSource lookupSource = new TestLookupSource(ImmutableList.of(BIGINT, BIGINT), page);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(ImmutableList.of(BIGINT, BIGINT));

        int joinPosition = 0;
        while (!lookupJoinPageBuilder.isFull() && probe.advanceNextPosition()) {
            lookupJoinPageBuilder.appendRow(probe, lookupSource, joinPosition++);
            lookupJoinPageBuilder.appendNullForBuild(probe);
        }
        assertFalse(lookupJoinPageBuilder.isEmpty());

        Page output = lookupJoinPageBuilder.build(probe);
        assertEquals(output.getChannelCount(), 4);
        assertTrue(output.getBlock(0) instanceof DictionaryBlock);
        assertTrue(output.getBlock(1) instanceof DictionaryBlock);
        for (int i = 0; i < output.getPositionCount(); i++) {
            assertFalse(output.getBlock(0).isNull(i));
            assertFalse(output.getBlock(1).isNull(i));
            assertEquals(output.getBlock(0).getLong(i), i / 2);
            assertEquals(output.getBlock(1).getLong(i), i / 2);
            if (i % 2 == 0) {
                assertFalse(output.getBlock(2).isNull(i));
                assertFalse(output.getBlock(3).isNull(i));
                assertEquals(output.getBlock(2).getLong(i), i / 2);
                assertEquals(output.getBlock(3).getLong(i), i / 2);
            }
            else {
                assertTrue(output.getBlock(2).isNull(i));
                assertTrue(output.getBlock(3).isNull(i));
            }
        }
        assertTrue(lookupJoinPageBuilder.toString().contains("positionCount=" + output.getPositionCount()));

        lookupJoinPageBuilder.reset();
        assertTrue(lookupJoinPageBuilder.isEmpty());
    }

    @Test
    public void testDifferentPositions()
    {
        int entries = 100;
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block);
        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(new int[] {0}, ImmutableList.of(0), OptionalInt.empty());
        LookupSource lookupSource = new TestLookupSource(ImmutableList.of(BIGINT), page);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(ImmutableList.of(BIGINT));

        // empty
        JoinProbe probe = joinProbeFactory.createJoinProbe(page);
        Page output = lookupJoinPageBuilder.build(probe);
        assertEquals(output.getChannelCount(), 2);
        assertTrue(output.getBlock(0) instanceof DictionaryBlock);
        assertEquals(output.getPositionCount(), 0);
        lookupJoinPageBuilder.reset();

        // the probe covers non-sequential positions
        probe = joinProbeFactory.createJoinProbe(page);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            if (joinPosition % 2 == 1) {
                continue;
            }
            lookupJoinPageBuilder.appendRow(probe, lookupSource, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe);
        assertEquals(output.getChannelCount(), 2);
        assertTrue(output.getBlock(0) instanceof DictionaryBlock);
        assertEquals(output.getPositionCount(), entries / 2);
        for (int i = 0; i < entries / 2; i++) {
            assertEquals(output.getBlock(0).getLong(i), i * 2);
            assertEquals(output.getBlock(1).getLong(i), i * 2);
        }
        lookupJoinPageBuilder.reset();

        // the probe covers everything
        probe = joinProbeFactory.createJoinProbe(page);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            lookupJoinPageBuilder.appendRow(probe, lookupSource, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe);
        assertEquals(output.getChannelCount(), 2);
        assertFalse(output.getBlock(0) instanceof DictionaryBlock);
        assertEquals(output.getPositionCount(), entries);
        for (int i = 0; i < entries; i++) {
            assertEquals(output.getBlock(0).getLong(i), i);
            assertEquals(output.getBlock(1).getLong(i), i);
        }
        lookupJoinPageBuilder.reset();

        // the probe covers some sequential positions
        probe = joinProbeFactory.createJoinProbe(page);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            if (joinPosition < 10 || joinPosition >= 50) {
                continue;
            }
            lookupJoinPageBuilder.appendRow(probe, lookupSource, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe);
        assertEquals(output.getChannelCount(), 2);
        assertFalse(output.getBlock(0) instanceof DictionaryBlock);
        assertEquals(output.getPositionCount(), 40);
        for (int i = 10; i < 50; i++) {
            assertEquals(output.getBlock(0).getLong(i - 10), i);
            assertEquals(output.getBlock(1).getLong(i - 10), i);
        }
    }

    @Test
    public void testCrossJoinWithEmptyBuild()
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(blockBuilder, 0);
        Page page = new Page(blockBuilder.build());

        // nothing on the build side so we don't append anything
        LookupSource lookupSource = new TestLookupSource(ImmutableList.of(), page);
        JoinProbe probe = (new JoinProbeFactory(new int[] {0}, ImmutableList.of(0), OptionalInt.empty())).createJoinProbe(page);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(ImmutableList.of(BIGINT));

        // append the same row many times should also flush in the end
        probe.advanceNextPosition();
        for (int i = 0; i < 300_000; i++) {
            lookupJoinPageBuilder.appendRow(probe, lookupSource, 0);
        }
        assertTrue(lookupJoinPageBuilder.isFull());
    }

    private final class TestLookupSource
            implements LookupSource
    {
        private final List<Type> types;
        private final Page page;

        public TestLookupSource(List<Type> types, Page page)
        {
            this.types = types;
            this.page = page;
        }

        @Override
        public boolean isEmpty()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getChannelCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPositionCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long joinPositionWithinPartition(long joinPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page page, Page allChannelsPage, long rawHash)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            for (int i = 0; i < types.size(); i++) {
                types.get(i).appendTo(page.getBlock(i), (int) position, pageBuilder.getBlockBuilder(i));
            }
        }

        @Override
        public void close()
        {
        }
    }
}
