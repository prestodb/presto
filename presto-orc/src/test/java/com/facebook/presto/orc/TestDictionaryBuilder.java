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
package com.facebook.presto.orc;

import com.facebook.presto.orc.writer.DictionaryBuilder;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.wrappedBuffer;
import static org.testng.Assert.assertEquals;

public class TestDictionaryBuilder
{
    @Test
    public void testSkipReservedSlots()
    {
        Set<Integer> positions = new HashSet<>();
        DictionaryBuilder dictionaryBuilder = new DictionaryBuilder(64);
        for (int i = 0; i < 64; i++) {
            positions.add(dictionaryBuilder.putIfAbsent(new TestHashCollisionBlock(1, wrappedBuffer(new byte[]{1}), new int[]{0, 1}, new boolean[]{false}), 0));
            positions.add(dictionaryBuilder.putIfAbsent(new TestHashCollisionBlock(1, wrappedBuffer(new byte[]{2}), new int[]{0, 1}, new boolean[]{false}), 0));
        }
        assertEquals(positions, ImmutableSet.of(1, 2));
    }

    private class TestHashCollisionBlock
            extends VariableWidthBlock
    {
        public TestHashCollisionBlock(int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull)
        {
            super(positionCount, slice, offsets, Optional.of(valueIsNull));
        }

        @Override
        public long hash(int position, int offset, int length)
        {
            // return 0 to hash to the reserved null position which is zero
            return 0;
        }
    }
}
