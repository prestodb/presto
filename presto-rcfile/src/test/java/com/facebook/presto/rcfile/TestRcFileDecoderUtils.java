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
package com.facebook.presto.rcfile;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.hadoop.io.WritableUtils;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TestRcFileDecoderUtils
{
    @Test
    public void testVInt()
            throws Exception
    {
        Slice slice = Slices.allocate(100);
        SliceOutput output = slice.getOutput();

        assertVIntRoundTrip(output, 0);
        assertVIntRoundTrip(output, 1);
        assertVIntRoundTrip(output, -1);
        assertVIntRoundTrip(output, Integer.MAX_VALUE);
        assertVIntRoundTrip(output, Integer.MAX_VALUE + 1L);
        assertVIntRoundTrip(output, Integer.MAX_VALUE - 1L);
        assertVIntRoundTrip(output, Integer.MIN_VALUE);
        assertVIntRoundTrip(output, Integer.MIN_VALUE + 1L);
        assertVIntRoundTrip(output, Integer.MIN_VALUE - 1L);
        assertVIntRoundTrip(output, Long.MAX_VALUE);
        assertVIntRoundTrip(output, Long.MAX_VALUE - 1);
        assertVIntRoundTrip(output, Long.MIN_VALUE + 1);

        for (int value = -100_000; value < 100_000; value++) {
            assertVIntRoundTrip(output, value);
        }
    }

    private static void assertVIntRoundTrip(SliceOutput output, long value)
            throws IOException
    {
        Slice oldBytes = writeVintOld(output, value);

        long readValueOld = WritableUtils.readVLong(oldBytes.getInput());
        assertEquals(readValueOld, value);
        long readValueNew = RcFileDecoderUtils.readVInt(oldBytes, 0);
        assertEquals(readValueNew, value);
    }

    private static Slice writeVintOld(SliceOutput output, long value)
            throws IOException
    {
        output.reset();
        WritableUtils.writeVLong(output, value);
        Slice vLong = Slices.copyOf(output.slice());

        if (value == (int) value) {
            output.reset();
            WritableUtils.writeVInt(output, (int) value);
            Slice vInt = Slices.copyOf(output.slice());
            assertEquals(vInt, vLong);
        }
        return vLong;
    }
}
