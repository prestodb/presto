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

import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.orc.FlushReason.DICTIONARY_FULL;
import static com.facebook.presto.orc.FlushReason.MAX_BYTES;
import static com.facebook.presto.orc.FlushReason.MAX_ROWS;
import static io.airlift.units.DataSize.Unit.BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDefaultOrcWriterFlushPolicy
{
    @Test
    public void testFlushMaxStripeRowCount()
    {
        DefaultOrcWriterFlushPolicy flushPolicy = DefaultOrcWriterFlushPolicy.builder()
                .withStripeMaxRowCount(10)
                .build();

        assertEquals(flushPolicy.getStripeMaxRowCount(), 10);

        Optional<FlushReason> actual = flushPolicy.shouldFlushStripe(5, 0, false);
        assertFalse(actual.isPresent());

        actual = flushPolicy.shouldFlushStripe(10, 0, false);
        assertTrue(actual.isPresent());
        assertEquals(actual.get(), MAX_ROWS);

        actual = flushPolicy.shouldFlushStripe(20, 0, false);
        assertFalse(actual.isPresent());
    }

    @Test
    public void testFlushMaxStripeSize()
    {
        DefaultOrcWriterFlushPolicy flushPolicy = DefaultOrcWriterFlushPolicy.builder()
                .withStripeMinSize(new DataSize(50, BYTE))
                .withStripeMaxSize(new DataSize(100, BYTE))
                .build();

        assertEquals(flushPolicy.getStripeMinBytes(), 50);
        assertEquals(flushPolicy.getStripeMaxBytes(), 100);

        Optional<FlushReason> actual = flushPolicy.shouldFlushStripe(1, 90, false);
        assertFalse(actual.isPresent());

        actual = flushPolicy.shouldFlushStripe(1, 100, false);
        assertFalse(actual.isPresent());

        actual = flushPolicy.shouldFlushStripe(1, 200, false);
        assertTrue(actual.isPresent());
        assertEquals(actual.get(), MAX_BYTES);
    }

    @Test
    public void testFlushDictionaryFull()
    {
        DefaultOrcWriterFlushPolicy flushPolicy = DefaultOrcWriterFlushPolicy.builder().build();

        Optional<FlushReason> actual = flushPolicy.shouldFlushStripe(1, 1, false);
        assertFalse(actual.isPresent());

        actual = flushPolicy.shouldFlushStripe(1, 1, true);
        assertTrue(actual.isPresent());
        assertEquals(actual.get(), DICTIONARY_FULL);
    }
}
