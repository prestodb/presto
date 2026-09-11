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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestTimestampWithTimeZoneType
{
    @Test
    public void testInterning()
    {
        for (int p = 0; p <= TimestampWithTimeZoneType.MAX_PRECISION; p++) {
            assertSame(createTimestampWithTimeZoneType(p), createTimestampWithTimeZoneType(p),
                    "Expected same instance for precision " + p);
        }
    }

    @Test
    public void testPreAllocation()
    {
        for (int p = 0; p <= TimestampWithTimeZoneType.MAX_PRECISION; p++) {
            TimestampWithTimeZoneType instance = createTimestampWithTimeZoneType(p);
            assertEquals(instance.getPrecision(), p);
        }
    }

    @Test
    public void testBackwardCompatConstant()
    {
        assertSame(TIMESTAMP_WITH_TIME_ZONE, createTimestampWithTimeZoneType(3));
    }

    @Test
    public void testBackwardCompatTypeSignature()
    {
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().getBase(), "timestamp with time zone");
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().getParameters().size(), 0);
    }

    @Test
    public void testInvalidPrecisionNegative()
    {
        expectThrows(IllegalArgumentException.class, () -> createTimestampWithTimeZoneType(-1));
    }

    @Test
    public void testInvalidPrecisionTooLarge()
    {
        expectThrows(IllegalArgumentException.class, () -> createTimestampWithTimeZoneType(TimestampWithTimeZoneType.MAX_PRECISION + 1));
    }

    @Test
    public void testDistinctInstances()
    {
        for (int p = 0; p <= TimestampWithTimeZoneType.MAX_PRECISION; p++) {
            for (int q = p + 1; q <= TimestampWithTimeZoneType.MAX_PRECISION; q++) {
                TimestampWithTimeZoneType a = createTimestampWithTimeZoneType(p);
                TimestampWithTimeZoneType b = createTimestampWithTimeZoneType(q);
                assertNotSame(a, b, "Expected distinct instances for p=" + p + " and q=" + q);
            }
        }
    }

    @Test
    public void testGetObjectValueSupportedForDefaultPrecision()
    {
        BlockBuilder builder = TIMESTAMP_WITH_TIME_ZONE.createBlockBuilder(null, 1);
        TIMESTAMP_WITH_TIME_ZONE.writeLong(builder, 0L);
        Block block = builder.build();

        Object value = TIMESTAMP_WITH_TIME_ZONE.getObjectValue(null, block, 0);
        assertTrue(value instanceof SqlTimestampWithTimeZone, "Expected a SqlTimestampWithTimeZone for the default precision");
    }

    @Test
    public void testGetObjectValueUnsupportedForNonDefaultPrecision()
    {
        BlockBuilder builder = TIMESTAMP_WITH_TIME_ZONE.createBlockBuilder(null, 1);
        TIMESTAMP_WITH_TIME_ZONE.writeLong(builder, 0L);
        Block block = builder.build();

        for (int p = 0; p <= TimestampWithTimeZoneType.MAX_PRECISION; p++) {
            if (p == TimestampWithTimeZoneType.DEFAULT_PRECISION) {
                continue;
            }
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(p);
            expectThrows(UnsupportedOperationException.class, () -> type.getObjectValue(null, block, 0));
        }
    }
}
