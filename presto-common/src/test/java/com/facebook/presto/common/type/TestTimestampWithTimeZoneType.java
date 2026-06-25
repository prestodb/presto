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

import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

public class TestTimestampWithTimeZoneType
{
    @Test
    public void testInterning()
    {
        for (int p = 0; p <= 12; p++) {
            assertSame(createTimestampWithTimeZoneType(p), createTimestampWithTimeZoneType(p),
                    "Expected same instance for precision " + p);
        }
    }

    @Test
    public void testPreAllocation()
    {
        for (int p = 0; p <= 12; p++) {
            TimestampWithTimeZoneType instance = createTimestampWithTimeZoneType(p);
            assertEquals(instance.getPrecision(), p);
        }
    }

    @Test
    public void testBackwardCompatConstant()
    {
        assertSame(TIMESTAMP_WITH_TIME_ZONE, createTimestampWithTimeZoneType(3));
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getPrecision(), 3);
    }

    @Test
    public void testBackwardCompatTypeSignature()
    {
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().getBase(), "timestamp with time zone");
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().getParameters().size(), 0);
    }

    @Test
    public void testReferenceEquality()
    {
        TimestampWithTimeZoneType a = createTimestampWithTimeZoneType(6);
        TimestampWithTimeZoneType b = createTimestampWithTimeZoneType(6);
        assertSame(a, b);
        assertEquals(a, b);
    }

    @Test
    public void testEqualsWithConstant()
    {
        assertEquals(TIMESTAMP_WITH_TIME_ZONE, createTimestampWithTimeZoneType(3));
    }

    @Test
    public void testInvalidPrecisionNegative()
    {
        try {
            createTimestampWithTimeZoneType(-1);
            fail("Expected IllegalArgumentException for precision -1");
        }
        catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testInvalidPrecisionTooLarge()
    {
        try {
            createTimestampWithTimeZoneType(13);
            fail("Expected IllegalArgumentException for precision 13");
        }
        catch (IllegalArgumentException e) {
            // expected
        }
    }

    // All 13 instances are distinct objects
    @Test
    public void testDistinctInstances()
    {
        for (int p = 0; p <= 12; p++) {
            for (int q = p + 1; q <= 12; q++) {
                TimestampWithTimeZoneType a = createTimestampWithTimeZoneType(p);
                TimestampWithTimeZoneType b = createTimestampWithTimeZoneType(q);
                // Different precisions → different instances
                assertNotSame(a, b, "Expected distinct instances for p=" + p + " and q=" + q);
            }
        }
    }
}
