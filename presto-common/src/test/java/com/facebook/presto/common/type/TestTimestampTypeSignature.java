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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestTimestampTypeSignature
{
    @Test
    public void testDefaultPrecisionSerializesWithoutParam()
    {
        TimestampType ts3 = createTimestampType(3);
        assertEquals(ts3.getTypeSignature().toString(), "timestamp");
        assertEquals(ts3.getTypeSignature().getParameters().size(), 0);
    }

    @Test
    public void testNonDefaultPrecisionsSerializeWithParam()
    {
        for (int p = 0; p <= 12; p++) {
            if (p == 3) {
                continue;
            }
            TimestampType type = createTimestampType(p);
            String sig = type.getTypeSignature().toString();
            assertEquals(sig, "timestamp(" + p + ")",
                    "Expected timestamp(" + p + ") but got: " + sig);
        }
    }

    @Test
    public void testMicrosecondsPrecisionUsesParametricForm()
    {
        TimestampType ts6 = createTimestampType(6);
        assertEquals(ts6.getTypeSignature().toString(), "timestamp(6)");
        assertEquals(ts6.getTypeSignature().getBase(), "timestamp");
        assertEquals(ts6.getTypeSignature().getParameters().size(), 1);
        assertEquals(ts6.getTypeSignature().getParameters().get(0).getLongLiteral(), 6L);
    }

    @Test
    public void testBareTimestampParsesToDefaultPrecision()
    {
        TypeSignature sig = parseTypeSignature("timestamp");
        assertEquals(sig.getBase(), "timestamp");
        assertEquals(sig.getParameters().size(), 0);
        // TimestampParametricType.createType with no params returns TIMESTAMP(3)
        Type resolved = TimestampParametricType.TIMESTAMP.createType(ImmutableList.of());
        assertSame(resolved, createTimestampType(3));
    }

    @Test
    public void testExplicitPrecision3RoundTrips()
    {
        TypeSignature sig = parseTypeSignature("timestamp(3)");
        assertEquals(sig.getBase(), "timestamp");
        assertEquals(sig.getParameters().size(), 1);
        assertEquals(sig.getParameters().get(0).getLongLiteral(), 3L);
        TypeParameter param = TypeParameter.of(3L);
        Type resolved = TimestampParametricType.TIMESTAMP.createType(ImmutableList.of(param));
        assertSame(resolved, createTimestampType(3));
    }

    @Test
    public void testRoundTripForAllPrecisions()
    {
        for (int p = 0; p <= 12; p++) {
            TimestampType original = createTimestampType(p);
            String serialized = original.getTypeSignature().toString();

            // Parse the serialized form back to a TypeSignature
            TypeSignature parsed = parseTypeSignature(serialized);

            // Reconstruct via TimestampParametricType
            Type reconstructed;
            if (parsed.getParameters().isEmpty()) {
                reconstructed = TimestampParametricType.TIMESTAMP.createType(ImmutableList.of());
            }
            else {
                long precision = parsed.getParameters().get(0).getLongLiteral();
                reconstructed = TimestampParametricType.TIMESTAMP.createType(
                        ImmutableList.of(TypeParameter.of(precision)));
            }

            assertTrue(reconstructed instanceof TimestampType);
            assertEquals(((TimestampType) reconstructed).getPrecision(), p,
                    "Round-trip failed for precision " + p);
            assertSame(reconstructed, original,
                    "Round-trip returned different instance for precision " + p);
        }
    }

    @Test
    public void testMicrosecondsRoundTrip()
    {
        TimestampType ts6 = createTimestampType(6);
        String serialized = ts6.getTypeSignature().toString();
        assertEquals(serialized, "timestamp(6)");

        TypeSignature parsed = parseTypeSignature(serialized);
        TypeParameter param = TypeParameter.of(parsed.getParameters().get(0).getLongLiteral());
        Type reconstructed = TimestampParametricType.TIMESTAMP.createType(ImmutableList.of(param));

        assertSame(reconstructed, ts6);
        assertEquals(((TimestampType) reconstructed).getPrecision(), 6);
    }
}
