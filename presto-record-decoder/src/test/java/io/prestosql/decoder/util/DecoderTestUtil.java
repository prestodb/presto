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
package io.prestosql.decoder.util;

import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public final class DecoderTestUtil
{
    private DecoderTestUtil() {}

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, String value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getSlice().toStringUtf8(), value);
    }

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getLong(), value);
    }

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, double value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getDouble(), value, 0.0001);
    }

    public static void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, boolean value)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertEquals(provider.getBoolean(), value);
    }

    public static void checkIsNull(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        assertTrue(provider.isNull());
    }
}
