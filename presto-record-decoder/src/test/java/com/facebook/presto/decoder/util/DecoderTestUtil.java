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
package com.facebook.presto.decoder.util;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public final class DecoderTestUtil
{
    private DecoderTestUtil() {}

    private static FieldValueProvider findValueProvider(Set<FieldValueProvider> providers, DecoderColumnHandle handle)
    {
        for (FieldValueProvider provider : providers) {
            if (provider.accept(handle)) {
                return provider;
            }
        }
        return null;
    }

    public static void checkValue(Set<FieldValueProvider> providers, DecoderColumnHandle handle, String value)
    {
        FieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(new String(provider.getSlice().getBytes(), StandardCharsets.UTF_8), value);
    }

    public static void checkValue(Set<FieldValueProvider> providers, DecoderColumnHandle handle, long value)
    {
        FieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getLong(), value);
    }

    public static void checkValue(Set<FieldValueProvider> providers, DecoderColumnHandle handle, double value)
    {
        FieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getDouble(), value, 0.0001);
    }

    public static void checkValue(Set<FieldValueProvider> providers, DecoderColumnHandle handle, boolean value)
    {
        FieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getBoolean(), value);
    }

    public static void checkIsNull(Set<FieldValueProvider> providers, DecoderColumnHandle handle)
    {
        FieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertTrue(provider.isNull());
    }
}
