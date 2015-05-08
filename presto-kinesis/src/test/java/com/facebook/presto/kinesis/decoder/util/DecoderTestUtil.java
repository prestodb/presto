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
package com.facebook.presto.kinesis.decoder.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import com.facebook.presto.kinesis.KinesisColumnHandle;
import com.facebook.presto.kinesis.KinesisFieldValueProvider;

public class DecoderTestUtil
{
    private DecoderTestUtil() {}

    private static KinesisFieldValueProvider findValueProvider(Set<KinesisFieldValueProvider> providers, KinesisColumnHandle handle)
    {
        for (KinesisFieldValueProvider provider : providers) {
            if (provider.accept(handle)) {
                return provider;
            }
        }
        return null;
    }

    public static void checkValue(Set<KinesisFieldValueProvider> providers, KinesisColumnHandle handle, String value)
    {
        KinesisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(new String(provider.getSlice().getBytes(), StandardCharsets.UTF_8), value);
    }

    public static void checkValue(Set<KinesisFieldValueProvider> providers, KinesisColumnHandle handle, long value)
    {
        KinesisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getLong(), value);
    }

    public static void checkValue(Set<KinesisFieldValueProvider> providers, KinesisColumnHandle handle, double value)
    {
        KinesisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getDouble(), value, 0.0001);
    }

    public static void checkValue(Set<KinesisFieldValueProvider> providers, KinesisColumnHandle handle, boolean value)
    {
        KinesisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getBoolean(), value);
    }

    public static void checkIsNull(Set<KinesisFieldValueProvider> providers, KinesisColumnHandle handle)
    {
        KinesisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertTrue(provider.isNull());
    }
}
