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
package com.facebook.presto.kafka.decoder.util;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public final class DecoderTestUtil
{
    private DecoderTestUtil() {}

    private static KafkaFieldValueProvider findValueProvider(Set<KafkaFieldValueProvider> providers, KafkaColumnHandle handle)
    {
        for (KafkaFieldValueProvider provider : providers) {
            if (provider.accept(handle)) {
                return provider;
            }
        }
        return null;
    }

    public static void checkValue(Set<KafkaFieldValueProvider> providers, KafkaColumnHandle handle, String value)
    {
        KafkaFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(new String(provider.getSlice().getBytes(), StandardCharsets.UTF_8), value);
    }

    public static void checkValue(Set<KafkaFieldValueProvider> providers, KafkaColumnHandle handle, long value)
    {
        KafkaFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getLong(), value);
    }

    public static void checkValue(Set<KafkaFieldValueProvider> providers, KafkaColumnHandle handle, double value)
    {
        KafkaFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getDouble(), value, 0.0001);
    }

    public static void checkValue(Set<KafkaFieldValueProvider> providers, KafkaColumnHandle handle, boolean value)
    {
        KafkaFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getBoolean(), value);
    }

    public static void checkIsNull(Set<KafkaFieldValueProvider> providers, KafkaColumnHandle handle)
    {
        KafkaFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertTrue(provider.isNull());
    }
}
