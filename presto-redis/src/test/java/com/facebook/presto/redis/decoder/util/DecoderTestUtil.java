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
package com.facebook.presto.redis.decoder.util;

import com.facebook.presto.redis.RedisColumnHandle;
import com.facebook.presto.redis.RedisFieldValueProvider;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public final class DecoderTestUtil
{
    private DecoderTestUtil() {}

    private static RedisFieldValueProvider findValueProvider(Set<RedisFieldValueProvider> providers, RedisColumnHandle handle)
    {
        for (RedisFieldValueProvider provider : providers) {
            if (provider.accept(handle)) {
                return provider;
            }
        }
        return null;
    }

    public static void checkValue(Set<RedisFieldValueProvider> providers, RedisColumnHandle handle, String value)
    {
        RedisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(new String(provider.getSlice().getBytes(), StandardCharsets.UTF_8), value);
    }

    public static void checkValue(Set<RedisFieldValueProvider> providers, RedisColumnHandle handle, long value)
    {
        RedisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getLong(), value);
    }

    public static void checkValue(Set<RedisFieldValueProvider> providers, RedisColumnHandle handle, double value)
    {
        RedisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getDouble(), value, 0.0001);
    }

    public static void checkValue(Set<RedisFieldValueProvider> providers, RedisColumnHandle handle, boolean value)
    {
        RedisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertEquals(provider.getBoolean(), value);
    }

    public static void checkIsNull(Set<RedisFieldValueProvider> providers, RedisColumnHandle handle)
    {
        RedisFieldValueProvider provider = findValueProvider(providers, handle);
        assertNotNull(provider);
        assertTrue(provider.isNull());
    }
}
