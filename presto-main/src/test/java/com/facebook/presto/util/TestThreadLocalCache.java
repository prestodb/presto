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
package com.facebook.presto.util;

import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class TestThreadLocalCache
{
    @Test
    public void testSanity()
            throws Exception
    {
        final AtomicInteger count = new AtomicInteger(0);
        ThreadLocalCache<String, String> cache = new ThreadLocalCache<String, String>(2)
        {
            @Override
            protected String load(String key)
            {
                // Concatenate key with counter
                return key + count.getAndAdd(1);
            }
        };

        // Load first key
        assertEquals(cache.get("abc"), "abc0");
        assertEquals(cache.get("abc"), "abc0");

        // Load second key
        assertEquals(cache.get("def"), "def1");

        // First key should still be there
        assertEquals(cache.get("abc"), "abc0");

        // Expire first key by exceeding max size
        assertEquals(cache.get("ghi"), "ghi2");

        // First key should now be regenerated
        assertEquals(cache.get("abc"), "abc3");

        // TODO: add tests for multiple threads
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "value must not be null")
    public void testDisallowsNulls()
    {
        new ThreadLocalCache<String, String>(10)
        {
            @Override
            protected String load(String key)
            {
                return null;
            }
        }.get("foo");
    }
}
