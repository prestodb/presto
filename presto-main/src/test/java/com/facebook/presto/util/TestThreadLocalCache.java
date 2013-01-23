package com.facebook.presto.util;

import org.testng.Assert;
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
        ThreadLocalCache<String, String> cache = new ThreadLocalCache<String, String>(2) {
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
}
