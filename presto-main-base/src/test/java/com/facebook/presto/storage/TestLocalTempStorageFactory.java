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
package com.facebook.presto.storage;

import com.facebook.presto.spi.storage.TempStorage;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestLocalTempStorageFactory
{
    @Test
    public void testFactoryName()
    {
        LocalTempStorageFactory factory = new LocalTempStorageFactory();
        assertEquals(factory.getName(), "local");
    }

    @Test
    public void testCreateWithValidConfig()
    {
        LocalTempStorageFactory factory = new LocalTempStorageFactory();

        String tempDir = System.getProperty("java.io.tmpdir");
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("temp-storage.path", tempDir)
                .put("temp-storage.max-used-space-threshold", "0.95")
                .build();

        TempStorage storage = factory.create(config, null);

        assertNotNull(storage);
        assertEquals(storage.getClass(), LocalTempStorage.class);
    }

    @Test
    public void testCreateWithMultiplePaths()
    {
        LocalTempStorageFactory factory = new LocalTempStorageFactory();

        String tempDir = System.getProperty("java.io.tmpdir");
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("temp-storage.path", tempDir + "," + tempDir)
                .put("temp-storage.max-used-space-threshold", "1.0")
                .build();

        TempStorage storage = factory.create(config, null);

        assertNotNull(storage);
        assertEquals(storage.getClass(), LocalTempStorage.class);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testCreateWithMissingPath()
    {
        LocalTempStorageFactory factory = new LocalTempStorageFactory();

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("temp-storage.max-used-space-threshold", "0.9")
                .build();

        factory.create(config, null);
    }
}
