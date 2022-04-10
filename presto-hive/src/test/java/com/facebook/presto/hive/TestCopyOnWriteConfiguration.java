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
package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

public class TestCopyOnWriteConfiguration
{
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VALUE = "test-value";
    private static final String TEST_UPDATED_VALUE = "test-updated-value";
    private static final String TEST_KEY_INT = "test-key-int";
    private static final int TEST_VALUE_INT = 1;

    @Test
    public void testCopyOnWriteConfiguration()
    {
        Configuration originalConfig = new Configuration();
        originalConfig.set(TEST_KEY, TEST_VALUE);

        CopyOnWriteConfiguration copyOnWriteConfig = new CopyOnWriteConfiguration(originalConfig);

        // Assert the config remains same after a get call
        assertEquals(originalConfig.get(TEST_KEY), copyOnWriteConfig.get(TEST_KEY));
        assertEquals(TEST_VALUE, copyOnWriteConfig.get(TEST_KEY));
        assertSame(originalConfig, copyOnWriteConfig.getConfig());

        // Set the updated value
        copyOnWriteConfig.set(TEST_KEY, TEST_UPDATED_VALUE);

        // Assert value is different from the value in original config
        assertEquals(TEST_UPDATED_VALUE, copyOnWriteConfig.get(TEST_KEY));
        assertNotEquals(originalConfig.get(TEST_KEY), copyOnWriteConfig.get(TEST_KEY));

        // Assert that the config object inside the CopyOnWriteConfiguration has changed
        Configuration configAfterUpdate1 = copyOnWriteConfig.getConfig();
        assertNotSame(originalConfig, configAfterUpdate1);

        // Set a new key, value
        copyOnWriteConfig.setInt(TEST_KEY_INT, TEST_VALUE_INT);

        // Assert the config object after 2nd update is same as after 1st update
        Configuration configAfterUpdate2 = copyOnWriteConfig.getConfig();
        assertSame(configAfterUpdate1, configAfterUpdate2);
    }
}
