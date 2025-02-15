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
package org.apache.parquet.anonymization;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAnonymizationManagerFactory
{
    private Configuration conf;
    private static final String TEST_TABLE = "test_table";

    @BeforeClass
    public void setUp()
    {
        conf = new Configuration();
    }

    // Test valid implementation
    @Test
    public void testValidManagerCreation()
    {
        Optional<AnonymizationManager> manager = AnonymizationManagerFactory.getAnonymizationManager(
                ValidTestManager.class.getName(),
                conf,
                TEST_TABLE);

        assertTrue(manager.isPresent(), "Manager should be present");
        assertTrue(manager.get() instanceof ValidTestManager,
                "Manager should be instance of ValidTestManager");
    }

    @Test
    public void testNullManagerClass()
    {
        try {
            AnonymizationManagerFactory.getAnonymizationManager(null, conf, TEST_TABLE);
            fail("Expected IllegalStateException to be thrown");
        }
        catch (IllegalStateException e) {
            assertEquals("Failed to create anonymization manager:" +
                    " Anonymization manager class not specified", e.getMessage());
        }
    }

    @Test
    public void testEmptyManagerClass()
    {
        try {
            AnonymizationManagerFactory.getAnonymizationManager("  ", conf, TEST_TABLE);
            fail("Expected IllegalStateException to be thrown");
        }
        catch (IllegalStateException e) {
            assertEquals("Failed to create anonymization manager:" +
                    " Anonymization manager class not specified", e.getMessage());
        }
    }

    @Test
    public void testNonExistentManagerClass()
    {
        String nonExistentClass = "com.nonexistent.Manager";
        try {
            AnonymizationManagerFactory.getAnonymizationManager(
                    nonExistentClass,
                    conf,
                    TEST_TABLE);
            fail("Expected IllegalArgumentException to be thrown");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Anonymization manager class not found"));
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        }
    }

    @Test
    public void testInvalidManagerImplementation()
    {
        try {
            AnonymizationManagerFactory.getAnonymizationManager(
                    String.class.getName(),
                    conf,
                    TEST_TABLE);
            fail("Expected IllegalStateException to be thrown");
        }
        catch (IllegalStateException e) {
            assertEquals("Failed to create anonymization manager:" +
                            " Invalid anonymization manager class: " + String.class.getName(),
                    e.getMessage());
        }
    }

    @Test
    public void testFailingCreateMethod()
    {
        try {
            AnonymizationManagerFactory.getAnonymizationManager(
                    FailingTestManager.class.getName(),
                    conf,
                    TEST_TABLE);
            fail("Expected IllegalStateException to be thrown");
        }
        catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Failed to create anonymization manager"));
            assertTrue(e.getCause() instanceof RuntimeException);
        }
    }

    // Valid test implementation
    public static class ValidTestManager
            implements AnonymizationManager
    {
        public static ValidTestManager create(Configuration conf, String tableName)
        {
            return new ValidTestManager();
        }

        @Override
        public boolean shouldAnonymize(ColumnPath columnPath)
        {
            return false;
        }

        @Override
        public String anonymize(ColumnPath columnPath, String value)
        {
            return "";
        }
    }

    // Implementation with failing create method
    public static class FailingTestManager
            implements AnonymizationManager
    {
        public static FailingTestManager create(Configuration conf, String tableName)
        {
            throw new RuntimeException("Failed to create manager");
        }

        @Override
        public boolean shouldAnonymize(ColumnPath columnPath)
        {
            return false;
        }

        @Override
        public String anonymize(ColumnPath columnPath, String value)
        {
            return "";
        }
    }
}
