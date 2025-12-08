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
package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBlackHoleCaseSensitivity
{
    private static final ConnectorSession SESSION = TestingConnectorSession.SESSION;

    @Test
    public void testBlackHoleClientConfigDefaults()
    {
        BlackHoleClientConfig config = new BlackHoleClientConfig();
        assertFalse(config.isCaseSensitiveNameMatching(),
                "Default should be case-insensitive");
    }

    @Test
    public void testBlackHoleClientConfigCaseSensitive()
    {
        BlackHoleClientConfig config = new BlackHoleClientConfig()
                .setCaseSensitiveNameMatching(true);

        assertTrue(config.isCaseSensitiveNameMatching(),
                "Should be case-sensitive when enabled");
    }

    @Test
    public void testBlackHoleClientConfigCaseInsensitive()
    {
        BlackHoleClientConfig config = new BlackHoleClientConfig()
                .setCaseSensitiveNameMatching(false);

        assertFalse(config.isCaseSensitiveNameMatching(),
                "Should be case-insensitive when disabled");
    }

    @Test
    public void testBlackHoleMetadataNormalizeIdentifierCaseSensitive()
    {
        BlackHoleClientConfig config = new BlackHoleClientConfig()
                .setCaseSensitiveNameMatching(true);
        BlackHoleMetadata metadata = new BlackHoleMetadata(config);

        // Case-sensitive: identifiers should remain unchanged
        assertEquals(metadata.normalizeIdentifier(SESSION, "MyTable"), "MyTable");
        assertEquals(metadata.normalizeIdentifier(SESSION, "myTable"), "myTable");
        assertEquals(metadata.normalizeIdentifier(SESSION, "MYTABLE"), "MYTABLE");
        assertEquals(metadata.normalizeIdentifier(SESSION, "MySchema"), "MySchema");
        assertEquals(metadata.normalizeIdentifier(SESSION, "Test_Table"), "Test_Table");
    }

    @Test
    public void testBlackHoleMetadataNormalizeIdentifierCaseInsensitive()
    {
        BlackHoleClientConfig config = new BlackHoleClientConfig()
                .setCaseSensitiveNameMatching(false);
        BlackHoleMetadata metadata = new BlackHoleMetadata(config);

        // Case-insensitive: identifiers should be lowercased
        assertEquals(metadata.normalizeIdentifier(SESSION, "MyTable"), "mytable");
        assertEquals(metadata.normalizeIdentifier(SESSION, "myTable"), "mytable");
        assertEquals(metadata.normalizeIdentifier(SESSION, "MYTABLE"), "mytable");
        assertEquals(metadata.normalizeIdentifier(SESSION, "MySchema"), "myschema");
        assertEquals(metadata.normalizeIdentifier(SESSION, "Test_Table"), "test_table");
    }

    @Test
    public void testBlackHoleMetadataDefaultBehavior()
    {
        // Default metadata should be case-insensitive
        BlackHoleMetadata metadata = new BlackHoleMetadata();

        assertEquals(metadata.normalizeIdentifier(SESSION, "MyTable"), "mytable");
        assertEquals(metadata.normalizeIdentifier(SESSION, "TestSchema"), "testschema");
        assertEquals(metadata.normalizeIdentifier(SESSION, "UPPERCASE"), "uppercase");
    }

    @Test
    public void testConfigIntegrationWithMetadata()
    {
        // Test that config changes are properly reflected in metadata behavior

        // Case-sensitive config
        BlackHoleClientConfig caseSensitiveConfig = new BlackHoleClientConfig()
                .setCaseSensitiveNameMatching(true);
        BlackHoleMetadata caseSensitiveMetadata = new BlackHoleMetadata(caseSensitiveConfig);

        // Case-insensitive config
        BlackHoleClientConfig caseInsensitiveConfig = new BlackHoleClientConfig()
                .setCaseSensitiveNameMatching(false);
        BlackHoleMetadata caseInsensitiveMetadata = new BlackHoleMetadata(caseInsensitiveConfig);

        String testIdentifier = "MyTestTable";

        assertEquals(caseSensitiveMetadata.normalizeIdentifier(SESSION, testIdentifier), "MyTestTable");
        assertEquals(caseInsensitiveMetadata.normalizeIdentifier(SESSION, testIdentifier), "mytesttable");
    }

    @Test
    public void testMultipleConfigInstances()
    {
        // Test that multiple config instances work independently
        BlackHoleClientConfig config1 = new BlackHoleClientConfig().setCaseSensitiveNameMatching(true);
        BlackHoleClientConfig config2 = new BlackHoleClientConfig().setCaseSensitiveNameMatching(false);

        assertTrue(config1.isCaseSensitiveNameMatching());
        assertFalse(config2.isCaseSensitiveNameMatching());

        // Test that they don't interfere with each other
        BlackHoleMetadata metadata1 = new BlackHoleMetadata(config1);
        BlackHoleMetadata metadata2 = new BlackHoleMetadata(config2);

        assertEquals(metadata1.normalizeIdentifier(SESSION, "TestCase"), "TestCase");
        assertEquals(metadata2.normalizeIdentifier(SESSION, "TestCase"), "testcase");
    }

    @Test
    public void testConfigChaining()
    {
        // Test that config setter returns the config instance for chaining
        BlackHoleClientConfig config = new BlackHoleClientConfig()
                .setCaseSensitiveNameMatching(true)
                .setCaseSensitiveNameMatching(false)
                .setCaseSensitiveNameMatching(true);

        assertTrue(config.isCaseSensitiveNameMatching(),
                "Final value should be true after chaining");
    }
}
