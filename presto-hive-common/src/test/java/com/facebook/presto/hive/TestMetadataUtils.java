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

import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.MetadataUtils.constructSchemaName;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestMetadataUtils
{
    @Test
    void testWithCatalogAndValidSchema()
    {
        String result = constructSchemaName(Optional.of("testCatalog"), "testSchema");
        assertTrue(result.equals("@testCatalog#testSchema"));
    }

    @Test
    void testWithCatalogAndDefaultSchema()
    {
        String result = constructSchemaName(Optional.of("testCatalog"), "default");
        assertTrue(result.equals("default"));
    }

    @Test
    void testWithCatalogAndSchemaContainingSeparator()
    {
        String result = constructSchemaName(Optional.of("testCatalog"), "schema#with#dot");
        assertTrue(result.equals("schema#with#dot"));
    }

    @Test
    void testWithoutCatalog()
    {
        String result = constructSchemaName(Optional.empty(), "testSchema");
        assertTrue(result.equals("testSchema"));
    }

    @Test
    void testWithNullSchema()
    {
        String result = constructSchemaName(Optional.empty(), null);
        assertNull(result);
    }

    @Test
    public void testConstructSchemaWithGivenCatalogName()
    {
        constructSchemaName(Optional.of("test_catalog"), "test_schema");
    }
}
