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
package com.facebook.presto.ducklake;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.ducklake.CatalogType.POSTGRESQL;

public class TestDuckLakeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DuckLakeConfig.class)
                .setCatalogType(null)
                .setMinimumAssignedSplitWeight(0.05));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("ducklake.catalog.type", "POSTGRESQL")
                .put("ducklake.minimum-assigned-split-weight", "0.01")
                .build();

        DuckLakeConfig expected = new DuckLakeConfig()
                .setCatalogType(POSTGRESQL)
                .setMinimumAssignedSplitWeight(0.01);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testCatalogConfigDefaults()
    {
        assertRecordedDefaults(recordDefaults(DuckLakeCatalogConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setSchema("public"));
    }

    @Test
    public void testCatalogConfigExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("ducklake.catalog.connection-url", "jdbc:postgresql://localhost:5432/ducklake_catalog")
                .put("ducklake.catalog.connection-user", "ducklake")
                .put("ducklake.catalog.connection-password", "secret")
                .put("ducklake.catalog.schema", "ducklake_meta")
                .build();

        DuckLakeCatalogConfig expected = new DuckLakeCatalogConfig()
                .setConnectionUrl("jdbc:postgresql://localhost:5432/ducklake_catalog")
                .setConnectionUser("ducklake")
                .setConnectionPassword("secret")
                .setSchema("ducklake_meta");

        assertFullMapping(properties, expected);
    }
}
