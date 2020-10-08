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
package com.facebook.presto.hive.metastore.glue;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGlueHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GlueHiveMetastoreConfig.class)
                .setGlueRegion(null)
                .setPinGlueClientToCurrentRegion(false)
                .setMaxGlueConnections(5)
                .setDefaultWarehouseDir(null)
                .setCatalogId(null)
                .setPartitionSegments(5)
                .setGetPartitionThreads(20)
                .setIamRole(null));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.glue.region", "us-east-1")
                .put("hive.metastore.glue.pin-client-to-current-region", "true")
                .put("hive.metastore.glue.max-connections", "10")
                .put("hive.metastore.glue.default-warehouse-dir", "/location")
                .put("hive.metastore.glue.catalogid", "0123456789")
                .put("hive.metastore.glue.partitions-segments", "10")
                .put("hive.metastore.glue.get-partition-threads", "42")
                .put("hive.metastore.glue.iam-role", "role")
                .build();

        GlueHiveMetastoreConfig expected = new GlueHiveMetastoreConfig()
                .setGlueRegion("us-east-1")
                .setPinGlueClientToCurrentRegion(true)
                .setMaxGlueConnections(10)
                .setDefaultWarehouseDir("/location")
                .setCatalogId("0123456789")
                .setPartitionSegments(10)
                .setGetPartitionThreads(42)
                .setIamRole("role");

        assertFullMapping(properties, expected);
    }
}
