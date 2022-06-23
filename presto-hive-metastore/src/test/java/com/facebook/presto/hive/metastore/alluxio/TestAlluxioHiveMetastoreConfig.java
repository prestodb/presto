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
package com.facebook.presto.hive.metastore.alluxio;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAlluxioHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AlluxioHiveMetastoreConfig.class)
                .setMasterAddress(null)
                .setZookeeperEnabled(false)
                .setZookeeperAddress(null));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.alluxio.master.address", "localhost:19998")
                .put("hive.metastore.alluxio.zookeeper.enabled", "true")
                .put("hive.metastore.alluxio.zookeeper.address", "zK1:218,ZK2:2181,ZK3:2181")
                .build();

        AlluxioHiveMetastoreConfig expected = new AlluxioHiveMetastoreConfig()
                .setMasterAddress("localhost:19998")
                .setZookeeperEnabled(true)
                .setZookeeperAddress("zK1:218,ZK2:2181,ZK3:2181");

        assertFullMapping(properties, expected);
    }
}
