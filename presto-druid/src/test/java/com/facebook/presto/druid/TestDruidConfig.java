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
package com.facebook.presto.druid;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDruidConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DruidConfig.class)
                .setDruidBrokerUrl(null)
                .setDruidCoordinatorUrl(null)
                .setDruidSchema("druid")
                .setComputePushdownEnabled(false)
                .setHadoopConfiguration("")
                .setDruidAuthenticationType(null)
                .setUsername(null)
                .setPassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("druid.broker-url", "http://druid.broker:1234")
                .put("druid.coordinator-url", "http://druid.coordinator:4321")
                .put("druid.schema-name", "test")
                .put("druid.compute-pushdown-enabled", "true")
                .put("druid.hadoop.config.resources", "/etc/core-site.xml,/etc/hdfs-site.xml")
                .put("druid.authentication.type", "BASIC")
                .put("druid.authentication.username", "http_basic_username")
                .put("druid.authentication.password", "http_basic_password")
                .build();

        DruidConfig expected = new DruidConfig()
                .setDruidBrokerUrl("http://druid.broker:1234")
                .setDruidCoordinatorUrl("http://druid.coordinator:4321")
                .setDruidSchema("test")
                .setComputePushdownEnabled(true)
                .setHadoopConfiguration(ImmutableList.of("/etc/core-site.xml", "/etc/hdfs-site.xml"))
                .setDruidAuthenticationType(DruidConfig.DruidAuthenticationType.BASIC)
                .setUsername("http_basic_username")
                .setPassword("http_basic_password");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
