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
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.druid.DruidConfig.DruidAuthenticationType.BASIC;
import static com.facebook.presto.druid.DruidConfig.DruidAuthenticationType.NONE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

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
                .setDruidAuthenticationType(NONE)
                .setBasicAuthenticationUsername(null)
                .setBasicAuthenticationPassword(null)
                .setIngestionStoragePath(StandardSystemProperty.JAVA_IO_TMPDIR.value())
                .setCaseInsensitiveNameMatching(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES)));
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
                .put("druid.basic.authentication.username", "http_basic_username")
                .put("druid.basic.authentication.password", "http_basic_password")
                .put("druid.ingestion.storage.path", "hdfs://foo/bar/")
                .put("druid.case-insensitive-name-matching", "true")
                .put("druid.case-insensitive-name-matching.cache-ttl", "1s")
                .build();

        DruidConfig expected = new DruidConfig()
                .setDruidBrokerUrl("http://druid.broker:1234")
                .setDruidCoordinatorUrl("http://druid.coordinator:4321")
                .setDruidSchema("test")
                .setComputePushdownEnabled(true)
                .setHadoopConfiguration(ImmutableList.of("/etc/core-site.xml", "/etc/hdfs-site.xml"))
                .setDruidAuthenticationType(BASIC)
                .setBasicAuthenticationUsername("http_basic_username")
                .setBasicAuthenticationPassword("http_basic_password")
                .setIngestionStoragePath("hdfs://foo/bar/")
                .setCaseInsensitiveNameMatching(true)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
