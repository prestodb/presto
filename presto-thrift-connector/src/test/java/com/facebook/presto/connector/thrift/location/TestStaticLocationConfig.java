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
package com.facebook.presto.connector.thrift.location;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

public class TestStaticLocationConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(StaticLocationConfig.class)
                .setHosts(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("static-location.hosts", "localhost:7777,localhost:7779")
                .build();

        StaticLocationConfig expected = new StaticLocationConfig()
                .setHosts(HostList.of(
                        HostAddress.fromParts("localhost", 7777),
                        HostAddress.fromParts("localhost", 7779)));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
