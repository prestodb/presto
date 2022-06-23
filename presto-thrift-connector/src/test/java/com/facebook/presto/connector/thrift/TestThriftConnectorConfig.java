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
package com.facebook.presto.connector.thrift;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestThriftConnectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ThriftConnectorConfig.class)
                .setMaxResponseSize(new DataSize(16, MEGABYTE))
                .setMetadataRefreshThreads(1)
                .setLookupRequestsConcurrency(1)
                .setUseIdentityThriftHeader(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("presto-thrift.max-response-size", "2MB")
                .put("presto-thrift.metadata-refresh-threads", "10")
                .put("presto-thrift.lookup-requests-concurrency", "8")
                .put("presto-thrift.use-identity-thrift-headers", "true")
                .build();

        ThriftConnectorConfig expected = new ThriftConnectorConfig()
                .setMaxResponseSize(new DataSize(2, MEGABYTE))
                .setMetadataRefreshThreads(10)
                .setLookupRequestsConcurrency(8)
                .setUseIdentityThriftHeader(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
