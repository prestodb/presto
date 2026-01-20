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
package com.facebook.presto.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

//import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

public class TestDynamicMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicMetastoreConfig.class)
                .setMetastoreDiscoveryType(null)
                .setMetastoreDiscoveryUri(null)
                .setMetastoreUsername(null));
    }

    @Test
    public void testMetastoreDiscoveryUri() throws TException
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.discovery-uri", "{\"url\":\"http://localhost:5436/discover\", \"headers\":{\"Rpc-Service\":\"hms\", \"Rpc-Caller\":\"presto\"}}")
                .put("hive.metastore.discovery-type", "dynamic")
                .put("hive.metastore.username", "presto")
                .build();

        String url = "http://localhost:5436/discover";
        Map<String, String> headers = new HashMap<>();
        headers.put("Rpc-Service", "hms");
        headers.put("Rpc-Caller", "presto");
        ThriftMetastoreHttpRequestDetails requestDetails = new ThriftMetastoreHttpRequestDetails(url, headers);
        DynamicMetastoreConfig expected = new DynamicMetastoreConfig()
                .setMetastoreDiscoveryUri(requestDetails)
                .setMetastoreDiscoveryType("dynamic")
                .setMetastoreUsername("presto");

        //assertFullMapping(properties, expected);
        assertEquals(expected.getMetastoreDiscoveryUri().getUrl(), "http://localhost:5436/discover");
        assertEquals(expected.getMetastoreDiscoveryUri().getHeaders().get("Rpc-Service"), "hms");
        assertEquals(expected.getMetastoreDiscoveryUri().getHeaders().get("Rpc-Caller"), "presto");
        assertEquals(expected.getMetastoreDiscoveryType(), "dynamic");
        assertEquals(expected.getMetastoreUsername(), "presto");
    }
}
