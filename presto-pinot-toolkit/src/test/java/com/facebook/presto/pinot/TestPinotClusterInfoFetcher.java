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

package com.facebook.presto.pinot;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.MediaType;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class TestPinotClusterInfoFetcher
{
    @Test
    public void testBrokersParsed()
    {
        HttpClient httpClient = new TestingHttpClient((request) -> TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, "{\n" +
                "  \"tableName\": \"dummy\",\n" +
                "  \"brokers\": [\n" +
                "    {\n" +
                "      \"tableType\": \"offline\",\n" +
                "      \"instances\": [\n" +
                "        \"Broker_dummy-broker-host1-datacenter1_6513\",\n" +
                "        \"Broker_dummy-broker-host2-datacenter1_6513\",\n" +
                "        \"Broker_dummy-broker-host4-datacenter1_6513\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tableType\": \"realtime\",\n" +
                "      \"instances\": [\n" +
                "        \"Broker_dummy-broker-host1-datacenter1_6513\",\n" +
                "        \"Broker_dummy-broker-host2-datacenter1_6513\",\n" +
                "        \"Broker_dummy-broker-host3-datacenter1_6513\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"server\": [\n" +
                "    {\n" +
                "      \"tableType\": \"offline\",\n" +
                "      \"instances\": [\n" +
                "        \"Server_dummy-server-host8-datacenter1_7090\",\n" +
                "        \"Server_dummy-server-host9-datacenter1_7090\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tableType\": \"realtime\",\n" +
                "      \"instances\": [\n" +
                "        \"Server_dummy-server-host7-datacenter1_7090\",\n" +
                "        \"Server_dummy-server-host4-datacenter1_7090\",\n" +
                "        \"Server_dummy-server-host5-datacenter1_7090\",\n" +
                "        \"Server_dummy-server-host6-datacenter1_7090\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}"));
        PinotConfig pinotConfig = new PinotConfig()
                .setMetadataCacheExpiry(new Duration(0, TimeUnit.MILLISECONDS))
                .setControllerUrls("localhost:7900");
        PinotClusterInfoFetcher pinotClusterInfoFetcher = new PinotClusterInfoFetcher(pinotConfig, new PinotMetrics(), httpClient, MetadataUtil.TABLES_JSON_CODEC, MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC, MetadataUtil.ROUTING_TABLES_JSON_CODEC, MetadataUtil.ROUTING_TABLES_V2_JSON_CODEC, MetadataUtil.TIME_BOUNDARY_JSON_CODEC);
        ImmutableSet<String> brokers = ImmutableSet.copyOf(pinotClusterInfoFetcher.getAllBrokersForTable("dummy"));
        Assert.assertEquals(ImmutableSet.of("dummy-broker-host1-datacenter1:6513", "dummy-broker-host2-datacenter1:6513", "dummy-broker-host3-datacenter1:6513", "dummy-broker-host4-datacenter1:6513"), brokers);
    }
}
