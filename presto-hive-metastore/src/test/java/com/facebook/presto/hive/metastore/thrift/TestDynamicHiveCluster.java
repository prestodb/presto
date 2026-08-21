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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.units.Duration;
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestDynamicHiveCluster
{
    private final String url = "http://localhost:5436/discover";
    private final String serviceHeaderName = "Rpc-Service";
    private final String callerHeaderName = "Rpc-Caller";
    private final String serviceHeaderValue = "hms-random";
    private final String callerHeaderValue = "presto";
    private Map<String, String> headers = new HashMap<>();
    private final HiveMetastoreClient metastoreClient = createFakeMetastoreClient();
    private ThriftMetastoreHttpRequestDetails metastoreDiscoveryUri = new ThriftMetastoreHttpRequestDetails(url, headers);
    private ThriftMetastoreHttpRequestDetails emptyMetastoreDiscoveryUri = new ThriftMetastoreHttpRequestDetails("", headers);
    private final DynamicMetastoreConfig validConfig = new DynamicMetastoreConfig()
            .setMetastoreDiscoveryUri(metastoreDiscoveryUri);
    private final DynamicMetastoreConfig invalidConfig = new DynamicMetastoreConfig()
            .setMetastoreDiscoveryUri(emptyMetastoreDiscoveryUri);

    public TestDynamicHiveCluster() throws TException
    {
        headers.put(serviceHeaderName, serviceHeaderValue);
        headers.put(callerHeaderName, callerHeaderValue);
    }

    @Test
    public void testValidHiveMetastore()
            throws TException
    {
        HiveCluster cluster = createHiveCluster(validConfig, asList(metastoreClient));
        assertEquals(cluster.createMetastoreClient(), metastoreClient);
    }

    @Test
    public void testEmptyDiscoveryUriHiveMetastore() throws TException
    {
        assertThrows(IllegalStateException.class, () -> {
            createHiveCluster(invalidConfig, asList(metastoreClient));
        });
    }

    @Test
    public void testEmptyHostHiveMetastore() throws TException
    {
        HiveCluster cluster = createHiveClusterWithEmptyHost(validConfig, asList(metastoreClient));
        assertThrows(NullPointerException.class, () -> {
            cluster.createMetastoreClient();
        });
    }

    @Test
    public void testMalformedHostHiveMetastore() throws TException
    {
        HiveCluster cluster = createHiveClusterWithBadHost(validConfig, asList(metastoreClient));
        assertThrows(IllegalArgumentException.class, () -> {
            cluster.createMetastoreClient();
        });
    }

    @Test
    public void testInvalidHostHiveMetastore() throws TException
    {
        HiveCluster cluster = createHiveClusterWithInvalidHost(validConfig, asList(metastoreClient));
        assertThrows(IllegalArgumentException.class, () -> {
            cluster.createMetastoreClient();
        });
    }

    private HiveCluster createHiveCluster(DynamicMetastoreConfig config, List<HiveMetastoreClient> clients)
    {
        return new DynamicHiveCluster(config, new MockHiveMetastoreClientFactory(Optional.empty(), new Duration(1, SECONDS), clients), new JettyHttpClient(), new TestMetastoreUriFetcher());
    }

    private HiveCluster createHiveClusterWithEmptyHost(DynamicMetastoreConfig config, List<HiveMetastoreClient> clients)
    {
        return new DynamicHiveCluster(config, new MockHiveMetastoreClientFactory(Optional.empty(), new Duration(1, SECONDS), clients), new JettyHttpClient(), new TestEmptyMetastoreUriFetcher());
    }

    private HiveCluster createHiveClusterWithBadHost(DynamicMetastoreConfig config, List<HiveMetastoreClient> clients)
    {
        return new DynamicHiveCluster(config, new MockHiveMetastoreClientFactory(Optional.empty(), new Duration(1, SECONDS), clients), new JettyHttpClient(), new TestMalformedMetastoreUriFetcher());
    }

    private HiveCluster createHiveClusterWithInvalidHost(DynamicMetastoreConfig config, List<HiveMetastoreClient> clients)
    {
        return new DynamicHiveCluster(config, new MockHiveMetastoreClientFactory(Optional.empty(), new Duration(1, SECONDS), clients), new JettyHttpClient(), new TestInvalidMetastoreUriFetcher());
    }

    private HiveMetastoreClient createFakeMetastoreClient()
    {
        return new MockHiveMetastoreClient();
    }

    private class TestMetastoreUriFetcher
            extends ThriftMetastoreUriFetcher
    {
        @Override
        public URI getMetastoreUri(HttpClient httpClient, Request request) throws TException
        {
            return URI.create("thrift://localhost:9083");
        }
    }

    private class TestEmptyMetastoreUriFetcher
            extends ThriftMetastoreUriFetcher
    {
        @Override
        public URI getMetastoreUri(HttpClient httpClient, Request request) throws TException
        {
            return null;
        }
    }

    private class TestInvalidMetastoreUriFetcher
            extends ThriftMetastoreUriFetcher
    {
        @Override
        public URI getMetastoreUri(HttpClient httpClient, Request request) throws TException
        {
            return URI.create("http://localhost:9083");
        }
    }

    private class TestMalformedMetastoreUriFetcher
            extends ThriftMetastoreUriFetcher
    {
        @Override
        public URI getMetastoreUri(HttpClient httpClient, Request request) throws TException
        {
            return URI.create("not_a_valid_uri");
        }
    }
}
