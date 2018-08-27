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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Assertions.assertContains;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestStaticHiveCluster
{
    private static final HiveMetastoreClient DEFAULT_CLIENT = createFakeMetastoreClient();
    private static final HiveMetastoreClient FALLBACK_CLIENT = createFakeMetastoreClient();

    private static final String DEFAULT_URI = "thrift://default:8080";
    private static final String FALLBACK_URI = "thrift://fallback:8090";
    private static final String FALLBACK2_URI = "thrift://fallback2:8090";

    private static final StaticMetastoreConfig CONFIG_WITH_FALLBACK = new StaticMetastoreConfig()
            .setMetastoreUris(Joiner.on(',').join(DEFAULT_URI, FALLBACK_URI, FALLBACK2_URI));

    private static final StaticMetastoreConfig CONFIG_WITHOUT_FALLBACK = new StaticMetastoreConfig()
            .setMetastoreUris(DEFAULT_URI);

    private static final StaticMetastoreConfig CONFIG_WITH_FALLBACK_WITH_USER = new StaticMetastoreConfig()
            .setMetastoreUris(Joiner.on(',').join(DEFAULT_URI, FALLBACK_URI, FALLBACK2_URI))
            .setMetastoreUsername("presto");

    private static final StaticMetastoreConfig CONFIG_WITHOUT_FALLBACK_WITH_USER = new StaticMetastoreConfig()
            .setMetastoreUris(DEFAULT_URI)
            .setMetastoreUsername("presto");

    private static final Map<String, Optional<HiveMetastoreClient>> CLIENTS = ImmutableMap.of(DEFAULT_URI, Optional.of(DEFAULT_CLIENT), FALLBACK_URI, Optional.of(FALLBACK_CLIENT));

    @Test
    public void testDefaultHiveMetastore()
            throws TException
    {
        HiveCluster cluster = createHiveCluster(CONFIG_WITH_FALLBACK, ImmutableMap.of(DEFAULT_URI, Optional.of(DEFAULT_CLIENT)));
        assertEqualHiveClient(cluster.createMetastoreClient(), DEFAULT_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastore()
            throws TException
    {
        HiveCluster cluster = createHiveCluster(CONFIG_WITH_FALLBACK, ImmutableMap.of(DEFAULT_URI, Optional.empty(), FALLBACK_URI, Optional.of(FALLBACK_CLIENT)));
        assertEqualHiveClient(cluster.createMetastoreClient(), FALLBACK_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastoreFails()
    {
        HiveCluster cluster = createHiveCluster(CONFIG_WITH_FALLBACK, ImmutableMap.of());
        assertCreateClientFails(cluster, "Failed connecting to Hive metastore: [default:8080, fallback:8090, fallback2:8090]");
    }

    @Test
    public void testMetastoreFailedWithoutFallback()
    {
        HiveCluster cluster = createHiveCluster(CONFIG_WITHOUT_FALLBACK, ImmutableMap.of(DEFAULT_URI, Optional.empty()));
        assertCreateClientFails(cluster, "Failed connecting to Hive metastore: [default:8080]");
    }

    @Test
    public void testFallbackHiveMetastoreWithHiveUser()
            throws TException
    {
        HiveCluster cluster = createHiveCluster(CONFIG_WITH_FALLBACK_WITH_USER, ImmutableMap.of(DEFAULT_URI, Optional.empty(), FALLBACK_URI, Optional.empty(), FALLBACK2_URI, Optional.of(FALLBACK_CLIENT)));
        assertEqualHiveClient(cluster.createMetastoreClient(), FALLBACK_CLIENT);
    }

    @Test
    public void testMetastoreFailedWithoutFallbackWithHiveUser()
    {
        HiveCluster cluster = createHiveCluster(CONFIG_WITHOUT_FALLBACK_WITH_USER, ImmutableMap.of(DEFAULT_URI, Optional.empty()));
        assertCreateClientFails(cluster, "Failed connecting to Hive metastore: [default:8080]");
    }

    @Test
    public void testFallbackHiveMetastoreOnTimeOutException()
            throws TException
    {
        HiveCluster cluster = createHiveCluster(CONFIG_WITH_FALLBACK, CLIENTS);

        HiveMetastoreClient metastoreClient1 = cluster.createMetastoreClient();
        assertEqualHiveClient(metastoreClient1, DEFAULT_CLIENT);

        HiveMetastoreClient metastoreClient2 = cluster.createMetastoreClient();
        assertEqualHiveClient(metastoreClient2, DEFAULT_CLIENT);

        assertGetTableException(metastoreClient1);
        assertGetTableException(metastoreClient2);

        HiveMetastoreClient metastoreClient3 = cluster.createMetastoreClient();
        assertEqualHiveClient(metastoreClient3, FALLBACK_CLIENT);

        assertGetTableException(metastoreClient3);

        HiveMetastoreClient metastoreClient = cluster.createMetastoreClient();
        assertEqualHiveClient(metastoreClient, DEFAULT_CLIENT);
    }

    private static void assertGetTableException(HiveMetastoreClient client)
    {
        try {
            client.getTable("foo", "bar");
        }
        catch (TException e) {
            assertContains(e.getMessage(), "Read timeout");
        }
    }

    private static void assertCreateClientFails(HiveCluster cluster, String message)
    {
        try {
            cluster.createMetastoreClient();
            fail("expected exception");
        }
        catch (TException e) {
            assertContains(e.getMessage(), message);
        }
    }

    private static HiveCluster createHiveCluster(StaticMetastoreConfig config, Map<String, Optional<HiveMetastoreClient>> clients)
    {
        return new StaticHiveCluster(config, new MockHiveMetastoreClientFactory(Optional.empty(), new Duration(1, SECONDS), clients));
    }

    private static HiveMetastoreClient createFakeMetastoreClient()
    {
        return new MockHiveMetastoreClient()
        {
            @Override
            public Table getTable(String dbName, String tableName)
                    throws TException
            {
                throw new TException(new SocketTimeoutException("Read timeout"));
            }
        };
    }

    private void assertEqualHiveClient(HiveMetastoreClient actual, HiveMetastoreClient expected)
    {
        if (actual instanceof FailureAwareHiveMetaStoreClient) {
            actual = ((FailureAwareHiveMetaStoreClient) actual).getDelegate();
        }
        if (expected instanceof FailureAwareHiveMetaStoreClient) {
            expected = ((FailureAwareHiveMetaStoreClient) expected).getDelegate();
        }
        assertEquals(actual, expected);
    }
}
