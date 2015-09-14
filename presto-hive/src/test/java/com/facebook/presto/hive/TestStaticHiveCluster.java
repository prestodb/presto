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
package com.facebook.presto.hive;

import com.facebook.presto.spi.PrestoException;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransport;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.testing.Assertions.assertContains;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestStaticHiveCluster
{
    private static final HiveMetastoreClient DEFAULT_CLIENT = new HiveMetastoreClient((TTransport) null);
    private static final HiveMetastoreClient FALLBACK_CLIENT = new HiveMetastoreClient((TTransport) null);

    private static final StaticMetastoreConfig CONFIG_WITH_FALLBACK = new StaticMetastoreConfig().setMetastoreUris("thrift://default:8080,thrift://fallback:8090,thrift://fallback2:8090");
    private static final StaticMetastoreConfig CONFIG_WITHOUT_FALLBACK = new StaticMetastoreConfig().setMetastoreUris("thrift://default:8080");

    @Test
    public void testDefaultHiveMetastore()
    {
        MockHiveMetastoreClientFactory clientFactory = new MockHiveMetastoreClientFactory(null, new Duration(1, TimeUnit.SECONDS), newArrayList(DEFAULT_CLIENT));
        StaticHiveCluster hiveCluster = new StaticHiveCluster(CONFIG_WITH_FALLBACK, clientFactory);
        HiveMetastoreClient metastoreClient = hiveCluster.createMetastoreClient();
        assertEquals(metastoreClient, DEFAULT_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastore()
    {
        MockHiveMetastoreClientFactory clientFactory = new MockHiveMetastoreClientFactory(null, new Duration(1, TimeUnit.SECONDS),
                newArrayList(null, null, FALLBACK_CLIENT));
        StaticHiveCluster hiveCluster = new StaticHiveCluster(CONFIG_WITH_FALLBACK, clientFactory);
        HiveMetastoreClient metastoreClient = hiveCluster.createMetastoreClient();
        assertEquals(metastoreClient, FALLBACK_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastoreFails()
    {
        int maxMetastoreClients = 1000;
        ArrayList<HiveMetastoreClient> toReturn = newArrayList();
        for (int i = 0; i < maxMetastoreClients; i++) {
            toReturn.add(null);
        }
        MockHiveMetastoreClientFactory clientFactory = new MockHiveMetastoreClientFactory(null, new Duration(1, TimeUnit.SECONDS),
                toReturn);
        StaticHiveCluster hiveCluster = new StaticHiveCluster(CONFIG_WITH_FALLBACK, clientFactory);
        try {
            HiveMetastoreClient metastoreClient = hiveCluster.createMetastoreClient();
        }
        catch (PrestoException e) {
            String errorMessage = e.getMessage();
            assertTrue(errorMessage.contains("Unable to connect to metastore: [default:8080, fallback:8090, fallback2:8090]"));
            return;
        }
        fail();
    }

    @Test
    public void testMetastoreFailedWithoutFallback()
    {
        MockHiveMetastoreClientFactory clientFactory = new MockHiveMetastoreClientFactory(null, new Duration(1, TimeUnit.SECONDS),
                newArrayList((HiveMetastoreClient) null));
        StaticHiveCluster hiveCluster = new StaticHiveCluster(CONFIG_WITHOUT_FALLBACK, clientFactory);
        try {
            HiveMetastoreClient metastoreClient = hiveCluster.createMetastoreClient();
        }
        catch (PrestoException e) {
            assertContains(e.getMessage(), "Unable to connect to metastore: [default:8080]");
            return;
        }
        fail();
    }
}
