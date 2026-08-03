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
package com.facebook.presto.flightshim;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.ObjectNames;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestFlightShimJmxMetrics
{
    private TestingMBeanServer mbeanServer;
    private Injector injector;

    @BeforeMethod
    public void setUp()
    {
        mbeanServer = new TestingMBeanServer();
        injector = FlightShimServer.initialize(
                ImmutableMap.of(
                        "flight-shim.server", "localhost",
                        "flight-shim.server.port", "9999"),
                binder -> binder.bind(MBeanServer.class).toInstance(mbeanServer));
    }

    @AfterMethod
    public void tearDown()
    {
        injector.getInstance(FlightShimConnectorStatsManager.class).destroy();
    }

    @Test
    public void testFlightShimStatsExported()
            throws Exception
    {
        FlightShimStats stats = injector.getInstance(FlightShimStats.class);
        ObjectName objectName = new ObjectName(ObjectNames.generatedNameOf(FlightShimStats.class));

        stats.recordStreamStarted();
        assertEquals(mbeanServer.getAttribute(objectName, "ActiveStreams"), 1);

        stats.recordBatchShipped(100, 4096);
        stats.recordStreamFinished();
        stats.recordStreamCompleted(1_000_000);

        assertEquals(mbeanServer.getAttribute(objectName, "ActiveStreams"), 0);
        assertEquals(mbeanServer.getAttribute(objectName, "StreamsCompleted"), 1L);
        assertEquals(mbeanServer.getAttribute(objectName, "RowsShipped"), 100L);
        assertEquals(mbeanServer.getAttribute(objectName, "BatchesShipped"), 1L);
        assertEquals(mbeanServer.getAttribute(objectName, "BytesShipped"), 4096L);
    }

    @Test
    public void testFlightShimConnectorStatsExported()
            throws Exception
    {
        FlightShimConnectorStatsManager connectorStatsManager = injector.getInstance(FlightShimConnectorStatsManager.class);
        FlightShimConnectorStats connectorStats = connectorStatsManager.getOrCreate("mysql");

        String objectName = ObjectNames.builder(FlightShimConnectorStats.class, "mysql").build();

        connectorStats.recordStreamStarted();
        connectorStats.recordBatchShipped(50);
        connectorStats.recordStreamFinished();
        connectorStats.recordStreamCompleted(500_000);

        assertEquals(mbeanServer.getAttribute(new ObjectName(objectName), "ActiveStreams"), 0);
        assertEquals(mbeanServer.getAttribute(new ObjectName(objectName), "StreamsCompleted"), 1L);
        assertEquals(mbeanServer.getAttribute(new ObjectName(objectName), "RowsShipped"), 50L);
        assertEquals(mbeanServer.getAttribute(new ObjectName(objectName), "BatchesShipped"), 1L);
        assertEquals(mbeanServer.getAttribute(new ObjectName(objectName), "ConnectorId"), "mysql");
    }
}
