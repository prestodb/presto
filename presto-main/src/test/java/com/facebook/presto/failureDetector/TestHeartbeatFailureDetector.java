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
package com.facebook.presto.failureDetector;

import com.facebook.presto.execution.QueryManagerConfig;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import org.testng.annotations.Test;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.discovery.client.ServiceTypes.serviceType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHeartbeatFailureDetector
{
    @Test
    public void testExcludesCurrentNode()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingJmxModule(),
                new TestingDiscoveryModule(),
                new TestingHttpServerModule(),
                new TraceTokenModule(),
                new JsonModule(),
                new JaxrsModule(),
                new FailureDetectorModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        bindConfig(binder).to(QueryManagerConfig.class);
                        discoveryBinder(binder).bindSelector("presto");
                        discoveryBinder(binder).bindHttpAnnouncement("presto");
                    }
                });

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .initialize();

        ServiceSelector selector = injector.getInstance(Key.get(ServiceSelector.class, serviceType("presto")));
        assertEquals(selector.selectAllServices().size(), 1);

        HeartbeatFailureDetector detector = injector.getInstance(HeartbeatFailureDetector.class);
        detector.updateMonitoredServices();

        assertEquals(detector.getTotalCount(), 0);
        assertEquals(detector.getActiveCount(), 0);
        assertEquals(detector.getFailedCount(), 0);
        assertTrue(detector.getFailed().isEmpty());
    }
}
