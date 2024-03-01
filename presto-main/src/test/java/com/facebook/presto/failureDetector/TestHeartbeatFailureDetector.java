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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.discovery.client.ServiceSelector;
import com.facebook.airlift.discovery.client.testing.TestingDiscoveryModule;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.jmx.testing.TestingJmxModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.airlift.tracetoken.TraceTokenModule;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.failureDetector.HeartbeatFailureDetector.Stats;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.testng.annotations.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.net.SocketTimeoutException;
import java.net.URI;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.facebook.airlift.discovery.client.ServiceTypes.serviceType;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
                new JaxrsModule(true),
                new FailureDetectorModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        configBinder(binder).bindConfig(InternalCommunicationConfig.class);
                        configBinder(binder).bindConfig(QueryManagerConfig.class);
                        discoveryBinder(binder).bindSelector("presto");
                        discoveryBinder(binder).bindHttpAnnouncement("presto");

                        // Jersey with jetty 9 requires at least one resource
                        // todo add a dummy resource to airlift jaxrs in this case
                        jaxrsBinder(binder).bind(FooResource.class);
                    }
                });

        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
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

    @Test
    public void testHeartbeatStatsSerialization()
            throws Exception
    {
        ObjectMapper objectMapper = new JsonObjectMapperProvider().get();
        Stats stats = new Stats(new URI("http://example.com"), 60);
        String serialized = objectMapper.writeValueAsString(stats);
        JsonNode deserialized = objectMapper.readTree(serialized);
        assertFalse(deserialized.has("lastFailureInfo"));

        stats.recordFailure(new SocketTimeoutException("timeout"));
        serialized = objectMapper.writeValueAsString(stats);
        deserialized = objectMapper.readTree(serialized);
        assertFalse(deserialized.get("lastFailureInfo").isNull());
        assertEquals(deserialized.get("lastFailureInfo").get("type").asText(), SocketTimeoutException.class.getName());
    }

    @Path("/foo")
    public static class FooResource
    {
        @GET
        public static String hello()
        {
            return "hello";
        }
    }
}
