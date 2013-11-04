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
package com.facebook.presto.server;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.server.DiscoveryServerModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.testing.FileUtils;
import org.weakref.jmx.guice.MBeanModule;

import java.io.Closeable;
import java.io.File;
import java.net.URI;
import java.util.Map;

public class TestingDiscoveryServer
        implements Closeable
{
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;
    private final File tempDir;

    public TestingDiscoveryServer(String environment)
            throws Exception
    {
        tempDir = Files.createTempDir();

        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("static.db.location", tempDir.getAbsolutePath())
                .build();

        Bootstrap app = new Bootstrap(
                new MBeanModule(),
                new TestingNodeModule(environment),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new DiscoveryServerModule(),
                new DiscoveryModule(),
                new TestingJmxModule());

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties)
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        server = injector.getInstance(TestingHttpServer.class);
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    @Override
    public void close()
    {
        try {
            if (lifeCycleManager != null) {
                lifeCycleManager.stop();
            }
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        finally {
            FileUtils.deleteRecursively(tempDir);
        }
    }
}
