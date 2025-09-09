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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.ContextPropagatingExecutorService;
import org.apache.arrow.memory.BufferAllocator;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;

public class FlightShimServer
{
    private FlightShimServer()
    {
    }

    public static Injector initialize(Map<String, String> config, Module... extraModules)
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new FlightShimModule())
                .add(new JsonModule())
                .add(extraModules)
                .build());

        if (config != null && !config.isEmpty()) {
            // Required config was provided instead of vm option -Dconfig=<path-to-config>
            app.setRequiredConfigurationProperties(config);
        }

        return app.initialize();
    }

    public static FlightServer start(Injector injector, FlightServer.Builder builder)
            throws Exception
    {
        FlightShimPluginManager pluginManager = injector.getInstance(FlightShimPluginManager.class);
        pluginManager.loadPlugins();
        pluginManager.loadCatalogs();

        builder.allocator(injector.getInstance(BufferAllocator.class));
        FlightShimConfig config = injector.getInstance(FlightShimConfig.class);

        if (config.getServerName() == null || config.getServerPort() == null) {
            throw new IllegalArgumentException("Required configuration 'flight-shim.server' and 'flight-shim.server.port' not set");
        }

        if (config.getServerSslEnabled()) {
            builder.location(Location.forGrpcTls(config.getServerName(), config.getServerPort()));
            if (config.getServerSSLCertificateFile() == null || config.getServerSSLKeyFile() == null) {
                throw new IllegalArgumentException("'flight-shim.server-ssl-enabled' is enabled but 'flight-shim.server-ssl-certificate-file' or 'flight-shim.server-ssl-key-file' not set");
            }
            File certChainFile = new File(config.getServerSSLCertificateFile());
            File privateKeyFile = new File(config.getServerSSLKeyFile());
            builder.useTls(certChainFile, privateKeyFile);

            // Check if client cert is provided for mTLS
            if (config.getClientSSLCertificateFile() != null) {
                File clientCertFile = new File(config.getClientSSLCertificateFile());
                builder.useMTlsClientVerification(clientCertFile);
            }
        }
        else {
            builder.location(Location.forGrpcInsecure(config.getServerName(), config.getServerPort()));
        }

        ExecutorService executor = injector.getInstance(Key.get(ExecutorService.class, ForFlightShimServer.class));
        builder.executor(new ContextPropagatingExecutorService(executor));

        FlightShimProducer producer = injector.getInstance(FlightShimProducer.class);
        builder.producer(producer);

        FlightServer server = builder.build();
        server.start();

        return server;
    }

    public static void main(String[] args)
    {
        Logger log = Logger.get(FlightShimModule.class);

        final Map<String, String> config;
        if (System.getProperty("config") == null) {
            log.info("FlightShim server using default config, override with -Dconfig=<path-to-config>");
            ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
            configBuilder.put("flight-shim.server", "localhost");
            configBuilder.put("flight-shim.server.port", String.valueOf(9443));
            configBuilder.put("flight-shim.server-ssl-certificate-file", "src/test/resources/certs/server.crt");
            configBuilder.put("flight-shim.server-ssl-key-file", "src/test/resources/certs/server.key");
            config = configBuilder.build();
        }
        else {
            log.info("FlightShim server using config from: " + System.getProperty("config"));
            config = ImmutableMap.of();
        }
        Injector injector = initialize(config);

        log.info("FlightShim server initializing");
        try (FlightServer server = start(injector, FlightServer.builder());
                FlightShimProducer producer = injector.getInstance(FlightShimProducer.class)) {
            log.info(format("======== FlightShim Server started on port: %s ========", server.getPort()));
            server.awaitTermination();
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
    }
}
