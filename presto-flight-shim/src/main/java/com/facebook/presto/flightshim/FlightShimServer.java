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
import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.ContextPropagatingExecutorService;
import org.apache.arrow.memory.BufferAllocator;

import java.io.File;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;

public class FlightShimServer
{
    private FlightShimServer()
    {
    }

    public static Injector initialize(Module... extraModules)
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new FlightShimModule())
                //.add(new ServerMainModule(new SqlParserOptions()))
                .add(extraModules)
                .build());

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
            // TODO mTLS
            builder.location(Location.forGrpcTls(config.getServerName(), config.getServerPort()));
            File certChainFile = new File(config.getServerSSLCertificateFile());
            File privateKeyFile = new File(config.getServerSSLKeyFile());
            builder.useTls(certChainFile, privateKeyFile);
        } else {
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
        Injector injector = initialize();
        // TODO load from file
        FlightShimConfig config = injector.getInstance(FlightShimConfig.class);
        config.setServerName("localhost");
        config.setServerPort(9443);
        config.setServerSslEnabled(true);
        config.setServerSSLCertificateFile("src/test/resources/server.crt");
        config.setServerSSLKeyFile("src/test/resources/server.key");
        /////////////////////
        try (FlightServer server = start(injector, FlightServer.builder());
             FlightShimProducer producer = injector.getInstance(FlightShimProducer.class)) {
            log.info(format("======== Flight Connector Server started on port: %s ========", server.getPort()));
            server.awaitTermination();
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
    }
}


