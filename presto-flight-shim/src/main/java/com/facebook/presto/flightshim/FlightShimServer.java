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
import com.google.inject.Module;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FlightShimServer
{
    private FlightShimServer()
    {
    }

    public static void start(Module... extraModules)
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new FlightShimModule())
                .add(extraModules)
                .build());

        Logger log = Logger.get(FlightShimModule.class);
        try {
            Injector injector = app.initialize();
            FlightServer server = setupServer(FlightServer.builder(), injector).build();
            server.start();
            log.info(format("======== Flight Connector Server started on port: %s ========", server.getPort()));
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
    }

    public static FlightServer.Builder setupServer(FlightServer.Builder builder, Injector injector)
            throws Exception
    {
        FlightShimPluginManager pluginManager = injector.getInstance(FlightShimPluginManager.class);
        pluginManager.loadPlugins();
        pluginManager.loadCatalogs();

        builder.allocator(injector.getInstance(BufferAllocator.class));
        FlightShimConfig config = injector.getInstance(FlightShimConfig.class);
        if (config.getArrowFlightServerSslEnabled()) {
            builder.location(Location.forGrpcTls(config.getFlightServerName(), config.getArrowFlightPort()));
        } else {
            builder.location(Location.forGrpcInsecure(config.getFlightServerName(), config.getArrowFlightPort()));
        }

        FlightShimProducer producer = injector.getInstance(FlightShimProducer.class);
        builder.producer(producer);

        return builder;
    }

    public void shutdown()
    {
        // TODO graceful shutdown and close allocator

    }

    public static void main(String[] args)
    {
        start();
    }
}


