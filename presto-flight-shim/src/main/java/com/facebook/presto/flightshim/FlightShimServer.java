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

    public static FlightServer start(FlightServer.Builder builder, Module... extraModules)
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new FlightShimModule())
                .add(extraModules)
                .build());

        Logger log = Logger.get(FlightShimModule.class);
        FlightServer server;
        try {
            Injector injector = app.initialize();

            FlightShimPluginManager pluginManager = injector.getInstance(FlightShimPluginManager.class);
            pluginManager.loadPlugins();
            pluginManager.loadCatalogs();

            FlightShimProducer producer = injector.getInstance(FlightShimProducer.class);
            builder.producer(producer);

            server = builder.build();
            server.start();
            log.info(format("======== Flight Connector Server started on port: %s ========", server.getPort()));
            return server;
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
            return null;
        }
    }

    public static FlightServer.Builder builder(BufferAllocator allocator, Location location)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(location, "location is null");
        return FlightServer.builder().allocator(allocator).location(location);
    }

    public static void main(String[] args)
    {
        // TODO
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Location location = Location.forGrpcTls("localhost", 9431);
        start(builder(allocator, location));
    }
}


