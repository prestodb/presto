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
package io.prestosql.plugin.thrift.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.drift.transport.netty.server.DriftNettyServerModule;
import io.airlift.log.Logger;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class ThriftTpchServer
{
    private ThriftTpchServer()
    {
    }

    public static void start(List<Module> extraModules)
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new DriftNettyServerModule())
                        .add(new ThriftTpchServerModule())
                        .addAll(requireNonNull(extraModules, "extraModules is null"))
                        .build());
        app.strictConfig().initialize();
    }

    public static void main(String[] args)
    {
        Logger log = Logger.get(ThriftTpchServer.class);
        try {
            start(ImmutableList.of());
            log.info("======== SERVER STARTED ========");
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
    }
}
