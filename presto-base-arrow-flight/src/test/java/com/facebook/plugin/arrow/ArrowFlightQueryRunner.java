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

package com.facebook.plugin.arrow;

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class ArrowFlightQueryRunner
{
    private ArrowFlightQueryRunner()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static DistributedQueryRunner createQueryRunner(int flightServerPort) throws Exception
    {
        return createQueryRunner(ImmutableMap.of("arrow-flight.server.port", String.valueOf(flightServerPort)));
    }

    private static DistributedQueryRunner createQueryRunner(Map<String, String> catalogProperties) throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("arrow")
                .setSchema("tpch")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        try {
            String connectorName = "arrow";
            queryRunner.installPlugin(new ArrowPlugin(connectorName, new TestingArrowModule()));

            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("arrow-flight.server", "127.0.0.1")
                    .put("arrow-flight.server-ssl-enabled", "true")
                    .put("arrow-flight.server.verify", "false");

            queryRunner.createCatalog(connectorName, connectorName, properties.build());

            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create ArrowQueryRunner", e);
        }
    }
}
