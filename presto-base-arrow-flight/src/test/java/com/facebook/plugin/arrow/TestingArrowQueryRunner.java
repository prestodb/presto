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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestingArrowQueryRunner
{
    private static DistributedQueryRunner queryRunner;
    private static final Logger logger = Logger.get(TestingArrowQueryRunner.class);
    private TestingArrowQueryRunner()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static DistributedQueryRunner createQueryRunner() throws Exception
    {
        if (queryRunner == null) {
            queryRunner = createQueryRunner(ImmutableMap.of(), TestingArrowFactory.class);
        }
        return queryRunner;
    }

    private static DistributedQueryRunner createQueryRunner(Map<String, String> catalogProperties, Class<? extends TestingArrowFactory> factoryClass) throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("arrow")
                .setSchema("testdb")
                .build();

        if (queryRunner == null) {
            queryRunner = DistributedQueryRunner.builder(session).build();
        }

        try {
            String connectorName = "arrow";
            queryRunner.installPlugin(new ArrowPlugin(connectorName, new TestingArrowModule()));

            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("arrow-flight.server", "127.0.0.1")
                    .put("arrow-flight.server-ssl-enabled", "true")
                    .put("arrow-flight.server.port", "9443")
                    .put("arrow-flight.server.verify", "false");

            queryRunner.createCatalog(connectorName, connectorName, properties.build());

            return queryRunner;
        }
        catch (Exception e) {
            logger.error(e);
            throw new RuntimeException("Failed to create ArrowQueryRunner", e);
        }
    }
}
