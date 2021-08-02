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
package com.facebook.presto.druid;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class DruidQueryRunner
{
    private DruidQueryRunner() {}

    private static final Logger log = Logger.get(DruidQueryRunner.class);
    private static final String DEFAULT_SOURCE = "test";
    private static final String DEFAULT_CATALOG = "druid";
    private static final String DEFAULT_SCHEMA = "druid";

    private static String broker = "http://localhost:8082";
    private static String coordinator = "http://localhost:8081";

    public static DistributedQueryRunner createDruidQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession()).build();
        try {
            queryRunner.installPlugin(new DruidPlugin());
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("druid.coordinator-url", coordinator);
            connectorProperties.putIfAbsent("druid.broker-url", broker);

            queryRunner.createCatalog(DEFAULT_CATALOG, "druid", connectorProperties);
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setSource(DEFAULT_SOURCE)
                .setCatalog(DEFAULT_CATALOG)
                .setSchema(DEFAULT_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createDruidQueryRunner(ImmutableMap.of());
        log.info(format("Presto server started: %s", queryRunner.getCoordinator().getBaseUrl()));
    }
}
