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
package com.facebook.presto.lance;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class LanceQueryRunner
{
    private static final Logger log = Logger.get(LanceQueryRunner.class);
    private static final String DEFAULT_SOURCE = "test";
    private static final String DEFAULT_CATALOG = "lance";
    private static final String DEFAULT_SCHEMA = "default";

    private LanceQueryRunner()
    {
    }

    public static DistributedQueryRunner createLanceQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();
        try {
            queryRunner.installPlugin(new LancePlugin());
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));

            // Use a temp directory for lance root
            Path tempDir = Files.createTempDirectory("lance-test");
            connectorProperties.putIfAbsent("lance.root-url", tempDir.toString());

            queryRunner.createCatalog(DEFAULT_CATALOG, "lance", connectorProperties);
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
        DistributedQueryRunner queryRunner = createLanceQueryRunner(ImmutableMap.of());
        log.info(format("Presto server started: %s", queryRunner.getCoordinator().getBaseUrl()));
    }
}
