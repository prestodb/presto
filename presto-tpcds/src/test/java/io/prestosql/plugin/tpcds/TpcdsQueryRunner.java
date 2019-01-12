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
package io.prestosql.plugin.tpcds;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.Session;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.Map;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class TpcdsQueryRunner
{
    private TpcdsQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        return createQueryRunner(extraProperties, ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties, Map<String, String> coordinatorProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSource("test")
                .setCatalog("tpcds")
                .setSchema("sf1")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(4)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpcdsPlugin());
            queryRunner.createCatalog("tpcds", "tpcds");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"));
        Thread.sleep(10);
        Logger log = Logger.get(TpcdsQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
