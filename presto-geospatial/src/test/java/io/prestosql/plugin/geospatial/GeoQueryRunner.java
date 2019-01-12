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
package io.prestosql.plugin.geospatial;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.Map;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class GeoQueryRunner
{
    private static final int DEFAULT_WORKER_COUNT = 4;

    private GeoQueryRunner() {}

    private static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(testSessionBuilder().build(), DEFAULT_WORKER_COUNT, extraProperties);
        queryRunner.installPlugin(new GeoPlugin());
        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"));
        Logger log = Logger.get(GeoQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
