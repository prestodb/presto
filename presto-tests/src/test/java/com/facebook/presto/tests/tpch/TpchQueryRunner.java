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
package com.facebook.presto.tests.tpch;

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public final class TpchQueryRunner
{
    private Map<String, String> extraProperties = ImmutableMap.of();
    private Map<String, String> coordinatorProperties = ImmutableMap.of();
    private int nodeCount = 4;

    private TpchQueryRunner() {}

    public TpchQueryRunner setExtraProperties(Map<String, String> extraProperties)
    {
        this.extraProperties = extraProperties;
        return this;
    }

    public TpchQueryRunner setCoordinatorProperties(Map<String, String> coordinatorProperties)
    {
        this.coordinatorProperties = coordinatorProperties;
        return this;
    }

    public TpchQueryRunner setNodeCount(int nodeCount)
    {
        this.nodeCount = nodeCount;
        return this;
    }

    public DistributedQueryRunner build()
            throws Exception
    {
        DistributedQueryRunner queryRunner = buildWithoutCatalogs();
        try {
            queryRunner.createCatalog("tpch", "tpch");

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public DistributedQueryRunner buildWithoutCatalogs()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSource("test")
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session, nodeCount)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static TpchQueryRunner builder()
    {
        return new TpchQueryRunner();
    }

    // keep this method for convenience as it's common to pass no parameters
    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return builder().build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = builder()
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();
        Thread.sleep(10);
        Logger log = Logger.get(TpchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
