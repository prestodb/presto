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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public final class TpchQueryRunner
{
    private TpchQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .build();
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .setExtraProperties(extraProperties)
                .build();
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties, int coordinatorCount)
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setExtraProperties(extraProperties)
                .setResourceManagerEnabled(true)
                .setCoordinatorCount(coordinatorCount)
                .build();
        queryRunner.waitForClusterToGetReady();
        return queryRunner;
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> resourceManagerProperties, Map<String, String> coordinatorProperties, Map<String, String> extraProperties, int coordinatorCount)
            throws Exception
    {
        return createQueryRunner(resourceManagerProperties, coordinatorProperties, extraProperties, coordinatorCount, false);
    }

    public static DistributedQueryRunner createQueryRunnerWithNoClusterReadyCheck(Map<String, String> resourceManagerProperties, Map<String, String> coordinatorProperties, Map<String, String> extraProperties, int coordinatorCount)
            throws Exception
    {
        return createQueryRunner(resourceManagerProperties, coordinatorProperties, extraProperties, coordinatorCount, true);
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> resourceManagerProperties, Map<String, String> coordinatorProperties, Map<String, String> extraProperties, int coordinatorCount, boolean skipClusterReadyCheck)
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setResourceManagerProperties(resourceManagerProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(extraProperties)
                .setResourceManagerEnabled(true)
                .setCoordinatorCount(coordinatorCount)
                .build();
        if (!skipClusterReadyCheck) {
            queryRunner.waitForClusterToGetReady();
        }
        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"), 1);
        Thread.sleep(10);
        Logger log = Logger.get(TpchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
