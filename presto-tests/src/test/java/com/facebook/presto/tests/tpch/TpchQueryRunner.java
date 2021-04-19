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
import com.facebook.presto.dispatcher.RandomWaitingPrerequisiteManager;
import com.facebook.presto.spi.dispatcher.PrerequisiteManager;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Scopes;

import java.util.Map;

public final class TpchQueryRunner
{
    private TpchQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .setExtraProperties(extraProperties)
                .setExtraModules(ImmutableList.of((binder) -> binder.bind(PrerequisiteManager.class).to(RandomWaitingPrerequisiteManager.class).in(Scopes.SINGLETON)))
                .build();
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties, int coordinatorCount)
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .setExtraProperties(extraProperties)
                .setResourceManagerEnabled(true)
                .setCoordinatorCount(coordinatorCount)
                .setExtraModules(ImmutableList.of((binder) -> binder.bind(PrerequisiteManager.class).to(RandomWaitingPrerequisiteManager.class).in(Scopes.SINGLETON)))
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"));
        Thread.sleep(10);
        Logger log = Logger.get(TpchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
