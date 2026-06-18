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
package com.facebook.presto.execution;

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestStageInfo
{
    private TestingPrestoServer server;
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        queryRunner = createQueryRunner(ImmutableMap.of("query.client.timeout", "10s"));
        server = queryRunner.getCoordinator();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        server = null;
    }

    @Test
    public void testGetStageWithStageId()
    {
        String sql = "SELECT nationkey AS nationkey, count(*) AS count FROM tpch.sf1.supplier GROUP BY nationkey";
        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), sql);
        QueryId queryId = result.getQueryId();
        Optional<StageInfo> stageInfo = server.getQueryManager().getFullQueryInfo(queryId).getOutputStage();

        if (stageInfo.isPresent()) {
            List<StageInfo> allStages = stageInfo.get().getAllStages();

            for (StageInfo expectedStage : allStages) {
                Optional<StageInfo> actualStage = stageInfo.get().getStageWithStageId(expectedStage.getStageId());
                if (!actualStage.isPresent()) {
                    fail("StageInfo with id " + expectedStage.getStageId() + "not found");
                }
                assertEquals(expectedStage.getStageId(), actualStage.get().getStageId());
            }
        }
    }
}
