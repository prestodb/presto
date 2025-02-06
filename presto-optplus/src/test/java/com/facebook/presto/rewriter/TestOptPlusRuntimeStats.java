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

package com.facebook.presto.rewriter;

import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.rewriter.optplus.OptimizerStatus;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestOptPlusRuntimeStats
        extends AbstractOptPlusTestFramework
{
    @Override
    protected boolean enableFallback()
    {
        return false;
    }

    @Test
    public void testOptPlusRuntimeStats()
    {
        String queryResourcePath = "/tpch/q07.sql";

        String sql = read(queryResourcePath)
                .replaceAll("<schema>", getDb2TpchSchema().toLowerCase(Locale.ENGLISH))
                .replaceAll("<catalog>", getDb2TpchCatalog());

        MaterializedResult result = getQueryRunner().execute(getSession(), sql);
        assertTrue(!result.getMaterializedRows().isEmpty(), "Expected non-empty result set");

        // Cast to DistributedQueryRunner
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();

        // Get full query list from QueryManager
        QueryManager queryManager = distributedQueryRunner.getCoordinator().getQueryManager();
        List<QueryInfo> queries = queryManager.getQueries().stream()
                .map(info -> queryManager.getFullQueryInfo(info.getQueryId()))
                .collect(Collectors.toList());

        // Find query related to this test SQL
        QueryInfo queryInfo = queries.stream()
                .filter(q -> q.getQuery().toLowerCase(Locale.ENGLISH).contains("nation"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No matching query found"));

        RuntimeStats stats = queryInfo.getQueryStats().getRuntimeStats();

        Map<String, RuntimeMetric> metrics = stats.getMetrics();

        assertNotNull(metrics, "Expected RuntimeStats to be populated");
        assertTrue(!metrics.isEmpty(), "Expected runtime stats to be non-empty");

        assertTrue(metrics.containsKey(OptimizerStatus.OPTIMIZER_GUIDELINE_APPLIED.getStatus()), "Expected 'optplusGuidelineApplied' to be present in runtime stats");
        assertFalse(metrics.containsKey(OptimizerStatus.OPTIMIZER_FALLBACK.getStatus()), "Expected 'optplusFallback' to be not present in runtime stats");
    }
}
