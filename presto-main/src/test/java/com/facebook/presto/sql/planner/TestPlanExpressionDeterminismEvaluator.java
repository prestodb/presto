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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestPlanExpressionDeterminismEvaluator
        extends BasePlanTest
{
    private RowExpressionDeterminismEvaluator rowExpressionDeterminismEvaluator;

    public TestPlanExpressionDeterminismEvaluator()
    {
        createTestQueryRunner();
    }

    private LocalQueryRunner createTestQueryRunner()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestMetadataManager().getFunctionAndTypeManager();
        LocalQueryRunner queryRunner = createQueryRunner(ImmutableMap.of());
        rowExpressionDeterminismEvaluator = new RowExpressionDeterminismEvaluator(queryRunner.getFunctionAndTypeManager());
        return queryRunner;
    }

    @Test
    public void testDeterminism()
            throws Exception
    {
        assertDeterminism("SELECT * FROM orders", true);
        //Filter
        assertDeterminism("SELECT * FROM orders WHERE orderkey < 100", true);
        assertDeterminism("SELECT * FROM orders WHERE random() < 0.5", false);
        assertDeterminism("SELECT * FROM orders TABLESAMPLE BERNOULLI (50)", false);
        //Join
        assertDeterminism("SELECT * FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey", true);
        assertDeterminism("SELECT * FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey WHERE random() < 0.5", false);

        // window
        assertDeterminism("SELECT orderkey, row_number() OVER (PARTITION BY custkey) FROM orders", true);
        assertDeterminism("SELECT orderkey, row_number() OVER (PARTITION BY custkey ORDER BY random()) FROM orders", false);
    }

    private Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build();
    }

    private void assertDeterminism(String sql, boolean determinism)
    {
        Session session = createSession();
        PlanNode plan = plan(sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, session).getRoot();
        assertEquals(PlanDeterminismEvaluator.isDeterministic(plan, rowExpressionDeterminismEvaluator), determinism);
    }
}
