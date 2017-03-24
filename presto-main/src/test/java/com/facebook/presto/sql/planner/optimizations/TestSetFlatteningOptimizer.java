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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.StatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.except;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.intersect;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;

public class TestSetFlatteningOptimizer
        extends BasePlanTest
{
    @Test
    public void testFlattensUnion()
    {
        assertPlan(
                "(SELECT * FROM nation UNION SELECT * FROM nation)" +
                        "UNION (SELECT * FROM nation UNION SELECT * FROM nation)",
                anyTree(
                        union(
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"))));
    }

    @Test
    public void testFlattensUnionAll()
    {
        assertPlan(
                "(SELECT * FROM nation UNION ALL SELECT * FROM nation)" +
                        "UNION ALL (SELECT * FROM nation UNION ALL SELECT * FROM nation)",
                anyTree(
                        union(
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"))));
    }

    @Test
    public void testFlattensUnionAndUnionAllWhenAllowed()
    {
        assertPlan(
                "SELECT * FROM nation " +
                        "UNION ALL (SELECT * FROM nation " +
                        "UNION (SELECT * FROM nation UNION ALL select * FROM nation))",
                anyTree(
                        union(
                                tableScan("nation"),
                                anyTree(
                                        union(
                                                tableScan("nation"),
                                                tableScan("nation"),
                                                tableScan("nation"))))));
    }

    @Test
    public void testFlattensIntersect()
    {
        assertPlan(
                "(SELECT * FROM nation INTERSECT SELECT * FROM nation)" +
                        "INTERSECT (SELECT * FROM nation INTERSECT SELECT * FROM nation)",
                anyTree(
                        intersect(
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"))));
    }

    @Test
    public void testFlattensOnlyFirstInputOfExcept()
    {
        assertPlan(
                "(SELECT * FROM nation EXCEPT SELECT * FROM nation)" +
                        "EXCEPT (SELECT * FROM nation EXCEPT SELECT * FROM nation)",
                anyTree(
                        except(
                                tableScan("nation"),
                                tableScan("nation"),
                                except(
                                        tableScan("nation"),
                                        tableScan("nation")))));
    }

    @Test
    public void testDoesNotFlattenDifferentSetOperations()
    {
        assertPlan(
                "(SELECT * FROM nation EXCEPT SELECT * FROM nation)" +
                        "UNION (SELECT * FROM nation INTERSECT SELECT * FROM nation)",
                anyTree(
                        union(
                                except(
                                        tableScan("nation"),
                                        tableScan("nation")),
                                intersect(
                                        tableScan("nation"),
                                        tableScan("nation")))));
    }

    public void assertPlan(String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(new StatsRecorder(), ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new SetFlatteningOptimizer());
        assertPlanWithOptimizers(sql, pattern, optimizers);
    }
}
