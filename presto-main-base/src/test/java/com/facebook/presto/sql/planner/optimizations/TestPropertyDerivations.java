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

import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.optimizations.PropertyDerivations.derivePropertiesRecursively;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPropertyDerivations
        extends BaseRuleTest
{
    /**
     * A build side producing exactly one row leaves the probe's order intact: the nested loop join keeps
     * the page with more rows whole and appends the build's columns to it. This is the shape of a
     * broadcast scalar, such as a total joined onto every row.
     */
    @Test
    public void testCrossJoinWithScalarBuildKeepsProbeLocalProperties()
    {
        ActualProperties properties = deriveCrossJoin(p -> p.aggregation(agg -> agg
                .addAggregation(p.variable("total", BIGINT), p.rowExpression("count()"))
                .globalGrouping()
                .source(p.values(p.variable("ignored", BIGINT)))));
        assertEquals(sortedColumnNames(properties), ImmutableList.of("a"));
    }

    /**
     * A build side that may produce several rows repeats each probe row, so a run of equal probe values is
     * interleaved with the other runs and the probe's order no longer holds.
     */
    @Test
    public void testCrossJoinWithNonScalarBuildDiscardsProbeLocalProperties()
    {
        ActualProperties properties = deriveCrossJoin(p -> p.values(2, p.variable("total", BIGINT)));
        assertTrue(properties.getLocalProperties().isEmpty(), "A build side of more than one row must not keep the probe's order");
    }

    /**
     * Builds {@code cross join(sort(values) , build)} and derives its properties. The probe is sorted on
     * {@code a}, so the join's local properties are the question under test.
     */
    private ActualProperties deriveCrossJoin(Function<PlanBuilder, PlanNode> build)
    {
        PlanBuilder planBuilder = new PlanBuilder(tester().getSession(), new PlanNodeIdAllocator(), tester().getMetadata());
        VariableReferenceExpression a = planBuilder.variable("a", BIGINT);
        VariableReferenceExpression b = planBuilder.variable("b", BIGINT);
        PlanNode join = planBuilder.join(
                JoinType.INNER,
                planBuilder.sort(ImmutableList.of(a), planBuilder.values(a, b)),
                build.apply(planBuilder));
        return derivePropertiesRecursively(join, tester().getMetadata(), tester().getSession());
    }

    private static ImmutableList<String> sortedColumnNames(ActualProperties properties)
    {
        return properties.getLocalProperties().stream()
                .map(property -> (SortingProperty<VariableReferenceExpression>) property)
                .peek(property -> assertEquals(property.getOrder(), ASC_NULLS_FIRST))
                .map(property -> property.getColumn().getName())
                .collect(toImmutableList());
    }
}
