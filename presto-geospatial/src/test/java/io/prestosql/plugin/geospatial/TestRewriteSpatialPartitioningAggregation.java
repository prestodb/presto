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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.iterative.rule.test.RuleAssert;
import io.prestosql.sql.planner.plan.AggregationNode;
import org.testng.annotations.Test;

import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteSpatialPartitioningAggregation
        extends BaseRuleTest
{
    public TestRewriteSpatialPartitioningAggregation()
    {
        super(new GeoPlugin());
    }

    @Test
    public void testDoesNotFire()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(a ->
                        a.globalGrouping()
                                .step(AggregationNode.Step.FINAL)
                                .addAggregation(p.symbol("sp"), PlanBuilder.expression("spatial_partitioning(geometry, 10)"), ImmutableList.of(GEOMETRY))
                                .source(p.values(p.symbol("geometry")))))
                .doesNotFire();
    }

    @Test
    public void test()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(a ->
                        a.globalGrouping()
                                .step(AggregationNode.Step.FINAL)
                                .addAggregation(p.symbol("sp"), PlanBuilder.expression("spatial_partitioning(geometry)"), ImmutableList.of(GEOMETRY))
                                .source(p.values(p.symbol("geometry")))))
                .matches(
                        aggregation(
                                ImmutableMap.of("sp", functionCall("spatial_partitioning", ImmutableList.of("envelope", "partition_count"))),
                                project(
                                        ImmutableMap.of("partition_count", expression("100"),
                                                "envelope", expression("ST_Envelope(geometry)")),
                                        values("geometry"))));

        assertRuleApplication()
                .on(p -> p.aggregation(a ->
                        a.globalGrouping()
                                .step(AggregationNode.Step.FINAL)
                                .addAggregation(p.symbol("sp"), PlanBuilder.expression("spatial_partitioning(ST_Envelope(geometry))"), ImmutableList.of(GEOMETRY))
                                .source(p.values(p.symbol("geometry")))))
                .matches(
                        aggregation(
                                ImmutableMap.of("sp", functionCall("spatial_partitioning", ImmutableList.of("envelope", "partition_count"))),
                                project(
                                        ImmutableMap.of("partition_count", expression("100"),
                                                "envelope", expression("ST_Envelope(geometry)")),
                                        values("geometry"))));
    }

    private RuleAssert assertRuleApplication()
    {
        return tester().assertThat(new RewriteSpatialPartitioningAggregation(tester().getMetadata()));
    }
}
