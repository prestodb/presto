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

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.SystemSessionProperties.SIZE_BASED_JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

/**
 * Verifies that a join flipped because the probe side is provably empty survives to the final
 * distributed plan.
 * <p>
 * {@code DetermineJoinDistributionType} placing the provably-empty side on the build is only half of
 * the payoff: nothing between that rule and {@code AddExchanges}, nor fragmentation, may undo or
 * re-flip it. A {@code RuleTester} test cannot see that by construction -- it applies one rule to one
 * node -- so this is the full-pipeline assertion.
 * <p>
 * <b>Two ways to build this test wrongly:</b>
 * <ol>
 * <li>Expressing emptiness <i>structurally</i>. A 0-row {@code ValuesNode} makes
 * {@code QueryCardinalityUtil.isAtMostScalar} true ({@code visitValues} returns
 * {@code Range.singleton(rows.size())}), which sets {@code mustReplicate} and silently excludes the
 * flipped PARTITIONED candidate. Emptiness must come only from statistics. Here it is a derived row
 * count on a {@code Filter} over a real {@code TableScan}, whose cardinality bound is
 * {@code [0, infinity)} via {@code visitPlan}.</li>
 * <li>Letting the <i>size-based fallback</i> decide. If the probe's source tables are small,
 * {@code getSizeBasedJoin} flips purely because {@code isBelowBroadcastLimit} likes
 * {@code getSourceTablesSizeInBytes}, i.e. the raw scan size, and the derived zero is never consulted
 * -- so the test would pass with any predicate at all. Both arms below therefore use the <b>same large
 * source tables</b> ({@code sf100}, the largest scale factor with TPCH statistics resources), which puts
 * {@code getSourceTablesSizeInBytes} far above the broadcast limit in both arms and forces the decision
 * onto the cost-based path. That is also the path a real plan must use whenever an expanding node is
 * present, because {@code getSourceTablesSizeInBytes} then returns NaN.</li>
 * </ol>
 * The arms are identical except for the filter's estimated selectivity, so a difference in the
 * resulting join can only be attributable to the row-count estimate.
 */
public class TestProvablyEmptyProbeSideDistributedPlan
        extends BasePlanTest
{
    // linenumber is in [1, 7], so this equality is provably unsatisfiable to the stats calculator and
    // yields a known row count of 0. The filtered column is deliberately NOT the join key: filtering on
    // orderkey would propagate the same domain to the build side and zero both arms.
    private static final String EMPTY_PROBE = "(SELECT orderkey FROM local.sf100.lineitem WHERE linenumber = -1)";
    // Identical shape, identical source table, satisfiable predicate. linenumber has only 7 distinct
    // values, so ANY satisfiable predicate on it leaves at least ~1/7 of sf100.lineitem (~86M rows) --
    // comfortably more than the build side below. That is deliberate: it means no non-empty probe
    // estimate can make the probe the cheaper build side, so a flip can only come from the zero. There
    // is no middle ground for the test to accidentally land in.
    private static final String NON_EMPTY_PROBE = "(SELECT orderkey FROM local.sf100.lineitem WHERE linenumber = 1)";
    // ~15M rows: smaller than any non-empty probe estimate above, but still far above
    // join_max_broadcast_table_size, so both candidates stay PARTITIONED.
    private static final String BUILD = "(SELECT custkey FROM local.sf100.customer)";

    private static String leftJoin(String probe)
    {
        return "SELECT b.custkey FROM " + probe + " p LEFT JOIN " + BUILD + " b ON b.custkey = p.orderkey";
    }

    private Session costBasedFlipping()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name())
                // Both sides are far above this, so REPLICATED is never eligible and both candidates are
                // PARTITIONED -- the winner is chosen by the cost comparator, not by a size fallback.
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .build();
    }

    /**
     * A provably empty probe side ends up as the build side of a RIGHT/PARTITIONED join in the final
     * distributed plan. That shape is what allows an execution engine to finish the join early once the
     * build side turns out to be empty, without reading the probe at all.
     */
    @Test
    public void testFlipReachesDistributedPlan()
    {
        assertDistributedPlan(
                leftJoin(EMPTY_PROBE),
                costBasedFlipping(),
                anyTree(
                        join(RIGHT, ImmutableList.of(equiJoinClause("BUILD_KEY", "PROBE_KEY")), Optional.empty(), Optional.of(PARTITIONED),
                                anyTree(tableScan("customer", ImmutableMap.of("BUILD_KEY", "custkey"))),
                                anyTree(tableScan("lineitem", ImmutableMap.of("PROBE_KEY", "orderkey"))))));
    }

    /**
     * Statistical control -- the arm that makes the causal claim real. Same tables, same structure, same
     * source-table sizes, same session: only the filter's estimated selectivity differs. With a
     * non-empty probe estimate the join must NOT flip. If this arm ever starts producing a RIGHT join,
     * the test above has stopped testing the zero.
     */
    @Test
    public void testNoFlipWhenProbeEstimateIsNotZero()
    {
        assertDistributedPlan(
                leftJoin(NON_EMPTY_PROBE),
                costBasedFlipping(),
                anyTree(
                        join(LEFT, ImmutableList.of(equiJoinClause("PROBE_KEY", "BUILD_KEY")), Optional.empty(), Optional.of(PARTITIONED),
                                anyTree(tableScan("lineitem", ImmutableMap.of("PROBE_KEY", "orderkey"))),
                                anyTree(tableScan("customer", ImmutableMap.of("BUILD_KEY", "custkey"))))));
    }

    /**
     * Path control: the flip is won on the <b>cost-based</b> path, not by the size-based fallback.
     * {@code getSizeBasedJoin} is only reachable when {@code size_based_join_distribution_type} is on,
     * so with it off the fallback can only return {@code getSyntacticOrderJoin} (i.e. LEFT). The flip
     * still happening here means the winner came from the cost comparator.
     * <p>
     * This matters because a real plan containing an expanding node <i>cannot</i> use the fallback:
     * {@code getSourceTablesSizeInBytes} returns NaN in that case. A test that only proved the fallback
     * flips would be proving the wrong mechanism.
     */
    @Test
    public void testFlipIsWonOnTheCostBasedPath()
    {
        Session session = Session.builder(costBasedFlipping())
                .setSystemProperty(SIZE_BASED_JOIN_DISTRIBUTION_TYPE, "false")
                .build();

        assertDistributedPlan(
                leftJoin(EMPTY_PROBE),
                session,
                anyTree(
                        join(RIGHT, ImmutableList.of(equiJoinClause("BUILD_KEY", "PROBE_KEY")), Optional.empty(), Optional.of(PARTITIONED),
                                anyTree(tableScan("customer", ImmutableMap.of("BUILD_KEY", "custkey"))),
                                anyTree(tableScan("lineitem", ImmutableMap.of("PROBE_KEY", "orderkey"))))));
    }
}
