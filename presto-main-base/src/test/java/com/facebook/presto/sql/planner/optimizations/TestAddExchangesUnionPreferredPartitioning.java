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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static org.testng.Assert.assertTrue;

/**
 * Regression tests for {@code AddExchanges.visitUnion} case 1, which translates the parent's
 * preferred partitioning onto each union branch and therefore only works when every preferred
 * partitioning column is an output of the union.
 * <p>
 * See {@code AddExchanges.canTranslatePartitioningOntoBranches} for why a column need not be.
 * <p>
 * These tests drive {@code AddExchanges} over the resulting plan directly rather than through SQL.
 * Reaching the shape from a query additionally requires that no {@code ProjectNode} survive between
 * the {@code AssignUniqueId} and the union, because {@code visitProject} translates the preference
 * through {@code computeIdentityTranslations} and drops the untranslatable column. Whether such a
 * projection survives depends on {@code PruneUnreferencedOutputs} and
 * {@code RemoveRedundantIdentityProjections}, which is a plan-shape coincidence rather than an
 * invariant, so a query-level test would silently degrade into a vacuous pass.
 */
public class TestAddExchangesUnionPreferredPartitioning
        extends BaseRuleTest
{
    /**
     * Some preferred partitioning columns are union outputs and some are not.
     * Before the guard this threw {@link NullPointerException} from
     * {@code outputToInputTranslator}, because {@code getVariableMapping()} is a plain
     * {@code Map} and a missing key yields null.
     */
    @Test
    public void testUnionWithPartlyUnmappedPreferredPartitioning()
    {
        assertPlansWithUnionPreserved(true);
    }

    /**
     * Boundary case: no preferred partitioning column is a union output, failing at the same site
     * as the case above. It also pins that a null-safe {@code outputToInputTranslator} is not on
     * its own sufficient, since every column would then fail to translate,
     * {@code PartitioningProperties.translateVariable} would return {@code Optional.empty()}, and
     * the {@code get()} in {@code selectUnionPartitioning} would throw
     * {@link java.util.NoSuchElementException}.
     */
    @Test
    public void testUnionWithFullyUnmappedPreferredPartitioning()
    {
        assertPlansWithUnionPreserved(false);
    }

    private void assertPlansWithUnionPreserved(boolean includeUnionOutputInPartitioning)
    {
        tester().assertThat(new AddExchanges(tester().getMetadata(), new PartitioningProviderManager(), false))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression a1 = p.variable("a1");
                    VariableReferenceExpression a2 = p.variable("a2");
                    VariableReferenceExpression unique = p.variable("unique");
                    ImmutableList.Builder<VariableReferenceExpression> distinctVariables = ImmutableList.builder();
                    if (includeUnionOutputInPartitioning) {
                        distinctVariables.add(a);
                    }
                    // `unique` is produced by the AssignUniqueId, never by the union
                    distinctVariables.add(unique);
                    PlanNode union = p.union(
                            ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                    .putAll(a, a1, a2)
                                    .build(),
                            ImmutableList.of(p.values(a1), p.values(a2)));
                    return p.markDistinct(
                            p.variable("is_distinct", BOOLEAN),
                            distinctVariables.build(),
                            p.assignUniqueId(unique, union));
                })
                // The regression is that planning threw, so completing at all is the assertion.
                // The union check only rules out the node being rewritten away; it does not
                // distinguish case 1 from case 2, since both emit a UnionNode.
                .validates(plan -> assertTrue(
                        PlanNodeSearcher.searchFrom(plan.getRoot()).where(UnionNode.class::isInstance).matches(),
                        "expected the UnionNode to survive planning"));
    }
}
