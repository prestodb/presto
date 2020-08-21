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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert.TestingStatsCalculator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;

import java.util.function.Function;

import static com.facebook.presto.sql.planner.assertions.PlanAssert.assertPlan;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.fail;

public class OptimizerAssert
{
    private final Metadata metadata;
    private final TestingStatsCalculator statsCalculator;
    private final Session session;
    private final PlanOptimizer optimizer;
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;

    private TypeProvider types;
    private PlanNode plan;

    public OptimizerAssert(Metadata metadata, StatsCalculator statsCalculator, Session session, PlanOptimizer optimizer, TransactionManager transactionManager, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsCalculator = new TestingStatsCalculator(requireNonNull(statsCalculator, "statsCalculator is null"));
        this.session = requireNonNull(session, "session is null");
        this.optimizer = requireNonNull(optimizer, "optimizer is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "access control is null");
    }

    public OptimizerAssert on(Function<PlanBuilder, PlanNode> planProvider)
    {
        checkState(plan == null, "plan has already been set");

        PlanBuilder builder = new PlanBuilder(session, idAllocator, metadata);
        plan = planProvider.apply(builder);
        types = builder.getTypes();
        return this;
    }

    public void matches(PlanMatchPattern pattern)
    {
        PlanNode actual = optimizer.optimize(plan, session, types, new PlanVariableAllocator(), idAllocator, WarningCollector.NOOP);

        if (!ImmutableSet.copyOf(plan.getOutputVariables()).equals(ImmutableSet.copyOf(actual.getOutputVariables()))) {
            fail(String.format(
                    "%s: output schema of transformed and original plans are not equivalent\n" +
                            "\texpected: %s\n" +
                            "\tactual:   %s",
                    optimizer.getClass().getName(),
                    plan.getOutputVariables(),
                    actual.getOutputVariables()));
        }

        inTransaction(session -> {
            assertPlan(session, metadata, statsCalculator, new Plan(actual, types, StatsAndCosts.empty()), pattern);
            return null;
        });
    }

    private <T> void inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }
}
