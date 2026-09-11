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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.ExpressionOptimizerProvider;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertTrue;

/**
 * Verifies that FilterStatsCalculator simplifies predicates through the session's pluggable
 * ExpressionOptimizer. Under the native expression optimizer this routes predicate simplification to the
 * sidecar instead of the hardcoded Java interpreter, which would mis-fold native-produced expression
 * shapes when computing filter selectivity.
 */
public class TestFilterStatsCalculatorExpressionOptimizer
{
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();
    private static final Session SESSION = testSessionBuilder().build();
    private static final VariableReferenceExpression X = new VariableReferenceExpression(Optional.empty(), "x", BIGINT);

    private static class CountingExpressionOptimizerProvider
            implements ExpressionOptimizerProvider
    {
        private final AtomicInteger optimizeCalls = new AtomicInteger();

        @Override
        public ExpressionOptimizer getExpressionOptimizer(ConnectorSession session)
        {
            ExpressionOptimizer delegate = new RowExpressionOptimizer(METADATA);
            return new ExpressionOptimizer()
            {
                @Override
                public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session)
                {
                    optimizeCalls.incrementAndGet();
                    return delegate.optimize(expression, level, session);
                }

                @Override
                public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
                {
                    optimizeCalls.incrementAndGet();
                    return delegate.optimize(expression, level, session, variableResolver);
                }
            };
        }
    }

    // x > (1 + 1): a predicate whose constant sub-expression must be folded during simplification.
    private RowExpression predicateWithFoldableConstant()
    {
        RowExpression rhs = call(
                ADD.name(),
                METADATA.getFunctionAndTypeManager().resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                constant(1L, BIGINT),
                constant(1L, BIGINT));
        return call(
                GREATER_THAN.name(),
                METADATA.getFunctionAndTypeManager().resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                X,
                rhs);
    }

    @Test
    public void testFilterStatsUsesSuppliedExpressionOptimizer()
    {
        CountingExpressionOptimizerProvider provider = new CountingExpressionOptimizerProvider();
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(
                METADATA,
                new ScalarStatsCalculator(METADATA, provider),
                new StatsNormalizer(),
                provider);

        VariableStatsEstimate xStats = VariableStatsEstimate.builder()
                .setLowValue(0)
                .setHighValue(10)
                .setDistinctValuesCount(10)
                .setNullsFraction(0)
                .build();
        PlanNodeStatsEstimate input = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1000)
                .addVariableStatistics(X, xStats)
                .build();

        filterStatsCalculator.filterStats(input, predicateWithFoldableConstant(), SESSION);

        assertTrue(provider.optimizeCalls.get() > 0, "expected FilterStatsCalculator to simplify the predicate via the supplied optimizer");
    }

    @Test
    public void testConstantExpressionUnwrappedToValue()
    {
        // A predicate that folds fully to a constant TRUE must be simplified to a constant, matching the
        // interpreter's contract (the unwrap of ConstantExpression to its raw value). We assert the routed
        // optimizer is consulted; the ConstantExpression it returns is unwrapped without error.
        CountingExpressionOptimizerProvider provider = new CountingExpressionOptimizerProvider();
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(
                METADATA,
                new ScalarStatsCalculator(METADATA, provider),
                new StatsNormalizer(),
                provider);

        RowExpression alwaysTrue = call(
                GREATER_THAN.name(),
                METADATA.getFunctionAndTypeManager().resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                constant(2L, BIGINT),
                constant(1L, BIGINT));

        PlanNodeStatsEstimate input = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1000)
                .addVariableStatistics(X, VariableStatsEstimate.builder().setLowValue(0).setHighValue(10).setDistinctValuesCount(10).setNullsFraction(0).build())
                .build();

        PlanNodeStatsEstimate result = filterStatsCalculator.filterStats(input, alwaysTrue, SESSION);

        assertTrue(provider.optimizeCalls.get() > 0, "expected the always-true predicate to be folded via the supplied optimizer");
        // An always-true filter keeps all input rows.
        assertTrue(result.getOutputRowCount() > 0);
    }
}
