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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.ExpressionOptimizerProvider;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Verifies that PropertyDerivations routes ProjectNode constant-folding through the session's pluggable
 * ExpressionOptimizer when one is supplied. Under the native expression optimizer this sends the
 * expression to the sidecar instead of evaluating it with the hardcoded Java interpreter, which would
 * mis-derive constants for native-produced expression shapes.
 */
public class TestPropertyDerivationsExpressionOptimizer
{
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Session SESSION = testSessionBuilder().build();

    // A stub optimizer standing in for the native/sidecar optimizer: it folds ANY non-trivial expression
    // to a constant NULL (mimicking the shape divergence that produced the poisoned constant property).
    private static final ExpressionOptimizerProvider FOLDS_TO_NULL = new ExpressionOptimizerProvider()
    {
        @Override
        public ExpressionOptimizer getExpressionOptimizer(ConnectorSession session)
        {
            return new ExpressionOptimizer()
            {
                @Override
                public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session)
                {
                    return new ConstantExpression(null, expression.getType());
                }

                @Override
                public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
                {
                    return new ConstantExpression(null, expression.getType());
                }
            };
        }
    };

    private final VariableReferenceExpression out = new VariableReferenceExpression(Optional.empty(), "out", BIGINT);

    private ProjectNode nonTrivialProjection()
    {
        PlanBuilder p = new PlanBuilder(SESSION, new PlanNodeIdAllocator(), METADATA);
        VariableReferenceExpression a = p.variable("a", BIGINT);
        // out := a + a  (a non-constant, non-variable expression, so visitProject invokes the optimizer)
        RowExpression expression = call(
                ADD.name(),
                METADATA.getFunctionAndTypeManager().resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
                BIGINT, a, a);
        return p.project(assignment(out, expression), p.values(a));
    }

    @Test
    public void testProjectUsesSuppliedExpressionOptimizer()
    {
        ProjectNode project = nonTrivialProjection();

        // With the stub optimizer wired in, the projection is folded to a constant and recorded as a
        // constant property -- proving PropertyDerivations consulted the pluggable optimizer.
        ActualProperties withOptimizer = PropertyDerivations.deriveProperties(
                project,
                ImmutableList.of(ActualProperties.builder().build()),
                METADATA,
                SESSION,
                FOLDS_TO_NULL);
        assertTrue(withOptimizer.getConstants().containsKey(out),
                "expected the projection to be folded to a constant via the supplied optimizer");
    }

    @Test
    public void testProjectFallsBackToJavaInterpreterWithoutProvider()
    {
        ProjectNode project = nonTrivialProjection();

        // Without a provider (legacy path, e.g. sanity checks), the hardcoded Java interpreter runs and
        // does NOT fold `a + a` to a constant. This confirms the routing genuinely changes behavior.
        ActualProperties withoutOptimizer = PropertyDerivations.deriveProperties(
                project,
                ImmutableList.of(ActualProperties.builder().build()),
                METADATA,
                SESSION);
        assertFalse(withoutOptimizer.getConstants().containsKey(out),
                "the Java interpreter must not fold a + a to a constant");
    }
}
