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
package com.facebook.presto.sql.relational;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.ExpressionOptimizerProvider;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Verifies that RowExpressionDomainTranslator routes constant-folding of comparison operands through the
 * session's pluggable ExpressionOptimizer when one is supplied. Under the native expression optimizer this
 * sends operands to the sidecar instead of the hardcoded Java interpreter, which would mis-fold
 * native-produced expression shapes and poison the extracted TupleDomain (issue behind the RightJoin
 * wrong-results bug).
 */
public class TestRowExpressionDomainTranslatorExpressionOptimizer
{
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Session SESSION = testSessionBuilder().build();
    private static final VariableReferenceExpression C_BIGINT = new VariableReferenceExpression(Optional.empty(), "c_bigint", BIGINT);

    // A provider that records whether it was consulted and delegates to the real Java optimizer, so the
    // extracted domain is unchanged. This proves the translator routes operand folding through the
    // pluggable optimizer without altering results on the default path.
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

    // c_bigint = (1 + 1): the right side is a non-trivial constant expression, so extraction folds it via
    // the optimizer to recover the singleton domain {2}.
    private RowExpression comparisonWithFoldableConstant()
    {
        RowExpression rhs = call(
                ADD.name(),
                METADATA.getFunctionAndTypeManager().resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                constant(1L, BIGINT),
                constant(1L, BIGINT));
        return call(
                EQUAL.name(),
                METADATA.getFunctionAndTypeManager().resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                C_BIGINT,
                rhs);
    }

    @Test
    public void testUsesSuppliedExpressionOptimizer()
    {
        CountingExpressionOptimizerProvider provider = new CountingExpressionOptimizerProvider();
        RowExpressionDomainTranslator translator = new RowExpressionDomainTranslator(METADATA, provider);

        ExtractionResult<VariableReferenceExpression> result = translator.fromPredicate(
                SESSION.toConnectorSession(),
                comparisonWithFoldableConstant(),
                BASIC_COLUMN_EXTRACTOR);

        assertTrue(provider.optimizeCalls.get() > 0, "expected the translator to consult the supplied optimizer for operand folding");
        // The folded operand 1 + 1 = 2 yields the singleton domain {2} on c_bigint.
        assertTrue(result.getTupleDomain().getDomains().isPresent());
        assertTrue(result.getTupleDomain().getDomains().get().containsKey(C_BIGINT));
    }

    @Test
    public void testDefaultProviderMatchesJavaInterpreter()
    {
        // The default provider (RowExpressionOptimizer, which wraps the Java interpreter) and the legacy
        // hardcoded-interpreter path must extract identical domains, so the routing is a no-op for default.
        RowExpressionDomainTranslator routed = new RowExpressionDomainTranslator(METADATA, new CountingExpressionOptimizerProvider());
        RowExpressionDomainTranslator legacy = new RowExpressionDomainTranslator(METADATA);

        ExtractionResult<VariableReferenceExpression> routedResult = routed.fromPredicate(
                SESSION.toConnectorSession(), comparisonWithFoldableConstant(), BASIC_COLUMN_EXTRACTOR);
        ExtractionResult<VariableReferenceExpression> legacyResult = legacy.fromPredicate(
                SESSION.toConnectorSession(), comparisonWithFoldableConstant(), BASIC_COLUMN_EXTRACTOR);

        assertEquals(routedResult.getTupleDomain(), legacyResult.getTupleDomain());
    }
}
