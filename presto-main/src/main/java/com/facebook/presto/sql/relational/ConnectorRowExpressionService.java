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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;

import static java.util.Objects.requireNonNull;

public final class ConnectorRowExpressionService
        implements RowExpressionService
{
    private final DomainTranslator domainTranslator;
    private final ExpressionOptimizer expressionOptimizer;
    private final PredicateCompiler predicateCompiler;
    private final DeterminismEvaluator determinismEvaluator;
    private final RowExpressionFormatter rowExpressionFormatter;

    public ConnectorRowExpressionService(DomainTranslator domainTranslator, ExpressionOptimizer expressionOptimizer, PredicateCompiler predicateCompiler, DeterminismEvaluator determinismEvaluator, RowExpressionFormatter rowExpressionFormatter)
    {
        this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
        this.predicateCompiler = requireNonNull(predicateCompiler, "predicateCompiler is null");
        this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        this.rowExpressionFormatter = requireNonNull(rowExpressionFormatter, "rowExpressionFormatter is null");
    }

    @Override
    public DomainTranslator getDomainTranslator()
    {
        return domainTranslator;
    }

    @Override
    public ExpressionOptimizer getExpressionOptimizer()
    {
        return expressionOptimizer;
    }

    @Override
    public PredicateCompiler getPredicateCompiler()
    {
        return predicateCompiler;
    }

    @Override
    public DeterminismEvaluator getDeterminismEvaluator()
    {
        return determinismEvaluator;
    }

    @Override
    public String formatRowExpression(ConnectorSession session, RowExpression expression)
    {
        return rowExpressionFormatter.formatRowExpression(session, expression);
    }
}
