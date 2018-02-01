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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static java.util.Collections.emptyList;

public class ConstraintEvaluator
{
    private final Session session;
    private final Metadata metadata;
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final List<Expression> conjuncts;

    public ConstraintEvaluator(Session session, Metadata metadata, SqlParser parser, Map<Symbol, Type> types, Expression expression)
    {
        this.session = session;
        this.metadata = metadata;

        expressionTypes = getExpressionTypes(session, metadata, parser, types, expression, emptyList());
        conjuncts = extractConjuncts(expression);
    }

    public boolean shouldPrune(Map<Symbol, ColumnHandle> assignments, Map<ColumnHandle, NullableValue> bindings, List<Symbol> correlations)
    {
        LookupSymbolResolver inputs = new LookupSymbolResolver(assignments, bindings);

        // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
        for (Expression expression : conjuncts) {
            if (SymbolsExtractor.extractUnique(expression).stream().anyMatch(correlations::contains)) {
                // expression contains correlated symbol with outer query
                continue;
            }
            ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
            Object optimized = optimizer.optimize(inputs);
            if (Boolean.FALSE.equals(optimized) || optimized == null || optimized instanceof NullLiteral) {
                return true;
            }
        }
        return false;
    }
}
