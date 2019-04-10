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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.relational.SqlToRowExpressionTranslator.translate;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class ExpressionEquivalence
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final RowExpressionCanonicalizer rowExpressionCanonicalizer;

    public ExpressionEquivalence(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.rowExpressionCanonicalizer = new RowExpressionCanonicalizer(metadata.getFunctionManager());
    }

    public boolean areExpressionsEquivalent(Session session, Expression leftExpression, Expression rightExpression, TypeProvider types)
    {
        Map<Symbol, Integer> symbolInput = new HashMap<>();
        int inputId = 0;
        for (Entry<Symbol, Type> entry : types.allTypes().entrySet()) {
            symbolInput.put(entry.getKey(), inputId);
            inputId++;
        }
        RowExpression leftRowExpression = toRowExpression(session, leftExpression, symbolInput, types);
        RowExpression rightRowExpression = toRowExpression(session, rightExpression, symbolInput, types);

        RowExpression canonicalizedLeft = rowExpressionCanonicalizer.canonicalize(leftRowExpression);
        RowExpression canonicalizedRight = rowExpressionCanonicalizer.canonicalize(rightRowExpression);

        return canonicalizedLeft.equals(canonicalizedRight);
    }

    private RowExpression toRowExpression(Session session, Expression expression, Map<Symbol, Integer> symbolInput, TypeProvider types)
    {
        // replace qualified names with input references since row expressions do not support these

        // determine the type of every expression
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                sqlParser,
                types,
                expression,
                emptyList(), /* parameters have already been replaced */
                WarningCollector.NOOP);

        // convert to row expression
        return translate(expression, expressionTypes, symbolInput, metadata.getFunctionManager(), metadata.getTypeManager(), session, false);
    }
}
