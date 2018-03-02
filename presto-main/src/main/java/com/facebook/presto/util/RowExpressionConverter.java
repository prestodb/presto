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
package com.facebook.presto.util;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;

public class RowExpressionConverter
{
    private final Map<Symbol, Integer> sourceLayout;
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final FunctionRegistry funcRegistry;
    private final TypeManager typeManager;
    private final Session session;

    public RowExpressionConverter(Map<Symbol, Integer> sourceLayout, Map<NodeRef<Expression>,
            Type> expressionTypes, FunctionRegistry funcRegistry, TypeManager typeManager, Session session)
    {
        this.sourceLayout = sourceLayout;
        this.expressionTypes = expressionTypes;
        this.funcRegistry = funcRegistry;
        this.typeManager = typeManager;
        this.session = session;
    }

    public Optional<RowExpression> expressionToRowExpression(Optional<Expression> staticFilter)
    {
        SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
        Optional<Expression> rewrittenFilter = staticFilter.map(symbolToInputRewriter::rewrite);
        return rewrittenFilter.map(filter -> toRowExpression(filter, expressionTypes));
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    private RowExpression toRowExpression(Expression expression, Map<NodeRef<Expression>, Type> types)
    {
        return SqlToRowExpressionTranslator.translateExpression(expression, SCALAR, types,
            funcRegistry, typeManager, session, true);
    }
}
