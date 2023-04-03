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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Objects.requireNonNull;

@Deprecated
public class DesugarAtTimeZoneRewriter
{
    public static Expression rewrite(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(expressionTypes), expression, null);
    }

    private DesugarAtTimeZoneRewriter() {}

    public static Expression rewrite(Expression expression, Session session, Metadata metadata, SqlParser sqlParser, VariableAllocator variableAllocator)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");

        if (expression instanceof SymbolReference) {
            return expression;
        }
        return new AnalyzedExpressionRewriter(session, metadata, sqlParser, TypeProvider.viewOf(variableAllocator.getVariables())).rewriteWith(Visitor::new, expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, Type> expressionTypes;

        public Visitor(Map<NodeRef<Expression>, Type> expressionTypes)
        {
            this.expressionTypes = requireNonNull(expressionTypes, "expressionTypes is null");
        }

        @Override
        public Expression rewriteAtTimeZone(AtTimeZone node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression value = treeRewriter.rewrite(node.getValue(), context);
            Type type = expressionTypes.get(NodeRef.of(node.getValue()));
            if (type.equals(TIME)) {
                value = new Cast(value, TIME_WITH_TIME_ZONE.getDisplayName());
            }
            else if (type.equals(TIMESTAMP)) {
                value = new Cast(value, TIMESTAMP_WITH_TIME_ZONE.getDisplayName());
            }

            return new FunctionCall(QualifiedName.of("at_timezone"), ImmutableList.of(value, treeRewriter.rewrite(node.getTimeZone(), context)));
        }
    }
}
