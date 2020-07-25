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
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Replaces subscript expression on Row with cast and dereference:
 *
 * <pre>
 * ROW(1, 'a', 2)[2]
 * </pre>
 * <p>
 * is transformed into:
 *
 * <pre>
 *     (CAST (ROW (1, 'a', 2) AS ROW (field_0 bigint, field_1 varchar(1), field_2 bigint))).field_1
 * </pre>
 */
public class DesugarRowSubscriptRewriter
{
    private DesugarRowSubscriptRewriter()
    {
    }

    public static Expression rewrite(Expression expression, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(expressionTypes), expression, null);
    }

    public static Expression rewrite(Expression expression, Session session, Metadata metadata, SqlParser sqlParser, PlanVariableAllocator variableAllocator)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");
        return new AnalyzedExpressionRewriter(session, metadata, sqlParser, variableAllocator.getTypes()).rewriteWith(Visitor::new, expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, Type> expressionTypes;

        public Visitor(Map<NodeRef<Expression>, Type> expressionTypes)
        {
            this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        }

        @Override
        public Expression rewriteSubscriptExpression(SubscriptExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression base = node.getBase();
            Expression index = node.getIndex();

            Expression result = node;

            Type type = getType(base);
            if (type instanceof RowType) {
                RowType rowType = (RowType) type;
                int position = toIntExact(((LongLiteral) index).getValue() - 1);

                Optional<String> fieldName = rowType.getFields().get(position).getName();

                // Do not cast if Row fields are named
                if (fieldName.isPresent()) {
                    result = new DereferenceExpression(base, new Identifier(fieldName.get()));
                }
                else {
                    // Cast to Row with named fields
                    ImmutableList.Builder<RowType.Field> namedFields = new ImmutableList.Builder<>();
                    for (int i = 0; i < rowType.getFields().size(); i++) {
                        namedFields.add(new RowType.Field(Optional.of("f" + i), rowType.getTypeParameters().get(i)));
                    }
                    RowType namedRowType = RowType.from(namedFields.build());
                    Cast cast = new Cast(base, namedRowType.getTypeSignature().toString());
                    result = new DereferenceExpression(cast, new Identifier("f" + position));
                }
            }

            return treeRewriter.defaultRewrite(result, context);
        }

        private Type getType(Expression expression)
        {
            return expressionTypes.get(NodeRef.of(expression));
        }
    }
}
