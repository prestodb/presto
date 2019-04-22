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

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;

/**
 * Convert a RowExpression to Expression.
 * This path is limited in scope and currently only exercised for passing RLS filters to the analyzer
 * since semantic checking of the structures is performed here.
 * Expanding the scope of this class also means that, we will be expanding the scope of what we accept
 * from Authorization Engines as RLS filters.
 */
public final class RowExpressionToSqlTranslator
{
    private RowExpressionToSqlTranslator() {}

    public static Expression translate(RowExpression rowExpression)
    {
        return rowExpression.accept(new RowExpressionVisitor<Expression, Void>()
        {
            @Override
            public Expression visitCall(CallExpression call, Void context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Expression visitInputReference(InputReferenceExpression reference, Void context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Expression visitConstant(ConstantExpression node, Void context)
            {
                if (node.getValue() == null) {
                    return new NullLiteral();
                }
                if (node.getType() == BOOLEAN) {
                    return new BooleanLiteral(Boolean.toString((Boolean) node.getValue()));
                }
                else if (node.getType() == INTEGER) {
                    return new LongLiteral(Integer.toString((Integer) node.getValue()));
                }
                else if (node.getType() == BIGINT) {
                    return new LongLiteral(Long.toString((Long) node.getValue()));
                }
                else if (node.getType() == DOUBLE) {
                    return new DoubleLiteral(Double.toString((Double) node.getValue()));
                }
                else if (node.getType() == VARCHAR) {
                    return new StringLiteral((String) node.getValue());
                }
                else if (node.getType() == DateType.DATE) {
                    return new GenericLiteral("DATE", new SqlDate(toIntExact((Long) node.getValue())).toString());
                }
                else if (node.getType() == TimestampType.TIMESTAMP) {
                    return new TimestampLiteral(Long.toString((Long) node.getValue()));
                }
                throw new IllegalStateException("Unsupported type found in Constant Expression : " + node.getType());
            }

            @Override
            public Expression visitLambda(LambdaDefinitionExpression lambda, Void context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Expression visitVariableReference(VariableReferenceExpression reference, Void context)
            {
                return new Identifier(reference.getName());
            }

            @Override
            public Expression visitSpecialForm(SpecialFormExpression node, Void context)
            {
                switch (node.getForm()) {
                    case AND:
                    case OR:
                    case IS_NULL: {
                        throw new UnsupportedOperationException();
                    }
                    case IN: {
                        checkState(node.getArguments().size() >= 2);
                        RowExpression target = node.getArguments().get(0);
                        List<RowExpression> values = node.getArguments().subList(1, node.getArguments().size());
                        checkState(!values.isEmpty(), "values should never be empty");
                        InListExpression inListExpression = new InListExpression(values.stream()
                                .map(rowExpression -> rowExpression.accept(this, context))
                                .collect(toImmutableList()));

                        Expression targetExpression = target.accept(this, context);
                        if (!(targetExpression instanceof Identifier)) {
                            throw new IllegalArgumentException("Target Expression is not an Identifier. Found : " + targetExpression.getClass());
                        }
                        return new InPredicate(targetExpression, inListExpression);
                    }

                    default:
                        throw new UnsupportedOperationException("Unknown Special Form type found : " + node.getForm().getClass());
                }
            }
        }, null);
    }
}
