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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.tree.CurrentTime;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.Extract;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.WhenClause;

import java.util.Optional;

public class CanonicalizeExpressionRewriter
{
    private CanonicalizeExpressionRewriter() {}

    public static Expression canonicalizeExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteIsNotNullPredicate(IsNotNullPredicate node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression value = treeRewriter.rewrite(node.getValue(), context);
            return new NotExpression(new IsNullPredicate(value));
        }

        @Override
        public Expression rewriteIfExpression(IfExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression condition = treeRewriter.rewrite(node.getCondition(), context);
            Expression trueValue = treeRewriter.rewrite(node.getTrueValue(), context);

            Optional<Expression> falseValue = node.getFalseValue().map((value) -> treeRewriter.rewrite(value, context));

            return new SearchedCaseExpression(ImmutableList.of(new WhenClause(condition, trueValue)), falseValue);
        }

        @Override
        public Expression rewriteCurrentTime(CurrentTime node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getPrecision() != null) {
                throw new UnsupportedOperationException("not yet implemented: non-default precision");
            }

            switch (node.getFunction()) {
                case DATE:
                    return new FunctionCall(QualifiedName.of("current_date"), ImmutableList.of());
                case TIME:
                    return new FunctionCall(QualifiedName.of("current_time"), ImmutableList.of());
                case LOCALTIME:
                    return new FunctionCall(QualifiedName.of("localtime"), ImmutableList.of());
                case TIMESTAMP:
                    return new FunctionCall(QualifiedName.of("current_timestamp"), ImmutableList.of());
                case LOCALTIMESTAMP:
                    return new FunctionCall(QualifiedName.of("localtimestamp"), ImmutableList.of());
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getFunction());
            }
        }

        @Override
        public Expression rewriteExtract(Extract node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression value = treeRewriter.rewrite(node.getExpression(), context);

            switch (node.getField()) {
                case YEAR:
                    return new FunctionCall(QualifiedName.of("year"), ImmutableList.of(value));
                case QUARTER:
                    return new FunctionCall(QualifiedName.of("quarter"), ImmutableList.of(value));
                case MONTH:
                    return new FunctionCall(QualifiedName.of("month"), ImmutableList.of(value));
                case WEEK:
                    return new FunctionCall(QualifiedName.of("week"), ImmutableList.of(value));
                case DAY:
                case DAY_OF_MONTH:
                    return new FunctionCall(QualifiedName.of("day"), ImmutableList.of(value));
                case DAY_OF_WEEK:
                case DOW:
                    return new FunctionCall(QualifiedName.of("day_of_week"), ImmutableList.of(value));
                case DAY_OF_YEAR:
                case DOY:
                    return new FunctionCall(QualifiedName.of("day_of_year"), ImmutableList.of(value));
                case YEAR_OF_WEEK:
                case YOW:
                    return new FunctionCall(QualifiedName.of("year_of_week"), ImmutableList.of(value));
                case HOUR:
                    return new FunctionCall(QualifiedName.of("hour"), ImmutableList.of(value));
                case MINUTE:
                    return new FunctionCall(QualifiedName.of("minute"), ImmutableList.of(value));
                case SECOND:
                    return new FunctionCall(QualifiedName.of("second"), ImmutableList.of(value));
                case TIMEZONE_MINUTE:
                    return new FunctionCall(QualifiedName.of("timezone_minute"), ImmutableList.of(value));
                case TIMEZONE_HOUR:
                    return new FunctionCall(QualifiedName.of("timezone_hour"), ImmutableList.of(value));
            }

            throw new UnsupportedOperationException("not yet implemented: " + node.getField());
        }
    }
}
