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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CurrentTimeRowExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.spi.relation.CurrentTimeRowExpression.Function.DATE;
import static com.facebook.presto.spi.relation.CurrentTimeRowExpression.Function.LOCALTIME;
import static com.facebook.presto.spi.relation.CurrentTimeRowExpression.Function.LOCALTIMESTAMP;
import static com.facebook.presto.spi.relation.CurrentTimeRowExpression.Function.TIME;
import static com.facebook.presto.spi.relation.CurrentTimeRowExpression.Function.TIMESTAMP;
import static com.facebook.presto.sql.relational.Expressions.call;
import static java.util.Objects.requireNonNull;

public class CanonicalizeRowExpressionRewriter
{
    private CanonicalizeRowExpressionRewriter() {}

    public static RowExpression canonicalizeExpression(FunctionAndTypeManager functionAndTypeManager, RowExpression expression)
    {
        return RowExpressionTreeRewriter.rewriteWith(new Visitor(functionAndTypeManager), expression);
    }

    private static class Visitor
            extends RowExpressionRewriter<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        private Visitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        @Override
        public RowExpression rewriteCurrentTime(CurrentTimeRowExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getPrecision() != null) {
                throw new UnsupportedOperationException("not yet implemented: non-default precision");
            }

            switch (node.getFunction()) {
                case DATE:
                    return call(functionAndTypeManager, "current_date", DATE.getType());
                case TIME:
                    return call(functionAndTypeManager, "current_time", TIME.getType());
                case LOCALTIME:
                    return call(functionAndTypeManager, "localtime", LOCALTIME.getType());
                case TIMESTAMP:
                    return call(functionAndTypeManager, "current_timestamp", TIMESTAMP.getType());
                case LOCALTIMESTAMP:
                    return call(functionAndTypeManager, "localtimestamp", LOCALTIMESTAMP.getType());
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getFunction());
            }
        }

        @Override
        public RowExpression rewriteExtract(Extract node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
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
