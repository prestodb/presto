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
package com.facebook.presto.sql.fingerprint;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Piggybacks on the ExpresionFormatter.Formatter class, and overrides only literal visit methods
 * to stub them out with a wildcard character (a question mark: ?).
 */
public class FingerprintExpressionFormatter
        extends ExpressionFormatter.Formatter
{
    private final FingerprintVisitorContext context;

    public FingerprintExpressionFormatter(FingerprintVisitorContext context, Optional<List<Expression>> parameters)
    {
        super(parameters);
        this.context = context;
    }

    @Override
    public String visitStringLiteral(StringLiteral node, Void context)
    {
        return "?";
    }

    @Override
    public String visitBooleanLiteral(BooleanLiteral node, Void context)
    {
        return "?";
    }

    @Override
    public String visitCharLiteral(CharLiteral node, Void context)
    {
        return "?";
    }

    @Override
    public String visitBinaryLiteral(BinaryLiteral node, Void context)
    {
        return "?";
    }

    @Override
    public String visitLongLiteral(LongLiteral node, Void context)
    {
        return "?";
    }

    @Override
    public String visitDecimalLiteral(DecimalLiteral node, Void context)
    {
        return "?";
    }

    @Override
    public String visitTimeLiteral(TimeLiteral node, Void context)
    {
        return "?";
    }

    @Override
    protected String visitTimestampLiteral(TimestampLiteral node, Void context)
    {
        return "?";
    }

    @Override
    public String visitIntervalLiteral(IntervalLiteral node, Void context)
    {
        String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "- " : "";
        StringBuilder builder = new StringBuilder()
                .append("INTERVAL ")
                .append(sign)
                .append(" '").append("?").append("' ")
                .append(node.getStartField());

        if (node.getEndField().isPresent()) {
            builder.append(" TO ").append(node.getEndField().get());
        }
        return builder.toString();
    }

    @Override
    public String visitDoubleLiteral(DoubleLiteral node, Void context)
    {
        return "?";
    }

    @Override
    public String visitInListExpression(InListExpression node, Void context)
    {
        return "( [?] )";
    }

    @Override
    protected String visitSubqueryExpression(SubqueryExpression node, Void context)
    {
        return "(" + FingerprintCreator.format(node.getQuery()) + ")";
    }

    @Override
    protected String visitExists(ExistsPredicate node, Void context)
    {
        return "(EXISTS " + FingerprintCreator.format(node.getSubquery()) + ")";
    }

    @Override
    protected String visitArrayConstructor(ArrayConstructor node, Void context)
    {
        ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
        for (Expression value : node.getValues()) {
            valueStrings.add(FingerprintCreator.format(value));
        }
        return "ARRAY[" + Joiner.on(",").join(valueStrings.build()) + "]";
    }

    @Override
    protected String visitSubscriptExpression(SubscriptExpression node, Void context)
    {
        return FingerprintCreator.format(node.getBase()) + "[" + FingerprintCreator.format(node.getIndex()) + "]";
    }
}
