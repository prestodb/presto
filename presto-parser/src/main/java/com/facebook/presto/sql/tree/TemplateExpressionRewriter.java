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
package com.facebook.presto.sql.tree;

public class TemplateExpressionRewriter
{
    private TemplateExpressionRewriter() {}

    public static Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private int counter;

        private Visitor()
        {
            counter = 0;
        }

        @Override
        public Expression rewriteBinaryLiteral(BinaryLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteBooleanLiteral(BooleanLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteCharLiteral(CharLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteDecimalLiteral(DecimalLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteDoubleLiteral(DoubleLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteGenericLiteral(GenericLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteIntervalLiteral(IntervalLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteLongLiteral(LongLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteStringLiteral(StringLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteTimeLiteral(TimeLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteTimestampLiteral(TimestampLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteNullLiteral(NullLiteral node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        @Override
        public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return createParameter();
        }

        private Parameter createParameter()
        {
            return new Parameter(counter++);
        }
    }
}
