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

import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TryExpression;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public final class NullabilityAnalyzer
{
    private NullabilityAnalyzer() {}

    /**
     * TODO: this currently produces a very conservative estimate.
     * We need to narrow down the conditions under which certain constructs
     * can return null (e.g., if(a, b, c) might return null for non-null a
     * only if b or c can be null.
     */
    public static boolean mayReturnNullOnNonNullInput(Expression expression)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean result = new AtomicBoolean(false);
        new Visitor().process(expression, result);
        return result.get();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, AtomicBoolean>
    {
        @Override
        protected Void visitCast(Cast node, AtomicBoolean result)
        {
            // try_cast and cast(JSON 'null' AS ...) can return null
            result.set(true);
            return null;
        }

        @Override
        protected Void visitNullIfExpression(NullIfExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitInPredicate(InPredicate node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSearchedCaseExpression(SearchedCaseExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSimpleCaseExpression(SimpleCaseExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitTryExpression(TryExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitIfExpression(IfExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, AtomicBoolean result)
        {
            // TODO: this should look at whether the return type of the function is annotated with @SqlNullable
            result.set(true);
            return null;
        }
    }
}
