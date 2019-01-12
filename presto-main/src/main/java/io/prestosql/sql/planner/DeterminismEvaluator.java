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
package io.prestosql.sql.planner;

import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Determines whether a given Expression is deterministic
 */
public final class DeterminismEvaluator
{
    private DeterminismEvaluator() {}

    public static boolean isDeterministic(Expression expression)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean deterministic = new AtomicBoolean(true);
        new Visitor().process(expression, deterministic);
        return deterministic.get();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, AtomicBoolean>
    {
        @Override
        protected Void visitFunctionCall(FunctionCall node, AtomicBoolean deterministic)
        {
            // TODO: total hack to figure out if a function is deterministic. martint should fix this when he refactors the planning code
            if (node.getName().equals(QualifiedName.of("rand")) ||
                    node.getName().equals(QualifiedName.of("random")) ||
                    node.getName().equals(QualifiedName.of("shuffle"))) {
                deterministic.set(false);
            }
            return super.visitFunctionCall(node, deterministic);
        }
    }
}
