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
package io.prestosql.sql.analyzer;

import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FieldReference;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.NodeRef;

import java.util.Set;

import static io.prestosql.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;

public final class ConstantExpressionVerifier
{
    private ConstantExpressionVerifier() {}

    public static void verifyExpressionIsConstant(Set<NodeRef<Expression>> columnReferences, Expression expression)
    {
        new ConstantExpressionVerifierVisitor(columnReferences, expression).process(expression, null);
    }

    private static class ConstantExpressionVerifierVisitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final Set<NodeRef<Expression>> columnReferences;
        private final Expression expression;

        public ConstantExpressionVerifierVisitor(Set<NodeRef<Expression>> columnReferences, Expression expression)
        {
            this.columnReferences = columnReferences;
            this.expression = expression;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
            }

            process(node.getBase(), context);
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void context)
        {
            throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
        }

        @Override
        protected Void visitFieldReference(FieldReference node, Void context)
        {
            throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
        }
    }
}
