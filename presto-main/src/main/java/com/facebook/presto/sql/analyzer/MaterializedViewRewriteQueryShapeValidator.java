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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuerySpecification;

import java.util.Optional;

import static com.facebook.presto.sql.MaterializedViewUtils.SUPPORTED_FUNCTION_CALLS;
import static java.lang.String.format;

/**
 * Validates that the {@link QuerySpecification} provided to
 * {@link MaterializedViewRewriteQueryShapeValidator#validate}
 * is able to be rewritten from a backing materialized view. This does not guarantee the
 * QuerySpecification has a compatible materialized view to be rewritten with, only that
 * its shape is not prohibitive to the optimization.
 */
public final class MaterializedViewRewriteQueryShapeValidator
{
    private MaterializedViewRewriteQueryShapeValidator() {}

    static Optional<String> validate(QuerySpecification querySpecification)
    {
        return new Visitor().validateMaterializedViewOptimizationQueryShape(querySpecification);
    }

    static class Visitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private Optional<String> errorMessage = Optional.empty();
        boolean hasGroupBy;

        public Optional<String> validateMaterializedViewOptimizationQueryShape(QuerySpecification querySpecification)
        {
            if (querySpecification.getHaving().isPresent()) {
                errorMessage = Optional.of("Query shape invalid: HAVING is not supported for materialized view optimizations");
            }
            else if (!querySpecification.getFrom().isPresent()) {
                errorMessage = Optional.of("Query shape invalid: QuerySpecification without from clause is not supported for materialized view optimization");
            }
            else {
                process(querySpecification);
            }

            // TODO: add a check requiring GROUP BY or WHERE to be present
            return errorMessage;
        }

        @Override
        protected Void visitGroupBy(GroupBy node, Void context)
        {
            hasGroupBy = true;
            return super.visitGroupBy(node, context);
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context)
        {
            QualifiedName name = node.getName();
            if (!SUPPORTED_FUNCTION_CALLS.contains(name)) {
                errorMessage = Optional.of(format("Query shape invalid: %s function is not supported for materialized view optimizations", name));
                return null;
            }
            return super.visitFunctionCall(node, context);
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Void context)
        {
            errorMessage = Optional.of("All columns rewrite is not supported in query optimizer");
            return null;
        }
    }
}
