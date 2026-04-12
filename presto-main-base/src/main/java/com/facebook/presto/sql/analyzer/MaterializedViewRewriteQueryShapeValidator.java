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

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.QuerySpecification;

import java.util.Optional;

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
    }
}
