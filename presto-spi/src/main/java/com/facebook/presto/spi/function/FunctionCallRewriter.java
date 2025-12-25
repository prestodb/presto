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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;

import java.util.Optional;

/**
 * Interface for rewriting function calls during query optimization.
 * Implementations can transform specific function calls into different expressions.
 *
 * This provides a pluggable extension point for modules to transform function calls
 * into alternative implementations during query planning.
 *
 * Implementations should be registered with FunctionCallRewriterManager to be
 * invoked by the FunctionCallRewriting rule during query optimization.
 */
public interface FunctionCallRewriter
{
    /**
     * Attempt to rewrite a function call.
     *
     * @param call The function call expression to potentially rewrite
     * @param session The connector session
     * @param variableAllocator Allocator for creating new variables during rewriting
     * @param idAllocator Allocator for creating new plan node IDs
     * @param functionResolution Resolution for looking up functions (e.g., casts, built-in functions)
     * @return Optional containing the rewritten expression, or empty if this rewriter doesn't handle this call
     */
    Optional<RowExpression> rewrite(
            CallExpression call,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            StandardFunctionResolution functionResolution);
}
