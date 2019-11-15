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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.ConnectorSession;

import java.util.function.Function;

public interface ExpressionOptimizer
{
    /**
     * Optimize a RowExpression to
     */
    RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session);

    Object optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver);

    enum Level
    {
        /**
         * SERIALIZABLE guarantees the optimized RowExpression can be serialized and deserialized.
         */
        SERIALIZABLE,
        /**
         * OPTIMIZED removes all redundancy in a RowExpression but can end up with non-serializable objects (e.g., Regex).
         */
        OPTIMIZED,
        /**
         * EVALUATE attempt to evaluate the RowExpression into a constant, even though it can be non-deterministic.
         */
        EVALUATED
    }
}
