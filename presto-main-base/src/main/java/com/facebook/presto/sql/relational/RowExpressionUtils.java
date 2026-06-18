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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;

import static java.util.Objects.requireNonNull;

public class RowExpressionUtils
{
    private RowExpressionUtils() {}

    public static boolean containsNonCoordinatorEligibleCallExpression(FunctionAndTypeManager functionAndTypeManager, RowExpression expression)
    {
        return expression.accept(new ContainsNonCoordinatorEligibleCallExpressionVisitor(functionAndTypeManager), null);
    }

    private static class ContainsNonCoordinatorEligibleCallExpressionVisitor
            implements RowExpressionVisitor<Boolean, Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        public ContainsNonCoordinatorEligibleCallExpressionVisitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        @Override
        public Boolean visitCall(CallExpression call, Void context)
        {
            // If the call is not a Java function, we return true to indicate that we found a non-Java expression
            FunctionHandle functionHandle = call.getFunctionHandle();
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(functionHandle);
            if (!functionMetadata.getImplementationType().canBeEvaluatedInCoordinator()) {
                return true;
            }
            for (RowExpression argument : call.getArguments()) {
                if (argument.accept(this, context)) {
                    return true; // Found a non-Java expression in arguments
                }
            }
            return false;
        }

        @Override
        public Boolean visitExpression(RowExpression expression, Void context)
        {
            for (RowExpression child : expression.getChildren()) {
                if (child.accept(this, context)) {
                    return true; // Found a non-Java expression
                }
            }
            return false;
        }
    }
}
