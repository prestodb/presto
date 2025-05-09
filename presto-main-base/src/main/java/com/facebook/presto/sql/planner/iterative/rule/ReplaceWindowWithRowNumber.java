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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.BuiltInFunctionHandle;
import com.facebook.presto.spi.function.CatalogSchemaFunctionName;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.RowNumberNode;

import java.util.Optional;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.sql.planner.plan.Patterns.window;
import static com.google.common.collect.Iterables.getOnlyElement;

public class ReplaceWindowWithRowNumber
        implements Rule<WindowNode>
{
    private static final CatalogSchemaFunctionName ROW_NUMBER_NAME = new CatalogSchemaFunctionName(JAVA_BUILTIN_NAMESPACE.getCatalogName(), JAVA_BUILTIN_NAMESPACE.getSchemaName(), "row_number");

    private final Pattern<WindowNode> pattern;

    public ReplaceWindowWithRowNumber()
    {
        this.pattern = window()
                .matching(window -> {
                    if (window.getWindowFunctions().size() != 1) {
                        return false;
                    }
                    FunctionHandle handle = getOnlyElement(window.getWindowFunctions().values()).getFunctionHandle();
                    if (handle instanceof BuiltInFunctionHandle) {
                        QualifiedObjectName functionName = ((BuiltInFunctionHandle) handle).getSignature().getName();
                        return handle.getArgumentTypes().isEmpty() && new CatalogSchemaFunctionName(functionName.getCatalogName(),
                                functionName.getSchemaName(), functionName.getObjectName()).equals(ROW_NUMBER_NAME);
                    }
                    return false;
                })
                .matching(window -> !window.getOrderingScheme().isPresent());
    }

    @Override
    public Pattern<WindowNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(WindowNode node, Captures captures, Context context)
    {
        return Result.ofPlanNode(new RowNumberNode(
                node.getSourceLocation(),
                node.getId(),
                node.getSource(),
                node.getPartitionBy(),
                getOnlyElement(node.getWindowFunctions().keySet()),
                Optional.empty(),
                false,
                Optional.empty()));
    }
}
