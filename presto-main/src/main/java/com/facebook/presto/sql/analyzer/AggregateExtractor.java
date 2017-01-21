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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

class AggregateExtractor
        extends DefaultExpressionTraversalVisitor<Void, Void>
{
    private final FunctionRegistry functionRegistry;

    private final ImmutableList.Builder<FunctionCall> aggregates = ImmutableList.builder();

    public AggregateExtractor(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, Void context)
    {
        if ((functionRegistry.isAggregationFunction(node.getName()) || node.getFilter().isPresent()) && !node.getWindow().isPresent()) {
            aggregates.add(node);
            return null;
        }

        return super.visitFunctionCall(node, null);
    }

    public List<FunctionCall> getAggregates()
    {
        return aggregates.build();
    }
}
