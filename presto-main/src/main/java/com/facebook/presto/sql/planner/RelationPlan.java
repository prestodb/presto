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

import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class RelationPlan
{
    private final PlanNode root;
    private final List<Symbol> outputSymbols;
    private final TupleDescriptor descriptor;

    public RelationPlan(PlanNode root, TupleDescriptor descriptor, List<Symbol> outputSymbols)
    {
        checkNotNull(root, "root is null");
        checkNotNull(outputSymbols, "outputSymbols is null");
        checkNotNull(descriptor, "descriptor is null");

        checkArgument(descriptor.getFields().size() == outputSymbols.size(), "Number of outputs (%s) doesn't match descriptor size (%s)", outputSymbols.size(), descriptor.getFields().size());

        this.root = root;
        this.descriptor = descriptor;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
    }

    public Symbol getSymbol(int fieldIndex)
    {
        Preconditions.checkArgument(fieldIndex >= 0 && fieldIndex < outputSymbols.size() && outputSymbols.get(fieldIndex) != null, "No field->symbol mapping for field %s", fieldIndex);
        return outputSymbols.get(fieldIndex);
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    public TupleDescriptor getDescriptor()
    {
        return descriptor;
    }
}
