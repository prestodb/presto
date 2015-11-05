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

import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class RelationPlan
{
    private final PlanNode root;
    private final List<Symbol> outputSymbols;
    private final RelationType descriptor;
    private final Optional<Symbol> sampleWeight;

    public RelationPlan(PlanNode root, RelationType descriptor, List<Symbol> outputSymbols, Optional<Symbol> sampleWeight)
    {
        requireNonNull(root, "root is null");
        requireNonNull(outputSymbols, "outputSymbols is null");
        requireNonNull(descriptor, "descriptor is null");
        requireNonNull(descriptor, "sampleWeight is null");

        checkArgument(descriptor.getAllFieldCount() == outputSymbols.size(),
                "Number of outputs (%s) doesn't match descriptor size (%s)", outputSymbols.size(), descriptor.getAllFieldCount());

        this.root = root;
        this.descriptor = descriptor;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
        this.sampleWeight = sampleWeight;
    }

    public Optional<Symbol> getSampleWeight()
    {
        return sampleWeight;
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

    public RelationType getDescriptor()
    {
        return descriptor;
    }
}
