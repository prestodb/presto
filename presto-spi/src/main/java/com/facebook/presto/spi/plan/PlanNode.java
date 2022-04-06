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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The basic component of a Presto IR (logic plan).
 * An IR is a tree structure with each PlanNode performing a specific operation.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, property = "@type")
public abstract class PlanNode
{
    private final Optional<SourceLocation> sourceLocation;
    private final PlanNodeId id;

    protected PlanNode(Optional<SourceLocation> sourceLocation, PlanNodeId id)
    {
        this.sourceLocation = sourceLocation;
        requireNonNull(id, "id is null");
        this.id = id;
    }

    @JsonProperty("id")
    public PlanNodeId getId()
    {
        return id;
    }

    @JsonProperty("sourceLocation")
    public Optional<SourceLocation> getSourceLocation()
    {
        return sourceLocation;
    }

    /**
     * Get the upstream PlanNodes (i.e., children) of the current PlanNode.
     */
    public abstract List<PlanNode> getSources();

    /**
     * The output from the upstream PlanNodes.
     * It should serve as the input for the current PlanNode.
     */
    public abstract List<VariableReferenceExpression> getOutputVariables();

    /**
     * Alter the upstream PlanNodes of the current PlanNode.
     */
    public abstract PlanNode replaceChildren(List<PlanNode> newChildren);

    /**
     * Create a deep copy of the current PlanNode.
     */
//    TODO Uncomment this code
//    public abstract PlanNode deepCopy(
//            PlanNodeIdAllocator planNodeIdAllocator,
//            VariableAllocator variableAllocator,
//            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings);
    public PlanNode deepCopy(
            PlanNodeIdAllocator planNodeIdAllocator,
            VariableAllocator variableAllocator,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings)
    {
        return null;
    }

    public static List<VariableReferenceExpression> translateVariableReferences(
            List<VariableReferenceExpression> originalVariables,
            VariableAllocator variableAllocator,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings)
    {
        List<VariableReferenceExpression> newVariables = new ArrayList<>();
        for (int i = 0; i < originalVariables.size(); ++i) {
            if (!variableMappings.containsKey(originalVariables.get(i))) {
                variableMappings.put(
                        originalVariables.get(i),
                        variableAllocator.newVariable(originalVariables.get(i).getName(), originalVariables.get(i).getType()));
            }
            newVariables.add(variableMappings.get(originalVariables.get(i)));
        }
        return newVariables;
    }

    /**
     * A visitor pattern interface to operate on IR.
     */
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
