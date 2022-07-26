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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class EnforceSingleRowNode
        extends PlanNode
{
    private final PlanNode source;

    @JsonCreator
    public EnforceSingleRowNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source)
    {
        super(sourceLocation, id);

        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.singletonList(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitEnforceSingleRow(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "Unexpected number of elements in list newChildren");
        return new EnforceSingleRowNode(getSourceLocation(), getId(), newChildren.get(0));
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
