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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;

@Immutable
public class FilterNode
        extends PlanNode
{
    private final PlanNode source;
    private final Expression predicate;
    private final Expression predicateWithoutTupleDomain;

    @JsonCreator
    public FilterNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
                      @JsonProperty("predicate") Expression predicate,
                      @JsonProperty("predicateWithoutTupleDomain") Expression predicateWithoutTupleDomain)
    {
        super(id);

        this.source = source;
        this.predicate = predicate;
        this.predicateWithoutTupleDomain = predicateWithoutTupleDomain;
    }

    public FilterNode(PlanNodeId id,
                       PlanNode source,
                       Expression predicate)
    {
        super(id);

        this.source = source;
        this.predicate = predicate;
        this.predicateWithoutTupleDomain = TRUE_LITERAL;
    }

    @JsonProperty("predicate")
    public Expression getPredicate()
    {
        return predicate;
    }

    @JsonProperty("predicateWithoutTupleDomain")
    public Expression getPredicateWithoutTupleDomain()
    {
        return predicateWithoutTupleDomain;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitFilter(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new FilterNode(getId(), Iterables.getOnlyElement(newChildren), predicate, predicateWithoutTupleDomain);
    }
}
