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

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class ValuesNode
        extends PlanNode
{
    private final List<Symbol> outputSymbols;
    private final List<List<Expression>> rows;

    @JsonCreator
    public ValuesNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("rows") List<List<Expression>> rows)
    {
        super(id);
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
        this.rows = ImmutableList.copyOf(rows);

        for (List<Expression> row : rows) {
            checkArgument(row.size() == outputSymbols.size() || row.size() == 0,
                            "Expected row to have %s values, but row has %s values", outputSymbols.size(), row.size());
        }
    }

    @Override
    @JsonProperty
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty
    public List<List<Expression>> getRows()
    {
        return rows;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitValues(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }
}
