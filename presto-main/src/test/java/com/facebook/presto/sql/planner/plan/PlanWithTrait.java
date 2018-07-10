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
import com.facebook.presto.sql.planner.iterative.Trait;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PlanWithTrait
        extends PlanNode
{
    private final Trait trait;

    public PlanWithTrait(PlanNodeId id, Trait trait)
    {
        super(id);
        this.trait = requireNonNull(trait, "trait is null");
    }

    public Trait getTrait()
    {
        return trait;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return this;
    }
}
