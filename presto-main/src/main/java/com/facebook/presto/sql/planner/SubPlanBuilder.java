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

import com.facebook.presto.sql.planner.PlanFragment.Partitioning;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;

public class SubPlanBuilder
{
    private final PlanFragmentId id;
    private final SymbolAllocator allocator;
    private final Partitioning partitioning;
    private final PlanNodeId partitionedSource;

    private PlanNode root;
    private List<SubPlan> children = new ArrayList<>();

    public SubPlanBuilder(PlanFragmentId id, SymbolAllocator allocator, Partitioning partitioning, PlanNode root, PlanNodeId partitionedSource)
    {
        this.partitionedSource = partitionedSource;
        this.id = checkNotNull(id, "id is null");
        this.allocator = checkNotNull(allocator, "allocator is null");
        this.root = checkNotNull(root, "root is null");
        this.partitioning = checkNotNull(partitioning, "partitioning is null");
    }

    public PlanFragmentId getId()
    {
        return id;
    }

    public boolean isPartitioned()
    {
        return partitioning != Partitioning.NONE;
    }

    public Partitioning getPartitioning()
    {
        return partitioning;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public SubPlanBuilder setRoot(PlanNode root)
    {
        checkNotNull(root, "root is null");
        this.root = root;
        return this;
    }

    public List<SubPlan> getChildren()
    {
        return children;
    }

    public SubPlanBuilder setChildren(Iterable<SubPlan> children)
    {
        this.children = Lists.newArrayList(children);
        return this;
    }

    public SubPlanBuilder addChild(SubPlan child)
    {
        this.children.add(child);
        return this;
    }

    public SubPlan build()
    {
        Set<Symbol> dependencies = SymbolExtractor.extract(root);

        PlanFragment fragment = new PlanFragment(id, root, Maps.filterKeys(allocator.getTypes(), in(dependencies)), partitioning, partitionedSource);

        return new SubPlan(fragment, children);
    }
}
