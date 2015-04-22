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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;

import java.util.Map;

public class Plan
{
    private final PlanNode root;
    private final SymbolAllocator symbolAllocator;

    public Plan(PlanNode root, SymbolAllocator symbolAllocator)
    {
        Preconditions.checkNotNull(root, "root is null");
        Preconditions.checkNotNull(symbolAllocator, "symbolAllocator is null");

        this.root = root;
        this.symbolAllocator = symbolAllocator;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public Map<Symbol, Type> getTypes()
    {
        return symbolAllocator.getTypes();
    }

    public SymbolAllocator getSymbolAllocator()
    {
        return symbolAllocator;
    }
}
