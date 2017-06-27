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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;

import java.util.Iterator;
import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.rule.Util.transpose;
import static com.facebook.presto.sql.planner.optimizations.WindowNodeUtil.dependsOn;

public class SwapAdjacentWindowsBySpecifications
        implements Rule
{
    private static final Pattern PATTERN = Pattern.matchByClass(WindowNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        WindowNode parent = (WindowNode) node;

        PlanNode child = lookup.resolve(parent.getSource());
        if (!(child instanceof WindowNode)) {
            return Optional.empty();
        }

        if ((compare(parent, (WindowNode) child) < 0) && (!dependsOn(parent, (WindowNode) child))) {
            return Optional.of(transpose(parent, child));
        }
        else {
            return Optional.empty();
        }
    }

    private static int compare(WindowNode o1, WindowNode o2)
    {
        int comparison = comparePartitionBy(o1, o2);
        if (comparison != 0) {
            return comparison;
        }

        comparison = compareOrderBy(o1, o2);
        if (comparison != 0) {
            return comparison;
        }

        // If PartitionBy and OrderBy clauses are identical, let's establish an arbitrary order to prevent non-deterministic results of swapping WindowNodes in such a case
        return o1.getId().toString().compareTo(o2.getId().toString());
    }

    private static int comparePartitionBy(WindowNode o1, WindowNode o2)
    {
        Iterator<Symbol> iterator1 = o1.getPartitionBy().iterator();
        Iterator<Symbol> iterator2 = o2.getPartitionBy().iterator();

        while (iterator1.hasNext() && iterator2.hasNext()) {
            Symbol symbol1 = iterator1.next();
            Symbol symbol2 = iterator2.next();

            int partitionByComparison = symbol1.compareTo(symbol2);
            if (partitionByComparison != 0) {
                return partitionByComparison;
            }
        }

        if (iterator1.hasNext()) {
            return 1;
        }
        if (iterator2.hasNext()) {
            return -1;
        }
        return 0;
    }

    private static int compareOrderBy(WindowNode o1, WindowNode o2)
    {
        Iterator<Symbol> iterator1 = o1.getOrderBy().iterator();
        Iterator<Symbol> iterator2 = o2.getOrderBy().iterator();

        while (iterator1.hasNext() && iterator2.hasNext()) {
            Symbol symbol1 = iterator1.next();
            Symbol symbol2 = iterator2.next();

            int orderByComparison = symbol1.compareTo(symbol2);
            if (orderByComparison != 0) {
                return orderByComparison;
            }
            else {
                int sortOrderComparison = o1.getOrderings().get(symbol1).compareTo(o2.getOrderings().get(symbol2));
                if (sortOrderComparison != 0) {
                    return sortOrderComparison;
                }
            }
        }

        if (iterator1.hasNext()) {
            return 1;
        }
        if (iterator2.hasNext()) {
            return -1;
        }
        return 0;
    }
}
