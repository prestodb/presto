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

package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

final class WindowMatcher
        implements Matcher
{
    private final WindowNode.Specification specification;
    private final List<FunctionCall> functionCalls;

    WindowMatcher(WindowNode.Specification specification, List<FunctionCall> functionCalls)
    {
        this.specification = specification;
        this.functionCalls = ImmutableList.copyOf(requireNonNull(functionCalls, "functionCalls is null"));
    }

    private static boolean samePartitionAndOrder(
            WindowNode.Specification expected,
            WindowNode.Specification actual)
    {
        if (!expected.getPartitionBy().equals(actual.getPartitionBy()) ||
                !expected.getOrderBy().equals(actual.getOrderBy())) {
            return false;
        }

        Map<Symbol, SortOrder> expectedOrderings = expected.getOrderings();
        Map<Symbol, SortOrder> actualOrderings = actual.getOrderings();

        if (expectedOrderings.size() != actualOrderings.size()) {
            return false;
        }

        /*
         * Ooof. Sad story time.
         *
         * Symbols in the plan get decorated with a number if the same symbol is used twice. E.g. if two parts
         * of a query both refer to a column foo, you'll end up with symbols foo and foo_32 (for example).
         *
         * When we verify the orderings in a plan, the orderings take the form of a Map<Symbol, SortOrder>.
         * Unfortunately, we have no way of knowing a priori what the number affixed to the symbol name, and
         * even if we did, we'd be relying on implementation details of the SymbolAllocator.
         *
         * What's further unfortunate is that we can't just extend Symbol with something that compares equal
         * to a Symbol with a name that has a unique id affixed. This is because we end up going through
         * Map.equals, and if you dig deep enough you end up finding that the hashCodes would have to match too.
         * Since we can't change how Symbol.hashCode is implemented (and wouldn't want to!), we're back to
         * needing to know the unknowable.
         *
         * Another approach to making to WindowNode.Specifications compare with .equals() that doesn't work
         * is extending Map to do the comparison in a way that doesn't rely on hashCode. That doesn't work
         * because WindowNode.Specification copies the members of the map you pass to its constructor.
         *
         * Instead, we implement the comparison here in a way that doesn't rely on hashCode. This is brittle
         * because if somebody adds another field to Specification that's included in a equals comparison,
         * this need to be updated too.
         */
        Comparator<Map.Entry<Symbol, SortOrder>> entryComparator =
                (Map.Entry<Symbol, SortOrder> x, Map.Entry<Symbol, SortOrder> y) -> x.getKey().compareTo(y.getKey());

        List<Map.Entry<Symbol, SortOrder>> actualEntries = actualOrderings
                .entrySet()
                .stream()
                .sorted(entryComparator)
                .collect(toImmutableList());

        List<Map.Entry<Symbol, SortOrder>> expectedEntries = expectedOrderings
                .entrySet()
                .stream()
                .sorted(entryComparator)
                .collect(toImmutableList());

        return expectedEntries.equals(actualEntries);
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        if (!(node instanceof WindowNode)) {
            return false;
        }

        WindowNode windowNode = (WindowNode) node;

        if (!samePartitionAndOrder(specification, windowNode.getSpecification())) {
            return false;
        }

        LinkedList<FunctionCall> actualCalls = windowNode.getWindowFunctions().values().stream()
                .map(WindowNode.Function::getFunctionCall)
                .collect(Collectors.toCollection(LinkedList::new));

        if (actualCalls.size() != functionCalls.size()) {
            return false;
        }

        for (FunctionCall expectedCall : functionCalls) {
            if (!actualCalls.remove(expectedCall)) {
                // Found an expectedCall not in expectedCalls.
                return false;
            }
        }

        // expectedCalls was missing something in actualCalls.
        return actualCalls.isEmpty();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("specification", specification)
                .add("functionCalls", functionCalls)
                .toString();
    }
}
