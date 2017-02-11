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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.LongLiteral;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * If the predicate for a delete is optimized to false, the target table scan
 * of the delete will be replaced with an empty values node. This type of
 * plan cannot be executed and is meaningless anyway, so we replace the
 * entire thing with a values node.
 * <p>
 * Transforms
 * <pre>
 *  - TableFinish
 *    - Exchange
 *      - Delete
 *        - empty Values
 * </pre>
 * into
 * <pre>
 *  - Values (0)
 * </pre>
 */
public class RemoveEmptyDelete
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        // TODO split into multiple rules (https://github.com/prestodb/presto/issues/7292)

        return tryCast(node, TableFinishNode.class)
                .flatMap(tableFinishNode -> tryCast(lookup.resolve(tableFinishNode.getSource()), ExchangeNode.class))
                .filter(exchangeNode -> exchangeNode.getSources().size() == 1)
                .flatMap(exchangeNode -> tryCast(lookup.resolve(getOnlyElement(exchangeNode.getSources())), DeleteNode.class))
                .flatMap(deleteNode -> tryCast(lookup.resolve(deleteNode.getSource()), ValuesNode.class))
                .filter(valuesNode -> valuesNode.getRows().isEmpty())
                .map(valuesNode -> new ValuesNode(
                        node.getId(),
                        node.getOutputSymbols(),
                        ImmutableList.of(ImmutableList.of(new LongLiteral("0"))))
                );
    }

    private static <T> Optional<T> tryCast(Object input, Class<T> desiredClass)
    {
        requireNonNull(input, "input cannot be null");
        requireNonNull(desiredClass, "desiredClass cannot be null");

        if (desiredClass.isInstance(input)) {
            return Optional.of(desiredClass.cast(input));
        }
        else {
            return Optional.empty();
        }
    }
}
