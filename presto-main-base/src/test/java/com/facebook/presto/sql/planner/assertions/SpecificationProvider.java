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

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class SpecificationProvider
        implements ExpectedValueProvider<DataOrganizationSpecification>
{
    private final List<SymbolAlias> partitionBy;
    private final List<SymbolAlias> orderBy;
    private final Map<SymbolAlias, SortOrder> orderings;

    SpecificationProvider(
            List<SymbolAlias> partitionBy,
            List<SymbolAlias> orderBy,
            Map<SymbolAlias, SortOrder> orderings)
    {
        this.partitionBy = ImmutableList.copyOf(requireNonNull(partitionBy, "partitionBy is null"));
        this.orderBy = ImmutableList.copyOf(requireNonNull(orderBy, "orderBy is null"));
        this.orderings = ImmutableMap.copyOf(requireNonNull(orderings, "orderings is null"));
    }

    @Override
    public DataOrganizationSpecification getExpectedValue(SymbolAliases aliases)
    {
        Optional<OrderingScheme> orderingScheme = Optional.empty();
        if (!orderBy.isEmpty()) {
            orderingScheme = Optional.of(new OrderingScheme(
                    orderings
                            .entrySet()
                            .stream()
                            .map(entry -> new Ordering(
                                    new VariableReferenceExpression(Optional.empty(), entry.getKey().toSymbol(aliases).getName(), UNKNOWN),
                                    entry.getValue()))
                            .collect(toImmutableList())));
        }

        return new DataOrganizationSpecification(
                partitionBy
                        .stream()
                        .map(alias -> new VariableReferenceExpression(Optional.empty(), alias.toSymbol(aliases).getName(), UNKNOWN))
                        .collect(toImmutableList()),
                orderingScheme);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionBy", this.partitionBy)
                .add("orderBy", this.orderBy)
                .add("orderings", this.orderings)
                .toString();
    }

    /*
     * Since plan matching is done through SymbolAlias, which does not include type information, we cannot directly use
     * VariableReferenceExpression::equals to check whether two specification are equivalent once they include VariableReferenceExpression.
     * TODO Directly use equals once SymbolAlias is converted to something with type information.
     */
    public static boolean matchSpecification(DataOrganizationSpecification actual, DataOrganizationSpecification expected)
    {
        return actual.getPartitionBy().stream().map(VariableReferenceExpression::getName).collect(toImmutableList())
                .equals(expected.getPartitionBy().stream().map(VariableReferenceExpression::getName).collect(toImmutableList())) &&
                actual.getOrderingScheme().map(orderingScheme -> orderingScheme.getOrderByVariables().stream()
                                .map(VariableReferenceExpression::getName)
                                .collect(toImmutableSet())
                                .equals(expected.getOrderingScheme().get().getOrderByVariables().stream()
                                        .map(VariableReferenceExpression::getName)
                                        .collect(toImmutableSet())) &&
                                orderingScheme.getOrderingsMap().entrySet().stream()
                                        .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue))
                                        .equals(expected.getOrderingScheme().get().getOrderingsMap().entrySet().stream()
                                                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue))))
                        .orElse(true);
    }

    public static boolean matchSpecification(DataOrganizationSpecification actual, SpecificationProvider expected)
    {
        return actual.getPartitionBy().stream().map(VariableReferenceExpression::getName).collect(toImmutableList())
                .equals(expected.partitionBy.stream().map(SymbolAlias::toString).collect(toImmutableList())) &&
                actual.getOrderingScheme().map(orderingScheme -> orderingScheme.getOrderByVariables().stream()
                                .map(VariableReferenceExpression::getName)
                                .collect(toImmutableSet())
                                .equals(expected.orderBy.stream()
                                        .map(SymbolAlias::toString)
                                        .collect(toImmutableSet())) &&
                                orderingScheme.getOrderingsMap().entrySet().stream()
                                        .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue))
                                        .equals(expected.orderings.entrySet().stream()
                                                .collect(toImmutableMap(entry -> entry.getKey().toString(), Map.Entry::getValue))))
                        .orElse(true);
    }
}
