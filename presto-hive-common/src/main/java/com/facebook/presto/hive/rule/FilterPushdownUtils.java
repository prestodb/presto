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
package com.facebook.presto.hive.rule;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.hive.BaseHiveColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.hive.MetadataUtils.isEntireColumn;

public final class FilterPushdownUtils
{
    private FilterPushdownUtils() {}

    public static TupleDomain<Subfield> getDomainPredicate(
            DomainTranslator.ExtractionResult<Subfield> decomposedFilter,
            TupleDomain<ColumnHandle> unenforcedConstraint)
    {
        return withColumnDomains(ImmutableMap.<Subfield, Domain>builder()
                .putAll(unenforcedConstraint
                        .transform(columnHandle -> new Subfield(((BaseHiveColumnHandle) columnHandle).getName(), ImmutableList.of()))
                        .getDomains()
                        .orElse(ImmutableMap.of()))
                .putAll(decomposedFilter.getTupleDomain()
                        .transform(subfield -> !isEntireColumn(subfield) ? subfield : null)
                        .getDomains()
                        .orElse(ImmutableMap.of()))
                .build());
    }

    public static Set<String> getPredicateColumnNames(
            RowExpression optimizedRemainingExpression,
            TupleDomain<Subfield> domainPredicate)
    {
        Set<String> predicateColumnNames = new HashSet<>();
        domainPredicate.getDomains().get().keySet().stream()
                .map(Subfield::getRootName)
                .forEach(predicateColumnNames::add);
        // Include only columns referenced in the optimized expression. Although the expression is sent to the worker node
        // unoptimized, the worker is expected to optimize the expression before executing.
        extractVariableExpressions(optimizedRemainingExpression).stream()
                .map(VariableReferenceExpression::getName)
                .forEach(predicateColumnNames::add);

        return predicateColumnNames;
    }

    private static Set<VariableReferenceExpression> extractVariableExpressions(RowExpression expression)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new VariableReferenceBuilderVisitor(), builder);
        return builder.build();
    }

    private static class VariableReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(
                VariableReferenceExpression variable,
                ImmutableSet.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
            return null;
        }
    }
}
