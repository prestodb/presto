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
package com.facebook.presto.hive;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.binaryExpression;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.rule.FilterPushdownUtils.isEntireColumn;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public final class MetadataUtils
{
    private MetadataUtils() {}

    public static Optional<DiscretePredicates> getDiscretePredicates(List<ColumnHandle> partitionColumns, List<HivePartition> partitions)
    {
        Optional<DiscretePredicates> discretePredicates = Optional.empty();
        if (!partitionColumns.isEmpty()) {
            // Do not create tuple domains for every partition at the same time!
            // There can be a huge number of partitions so use an iterable so
            // all domains do not need to be in memory at the same time.
            Iterable<TupleDomain<ColumnHandle>> partitionDomains = Iterables.transform(partitions, (hivePartition) -> TupleDomain.fromFixedValues(hivePartition.getKeys()));
            discretePredicates = Optional.of(new DiscretePredicates(partitionColumns, partitionDomains));
        }
        return discretePredicates;
    }

    public static TupleDomain<ColumnHandle> getPredicate(
            BaseHiveTableLayoutHandle layoutHandle,
            List<ColumnHandle> partitionColumns,
            List<HivePartition> partitions,
            Map<String, ColumnHandle> predicateColumns)
    {
        TupleDomain<ColumnHandle> predicate;
        predicate = layoutHandle.getDomainPredicate()
                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                .transform(predicateColumns::get)
                .intersect(createPredicate(partitionColumns, partitions));
        return predicate;
    }

    public static RowExpression getSubfieldPredicate(
            ConnectorSession session,
            BaseHiveTableLayoutHandle layoutHandle,
            Map<String, Type> columnTypes,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService)
    {
        SubfieldExtractor subfieldExtractor = new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session);

        return rowExpressionService.getDomainTranslator().toPredicate(
                layoutHandle.getDomainPredicate()
                        .transform(subfield -> !isEntireColumn(subfield) ? subfield : null)
                        .transform(subfield -> subfieldExtractor.toRowExpression(subfield, columnTypes.get(subfield.getRootName()))));
    }

    public static RowExpression getCombinedRemainingPredicate(BaseHiveTableLayoutHandle layoutHandle, RowExpression subfieldPredicate)
    {
        List<RowExpression> predicatesToCombine = ImmutableList.of(subfieldPredicate, layoutHandle.getRemainingPredicate()).stream()
                .filter(p -> !p.equals(TRUE_CONSTANT))
                .collect(toImmutableList());

        return binaryExpression(SpecialFormExpression.Form.AND, predicatesToCombine);
    }

    @VisibleForTesting
    public static TupleDomain<ColumnHandle> createPredicate(List<ColumnHandle> partitionColumns, List<HivePartition> partitions)
    {
        if (partitions.isEmpty()) {
            return TupleDomain.none();
        }

        return withColumnDomains(
                partitionColumns.stream()
                        .collect(toMap(identity(), column -> buildColumnDomain(column, partitions))));
    }

    private static Domain buildColumnDomain(ColumnHandle column, List<HivePartition> partitions)
    {
        checkArgument(!partitions.isEmpty(), "partitions cannot be empty");

        boolean hasNull = false;
        Set<Object> nonNullValues = new HashSet<>();
        Type type = null;

        for (HivePartition partition : partitions) {
            NullableValue value = partition.getKeys().get(column);
            if (value == null) {
                throw new PrestoException(HIVE_UNKNOWN_ERROR,
                        format("Partition %s does not have a value for partition column %s", partition, column));
            }

            if (value.isNull()) {
                hasNull = true;
            }
            else {
                nonNullValues.add(value.getValue());
            }

            if (type == null) {
                type = value.getType();
            }
        }

        if (!nonNullValues.isEmpty()) {
            Domain domain = Domain.multipleValues(type, ImmutableList.copyOf(nonNullValues));
            if (hasNull) {
                return domain.union(Domain.onlyNull(type));
            }

            return domain;
        }

        return Domain.onlyNull(type);
    }
}
