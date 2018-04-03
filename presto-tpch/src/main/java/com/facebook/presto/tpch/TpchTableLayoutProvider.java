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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTablePartitioning;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.connector.ConnectorTableLayoutProvider;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.OrderColumn;
import io.airlift.tpch.OrderGenerator;
import io.airlift.tpch.PartColumn;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.tpch.TpchMetadata.getPrestoType;
import static com.facebook.presto.tpch.util.PredicateUtils.convertToPredicate;
import static com.facebook.presto.tpch.util.PredicateUtils.filterOutColumnFromPredicate;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class TpchTableLayoutProvider
        implements ConnectorTableLayoutProvider
{
    private static final Set<Slice> ORDER_STATUS_VALUES = ImmutableSet.of("F", "O", "P").stream()
            .map(Slices::utf8Slice)
            .collect(toImmutableSet());
    private static final Set<NullableValue> ORDER_STATUS_NULLABLE_VALUES = ORDER_STATUS_VALUES.stream()
            .map(value -> new NullableValue(getPrestoType(OrderColumn.ORDER_STATUS), value))
            .collect(toSet());

    private static final Set<Slice> PART_TYPE_VALUES = Distributions.getDefaultDistributions().getPartTypes().getValues().stream()
            .map(Slices::utf8Slice)
            .collect(toImmutableSet());
    private static final Set<NullableValue> PART_TYPE_NULLABLE_VALUES = PART_TYPE_VALUES.stream()
            .map(value -> new NullableValue(getPrestoType(PartColumn.TYPE), value))
            .collect(toSet());

    private static final Set<Slice> PART_CONTAINER_VALUES = Distributions.getDefaultDistributions().getPartContainers().getValues().stream()
            .map(Slices::utf8Slice)
            .collect(toImmutableSet());
    private static final Set<NullableValue> PART_CONTAINER_NULLABLE_VALUES = PART_CONTAINER_VALUES.stream()
            .map(value -> new NullableValue(getPrestoType(PartColumn.CONTAINER), value))
            .collect(toSet());
    private final TpchMetadata tpchMetadata;
    private final TpchTableHandle table;
    private final ColumnNaming columnNaming;

    private TupleDomain<ColumnHandle> predicate = TupleDomain.all();
    private TupleDomain<ColumnHandle> unenforcedConstraint = TupleDomain.all();
    private OptionalLong limit = OptionalLong.empty();

    public TpchTableLayoutProvider(TpchMetadata tpchMetadata, TpchTableHandle table, ColumnNaming columnNaming, Optional<ConnectorTableLayoutHandle> layoutHandle)
    {
        this.tpchMetadata = requireNonNull(tpchMetadata, "tpchMetadata is null");
        this.table = requireNonNull(table, "table is null");
        this.columnNaming = requireNonNull(columnNaming, "columnNaming is null");

        if (layoutHandle.isPresent()) {
            TpchTableLayoutHandle tpchLayoutHandle = (TpchTableLayoutHandle) layoutHandle.get();
            predicate = tpchLayoutHandle.getPredicate();
            if (tpchLayoutHandle.getLimit() != Long.MAX_VALUE) {
                limit = OptionalLong.of(tpchLayoutHandle.getLimit());
            }
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> provide(ConnectorSession session)
    {
        Optional<ConnectorTablePartitioning> nodePartition = Optional.empty();
        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty();
        List<LocalProperty<ColumnHandle>> localProperties = ImmutableList.of();

        Map<String, ColumnHandle> columns = tpchMetadata.getColumnHandles(session, table);
        if (table.getTableName().equals(TpchTable.ORDERS.getTableName())) {
            ColumnHandle orderKeyColumn = columns.get(columnNaming.getName(OrderColumn.ORDER_KEY));
            nodePartition = Optional.of(new ConnectorTablePartitioning(
                    new TpchPartitioningHandle(
                            TpchTable.ORDERS.getTableName(),
                            calculateTotalRows(OrderGenerator.SCALE_BASE, table.getScaleFactor())),
                    ImmutableList.of(orderKeyColumn)));
            partitioningColumns = Optional.of(ImmutableSet.of(orderKeyColumn));
            localProperties = ImmutableList.of(new SortingProperty<>(orderKeyColumn, SortOrder.ASC_NULLS_FIRST));
        }
        else if (table.getTableName().equals(TpchTable.LINE_ITEM.getTableName())) {
            ColumnHandle orderKeyColumn = columns.get(columnNaming.getName(LineItemColumn.ORDER_KEY));
            nodePartition = Optional.of(new ConnectorTablePartitioning(
                    new TpchPartitioningHandle(
                            TpchTable.ORDERS.getTableName(),
                            calculateTotalRows(OrderGenerator.SCALE_BASE, table.getScaleFactor())),
                    ImmutableList.of(orderKeyColumn)));
            partitioningColumns = Optional.of(ImmutableSet.of(orderKeyColumn));
            localProperties = ImmutableList.of(
                    new SortingProperty<>(orderKeyColumn, SortOrder.ASC_NULLS_FIRST),
                    new SortingProperty<>(columns.get(columnNaming.getName(LineItemColumn.LINE_NUMBER)), SortOrder.ASC_NULLS_FIRST));
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(
                new TpchTableLayoutHandle(table, predicate, limit.orElse(Long.MAX_VALUE)),
                Optional.empty(),
                predicate, // TODO: return well-known properties (e.g., orderkey > 0, etc)
                nodePartition,
                partitioningColumns,
                Optional.empty(),
                localProperties);

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, unenforcedConstraint));
    }

    @Override
    public Optional<PredicatePushdown> getPredicatePushdown()
    {
        return Optional.of(new PredicatePushdown() {
            @Override
            public TupleDomain<ColumnHandle> getUnenforcedConstraint()
            {
                return unenforcedConstraint;
            }

            @Override
            public TupleDomain<ColumnHandle> getPredicate()
            {
                return predicate;
            }

            @Override
            public void pushDownPredicate(Constraint<ColumnHandle> constraint)
            {
                unenforcedConstraint = constraint.getSummary();
                if (table.getTableName().equals(TpchTable.ORDERS.getTableName())) {
                    predicate = toTupleDomain(ImmutableMap.of(
                            tpchMetadata.toColumnHandle(OrderColumn.ORDER_STATUS),
                            filterValues(ORDER_STATUS_NULLABLE_VALUES, OrderColumn.ORDER_STATUS, constraint)));
                    unenforcedConstraint = filterOutColumnFromPredicate(constraint.getSummary(), tpchMetadata.toColumnHandle(OrderColumn.ORDER_STATUS));
                }
                else if (table.getTableName().equals(TpchTable.PART.getTableName())) {
                    predicate = toTupleDomain(ImmutableMap.of(
                            tpchMetadata.toColumnHandle(PartColumn.CONTAINER),
                            filterValues(PART_CONTAINER_NULLABLE_VALUES, PartColumn.CONTAINER, constraint),
                            tpchMetadata.toColumnHandle(PartColumn.TYPE),
                            filterValues(PART_TYPE_NULLABLE_VALUES, PartColumn.TYPE, constraint)));
                    unenforcedConstraint = filterOutColumnFromPredicate(constraint.getSummary(), tpchMetadata.toColumnHandle(PartColumn.CONTAINER));
                    unenforcedConstraint = filterOutColumnFromPredicate(unenforcedConstraint, tpchMetadata.toColumnHandle(PartColumn.TYPE));
                }
            }
        });
    }

    private Set<NullableValue> filterValues(Set<NullableValue> nullableValues, TpchColumn<?> column, Constraint<ColumnHandle> constraint)
    {
        return nullableValues.stream()
                .filter(convertToPredicate(constraint.getSummary(), tpchMetadata.toColumnHandle(column)))
                .filter(value -> constraint.predicate().test(ImmutableMap.of(tpchMetadata.toColumnHandle(column), value)))
                .collect(toSet());
    }

    private TupleDomain<ColumnHandle> toTupleDomain(Map<TpchColumnHandle, Set<NullableValue>> predicate)
    {
        return TupleDomain.withColumnDomains(predicate.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    Type type = entry.getKey().getType();
                    return entry.getValue().stream()
                            .map(nullableValue -> Domain.singleValue(type, nullableValue.getValue()))
                            .reduce((Domain::union))
                            .orElse(Domain.none(type));
                })));
    }

    private long calculateTotalRows(int scaleBase, double scaleFactor)
    {
        double totalRows = scaleBase * scaleFactor;
        if (totalRows > Long.MAX_VALUE) {
            throw new IllegalArgumentException("Total rows is larger than 2^64");
        }
        return (long) totalRows;
    }

    @Override
    public Optional<LimitPushdown> getLimitPushdown()
    {
        return Optional.of(limit -> TpchTableLayoutProvider.this.limit = OptionalLong.of(limit));
    }
}
