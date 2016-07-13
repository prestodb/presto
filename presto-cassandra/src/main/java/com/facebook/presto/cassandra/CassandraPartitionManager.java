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
package com.facebook.presto.cassandra;

import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.cassandra.util.CassandraCqlUtils.toCQLCompatibleString;
import static com.facebook.presto.cassandra.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_EXTERNAL;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CassandraPartitionManager
{
    private static final Logger log = Logger.get(CassandraPartitionManager.class);

    private final CachingCassandraSchemaProvider schemaProvider;
    private final ListeningExecutorService executor;

    @Inject
    public CassandraPartitionManager(CachingCassandraSchemaProvider schemaProvider, @ForCassandra ExecutorService executor)
    {
        this.schemaProvider = requireNonNull(schemaProvider, "schemaProvider is null");
        this.executor = listeningDecorator(requireNonNull(executor, "executor is null"));
    }

    public CassandraPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        CassandraTableHandle cassandraTableHandle = checkType(tableHandle, CassandraTableHandle.class, "tableHandle");

        CassandraTable table = schemaProvider.getTable(cassandraTableHandle);
        List<CassandraColumnHandle> partitionKeys = table.getPartitionKeyColumns();

        // fetch the partitions
        List<CassandraPartition> allPartitions = getCassandraPartitions(table, tupleDomain);
        log.debug("%s.%s #partitions: %d", cassandraTableHandle.getSchemaName(), cassandraTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
        List<CassandraPartition> partitions = allPartitions.stream()
                .filter(partition -> tupleDomain.overlaps(partition.getTupleDomain()))
                .collect(toList());

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            if (partitions.size() == 1 && partitions.get(0).isUnpartitioned()) {
                remainingTupleDomain = tupleDomain;
            }
            else {
                @SuppressWarnings({"rawtypes", "unchecked"})
                List<ColumnHandle> partitionColumns = (List) partitionKeys;
                remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains().get(), not(in(partitionColumns))));
            }
        }

        // push down indexed column fixed value predicates only for unpartitioned partition which uses token range query
        if ((partitions.size() == 1) && partitions.get(0).isUnpartitioned()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
            List<ColumnHandle> indexedColumns = new ArrayList<>();
            // compose partitionId by using indexed column
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                CassandraColumnHandle column = (CassandraColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                if (column.isIndexed() && domain.isSingleValue()) {
                    sb.append(CassandraCqlUtils.validColumnName(column.getName()))
                            .append(" = ")
                            .append(CassandraCqlUtils.cqlValue(toCQLCompatibleString(entry.getValue().getSingleValue()), column.getCassandraType()));
                    indexedColumns.add(column);
                    // Only one indexed column predicate can be pushed down.
                    break;
                }
            }
            if (sb.length() > 0) {
                CassandraPartition partition = partitions.get(0);
                TupleDomain<ColumnHandle> filterIndexedColumn = TupleDomain.withColumnDomains(Maps.filterKeys(remainingTupleDomain.getDomains().get(), not(in(indexedColumns))));
                partitions = new ArrayList<>();
                partitions.add(new CassandraPartition(partition.getKey(), sb.toString(), filterIndexedColumn, true));
                return new CassandraPartitionResult(partitions, filterIndexedColumn);
            }
        }
        return new CassandraPartitionResult(partitions, remainingTupleDomain);
    }

    private List<CassandraPartition> getCassandraPartitions(CassandraTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of();
        }

        Set<List<Object>> partitionKeysSet = getPartitionKeysSet(table, tupleDomain);

        // empty filter means, all partitions
        if (partitionKeysSet.isEmpty()) {
            return schemaProvider.getAllPartitions(table);
        }

        ImmutableList.Builder<ListenableFuture<List<CassandraPartition>>> getPartitionResults = ImmutableList.builder();
        for (List<Object> partitionKeys : partitionKeysSet) {
            getPartitionResults.add(executor.submit(() -> schemaProvider.getPartitions(table, partitionKeys)));
        }

        ImmutableList.Builder<CassandraPartition> partitions = ImmutableList.builder();
        for (ListenableFuture<List<CassandraPartition>> result : getPartitionResults.build()) {
            try {
                partitions.addAll(result.get());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
                throw new PrestoException(GENERIC_EXTERNAL, "Error fetching cassandra partitions", e);
            }
        }

        return partitions.build();
    }

    private static Set<List<Object>> getPartitionKeysSet(CassandraTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        ImmutableList.Builder<Set<Object>> partitionColumnValues = ImmutableList.builder();
        for (CassandraColumnHandle columnHandle : table.getPartitionKeyColumns()) {
            Domain domain = tupleDomain.getDomains().get().get(columnHandle);

            // if there is no constraint on a partition key, return an empty set
            if (domain == null) {
                return ImmutableSet.of();
            }

            // todo does cassandra allow null partition keys?
            if (domain.isNullAllowed()) {
                return ImmutableSet.of();
            }

            Set<Object> values = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        ImmutableSet.Builder<Object> columnValues = ImmutableSet.builder();
                        for (Range range : ranges.getOrderedRanges()) {
                            // if the range is not a single value, we can not perform partition pruning
                            if (!range.isSingleValue()) {
                                return ImmutableSet.of();
                            }
                            Object value = range.getSingleValue();

                            CassandraType valueType = columnHandle.getCassandraType();
                            columnValues.add(valueType.validatePartitionKey(value));
                        }
                        return columnValues.build();
                    },
                    discreteValues -> {
                        if (discreteValues.isWhiteList()) {
                            return ImmutableSet.copyOf(discreteValues.getValues());
                        }
                        return ImmutableSet.of();
                    },
                    allOrNone -> ImmutableSet.of());
            partitionColumnValues.add(values);
        }
        return Sets.cartesianProduct(partitionColumnValues.build());
    }
}
