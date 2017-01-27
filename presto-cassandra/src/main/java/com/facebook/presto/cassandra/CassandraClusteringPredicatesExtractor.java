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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.cassandra.util.CassandraCqlUtils.toCQLCompatibleString;
import static com.google.common.collect.Sets.cartesianProduct;
import static java.util.Objects.requireNonNull;

public class CassandraClusteringPredicatesExtractor
{
    private final List<CassandraColumnHandle> clusteringColumns;
    private final TupleDomain<ColumnHandle> predicates;
    private final ClusteringPushDownResult clusteringPushDownResult;

    public CassandraClusteringPredicatesExtractor(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates)
    {
        this.clusteringColumns = ImmutableList.copyOf(requireNonNull(clusteringColumns, "clusteringColumns is null"));
        this.predicates = requireNonNull(predicates, "predicates is null");
        this.clusteringPushDownResult = getClusteringKeysSet(clusteringColumns, predicates);
    }

    public List<String> getClusteringKeyPredicates()
    {
        Set<List<Object>> pushedDownDomainValues = clusteringPushDownResult.getDomainValues();

        if (pushedDownDomainValues.isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<String> clusteringPredicates = ImmutableList.builder();
        for (List<Object> clusteringKeys : pushedDownDomainValues) {
            if (clusteringKeys.isEmpty()) {
                continue;
            }

            StringBuilder stringBuilder = new StringBuilder();

            for (int i = 0; i < clusteringKeys.size(); i++) {
                if (i > 0) {
                    stringBuilder.append(" AND ");
                }

                stringBuilder.append(CassandraCqlUtils.validColumnName(clusteringColumns.get(i).getName()));
                stringBuilder.append(" = ");
                stringBuilder.append(CassandraCqlUtils.cqlValue(toCQLCompatibleString(clusteringKeys.get(i)), clusteringColumns.get(i).getCassandraType()));
            }

            clusteringPredicates.add(stringBuilder.toString());
        }
        return clusteringPredicates.build();
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraints()
    {
        Map<ColumnHandle, Domain> pushedDown = clusteringPushDownResult.getDomains();
        Map<ColumnHandle, Domain> notPushedDown = new HashMap<>(predicates.getDomains().get());

        if (!notPushedDown.isEmpty() && !pushedDown.isEmpty()) {
            notPushedDown.entrySet().removeAll(pushedDown.entrySet());
        }

        return TupleDomain.withColumnDomains(notPushedDown);
    }

    private static ClusteringPushDownResult getClusteringKeysSet(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates)
    {
        ImmutableMap.Builder<ColumnHandle, Domain> domainsBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Set<Object>> clusteringColumnValues = ImmutableList.builder();
        for (CassandraColumnHandle columnHandle : clusteringColumns) {
            Domain domain = predicates.getDomains().get().get(columnHandle);

            if (domain == null) {
                break;
            }

            if (domain.isNullAllowed()) {
                return new ClusteringPushDownResult(domainsBuilder.build(), ImmutableSet.of());
            }

            Set<Object> values = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        ImmutableSet.Builder<Object> columnValues = ImmutableSet.builder();
                        for (Range range : ranges.getOrderedRanges()) {
                            if (!range.isSingleValue()) {
                                return ImmutableSet.of();
                            }
                            /* TODO add code to handle a range of values for the last column
                             * Prior to Cassandra 2.2, only the last clustering column can have a range of values
                             * Take a look at how this is done in PreparedStatementBuilder.java
                             */

                            Object value = range.getSingleValue();

                            CassandraType valueType = columnHandle.getCassandraType();
                            columnValues.add(valueType.validateClusteringKey(value));
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

            if (!values.isEmpty()) {
                clusteringColumnValues.add(values);
                domainsBuilder.put(columnHandle, domain);
            }
        }
        return new ClusteringPushDownResult(domainsBuilder.build(), cartesianProduct(clusteringColumnValues.build()));
    }

    private static class ClusteringPushDownResult
    {
        private final Map<ColumnHandle, Domain> domains;
        private final Set<List<Object>> domainValues;

        public ClusteringPushDownResult(Map<ColumnHandle, Domain> domains, Set<List<Object>> domainValues)
        {
            this.domains = requireNonNull(ImmutableMap.copyOf(domains));
            this.domainValues = requireNonNull(ImmutableSet.copyOf(domainValues));
        }

        public Map<ColumnHandle, Domain> getDomains()
        {
            return domains;
        }

        public Set<List<Object>> getDomainValues()
        {
            return domainValues;
        }
    }
}
