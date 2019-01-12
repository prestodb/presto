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
package io.prestosql.plugin.cassandra;

import com.datastax.driver.core.VersionNumber;
import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.cassandra.util.CassandraCqlUtils;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.toCQLCompatibleString;
import static java.util.Objects.requireNonNull;

public class CassandraClusteringPredicatesExtractor
{
    private final List<CassandraColumnHandle> clusteringColumns;
    private final ClusteringPushDownResult clusteringPushDownResult;
    private final TupleDomain<ColumnHandle> predicates;

    public CassandraClusteringPredicatesExtractor(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates, VersionNumber cassandraVersion)
    {
        this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
        this.predicates = requireNonNull(predicates, "predicates is null");
        this.clusteringPushDownResult = getClusteringKeysSet(clusteringColumns, predicates, requireNonNull(cassandraVersion, "cassandraVersion is null"));
    }

    public String getClusteringKeyPredicates()
    {
        return clusteringPushDownResult.getDomainQuery();
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

    private static ClusteringPushDownResult getClusteringKeysSet(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates, VersionNumber cassandraVersion)
    {
        ImmutableMap.Builder<ColumnHandle, Domain> domainsBuilder = ImmutableMap.builder();
        ImmutableList.Builder<String> clusteringColumnSql = ImmutableList.builder();
        int currentClusteringColumn = 0;
        for (CassandraColumnHandle columnHandle : clusteringColumns) {
            Domain domain = predicates.getDomains().get().get(columnHandle);
            if (domain == null) {
                break;
            }
            if (domain.isNullAllowed()) {
                break;
            }
            String predicateString = null;
            predicateString = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        List<Object> singleValues = new ArrayList<>();
                        List<String> rangeConjuncts = new ArrayList<>();
                        String predicate = null;

                        for (Range range : ranges.getOrderedRanges()) {
                            if (range.isAll()) {
                                return null;
                            }
                            if (range.isSingleValue()) {
                                singleValues.add(CassandraCqlUtils.cqlValue(toCQLCompatibleString(range.getSingleValue()),
                                        columnHandle.getCassandraType()));
                            }
                            else {
                                if (!range.getLow().isLowerUnbounded()) {
                                    switch (range.getLow().getBound()) {
                                        case ABOVE:
                                            rangeConjuncts.add(CassandraCqlUtils.validColumnName(columnHandle.getName()) + " > "
                                                    + CassandraCqlUtils.cqlValue(toCQLCompatibleString(range.getLow().getValue()),
                                                    columnHandle.getCassandraType()));
                                            break;
                                        case EXACTLY:
                                            rangeConjuncts.add(CassandraCqlUtils.validColumnName(columnHandle.getName()) + " >= "
                                                    + CassandraCqlUtils.cqlValue(toCQLCompatibleString(range.getLow().getValue()),
                                                    columnHandle.getCassandraType()));
                                            break;
                                        case BELOW:
                                            throw new VerifyException("Low Marker should never use BELOW bound");
                                        default:
                                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                                    }
                                }
                                if (!range.getHigh().isUpperUnbounded()) {
                                    switch (range.getHigh().getBound()) {
                                        case ABOVE:
                                            throw new VerifyException("High Marker should never use ABOVE bound");
                                        case EXACTLY:
                                            rangeConjuncts.add(CassandraCqlUtils.validColumnName(columnHandle.getName()) + " <= "
                                                    + CassandraCqlUtils.cqlValue(toCQLCompatibleString(range.getHigh().getValue()),
                                                    columnHandle.getCassandraType()));
                                            break;
                                        case BELOW:
                                            rangeConjuncts.add(CassandraCqlUtils.validColumnName(columnHandle.getName()) + " < "
                                                    + CassandraCqlUtils.cqlValue(toCQLCompatibleString(range.getHigh().getValue()),
                                                    columnHandle.getCassandraType()));
                                            break;
                                        default:
                                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                                    }
                                }
                            }
                        }

                        if (!singleValues.isEmpty() && !rangeConjuncts.isEmpty()) {
                            return null;
                        }
                        if (!singleValues.isEmpty()) {
                            if (singleValues.size() == 1) {
                                predicate = CassandraCqlUtils.validColumnName(columnHandle.getName()) + " = " + singleValues.get(0);
                            }
                            else {
                                predicate = CassandraCqlUtils.validColumnName(columnHandle.getName()) + " IN ("
                                        + Joiner.on(",").join(singleValues) + ")";
                            }
                        }
                        else if (!rangeConjuncts.isEmpty()) {
                            predicate = Joiner.on(" AND ").join(rangeConjuncts);
                        }
                        return predicate;
                    }, discreteValues -> {
                        if (discreteValues.isWhiteList()) {
                            ImmutableList.Builder<Object> discreteValuesList = ImmutableList.builder();
                            for (Object discreteValue : discreteValues.getValues()) {
                                discreteValuesList.add(CassandraCqlUtils.cqlValue(toCQLCompatibleString(discreteValue),
                                        columnHandle.getCassandraType()));
                            }
                            String predicate = CassandraCqlUtils.validColumnName(columnHandle.getName()) + " IN ("
                                    + Joiner.on(",").join(discreteValuesList.build()) + ")";
                            return predicate;
                        }
                        return null;
                    }, allOrNone -> null);

            if (predicateString == null) {
                break;
            }
            // IN restriction only on last clustering column for Cassandra version = 2.1
            if (predicateString.contains(" IN (") && cassandraVersion.compareTo(VersionNumber.parse("2.2.0")) < 0 && currentClusteringColumn != (clusteringColumns.size() - 1)) {
                break;
            }
            clusteringColumnSql.add(predicateString);
            domainsBuilder.put(columnHandle, domain);
            // Check for last clustering column should only be restricted by range condition
            if (predicateString.contains(">") || predicateString.contains("<")) {
                break;
            }
            currentClusteringColumn++;
        }
        List<String> clusteringColumnPredicates = clusteringColumnSql.build();

        return new ClusteringPushDownResult(domainsBuilder.build(), Joiner.on(" AND ").join(clusteringColumnPredicates));
    }

    private static class ClusteringPushDownResult
    {
        private final Map<ColumnHandle, Domain> domains;
        private final String domainQuery;

        public ClusteringPushDownResult(Map<ColumnHandle, Domain> domains, String domainQuery)
        {
            this.domains = requireNonNull(ImmutableMap.copyOf(domains));
            this.domainQuery = requireNonNull(domainQuery);
        }

        public Map<ColumnHandle, Domain> getDomains()
        {
            return domains;
        }

        public String getDomainQuery()
        {
            return domainQuery;
        }
    }
}
