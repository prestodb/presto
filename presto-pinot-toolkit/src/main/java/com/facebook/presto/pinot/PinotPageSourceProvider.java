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
package com.facebook.presto.pinot;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.Ranges;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.pinot.auth.PinotBrokerAuthenticationProvider;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.connector.presto.grpc.PinotStreamingQueryClient;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.DYNAMIC_FILTER_FUNCTION_TEMPLATE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.common.config.GrpcConfig.CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE;
import static org.apache.pinot.common.config.GrpcConfig.CONFIG_USE_PLAIN_TEXT;

public class PinotPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final String connectorId;
    private final PinotConfig pinotConfig;
    private final PinotStreamingQueryClient pinotStreamingQueryClient;
    private final PinotClusterInfoFetcher clusterInfoFetcher;
    private final ObjectMapper objectMapper;
    private final PinotBrokerAuthenticationProvider brokerAuthenticationProvider;

    @Inject
    public PinotPageSourceProvider(
            ConnectorId connectorId,
            PinotConfig pinotConfig,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper,
            PinotBrokerAuthenticationProvider brokerAuthenticationProvider)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
        this.pinotStreamingQueryClient = new PinotStreamingQueryClient(extractGrpcQueryClientConfig(pinotConfig));
        this.clusterInfoFetcher = requireNonNull(clusterInfoFetcher, "cluster info fetcher is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");
        this.brokerAuthenticationProvider = requireNonNull(brokerAuthenticationProvider, "broker authentication provider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle tableLayoutHandle,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        requireNonNull(split, "split is null");

        PinotSplit pinotSplit = (PinotSplit) split;
        checkArgument(pinotSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        PinotQueryGenerator.GeneratedPinotQuery brokerQuery = pinotSplit.getBrokerPinotQuery().get();

        if (splitContext.getDynamicFilterPredicate().isPresent()) {
            brokerQuery = processDynamicFilterSqlQuery(splitContext, brokerQuery, PinotSessionProperties.getMaxPushdownDynamicFilterSize(session));
        }

        // either no dynamic filter or dynamic filter is too large
        if (brokerQuery.getQuery().contains(DYNAMIC_FILTER_FUNCTION_TEMPLATE)) {
            String replacedPlaceholderQuery;
            if (brokerQuery.getQuery().contains(format("%s AND", DYNAMIC_FILTER_FUNCTION_TEMPLATE))) {
                replacedPlaceholderQuery = brokerQuery.getQuery().replace(format("%s AND", DYNAMIC_FILTER_FUNCTION_TEMPLATE), "");
            }
            else {
                replacedPlaceholderQuery = brokerQuery.getQuery().replace(format("WHERE %s", DYNAMIC_FILTER_FUNCTION_TEMPLATE), "");
            }
            brokerQuery = new PinotQueryGenerator.GeneratedPinotQuery(
                    brokerQuery.getTable(),
                    replacedPlaceholderQuery,
                    brokerQuery.getExpectedColumnIndices(),
                    brokerQuery.isHaveFilter(),
                    brokerQuery.forBroker());
        }

        List<PinotColumnHandle> handles = new ArrayList<>();
        for (ColumnHandle handle : columns) {
            handles.add((PinotColumnHandle) handle);
        }

        switch (pinotSplit.getSplitType()) {
            case SEGMENT:
                return new PinotSegmentPageSource(
                    session,
                    pinotConfig,
                    pinotStreamingQueryClient,
                    pinotSplit,
                    handles);
            case BROKER:
                return new PinotBrokerPageSource(
                    pinotConfig,
                    session,
                    brokerQuery,
                    handles,
                    pinotSplit.getExpectedColumnHandles(),
                    clusterInfoFetcher,
                    objectMapper,
                    brokerAuthenticationProvider);
            default:
                throw new UnsupportedOperationException("Unknown Pinot split type: " + pinotSplit.getSplitType());
        }
    }

    private static final ImmutableList<String> invalidDomainType(String domainType)
    {
        throw new IllegalStateException(domainType);
    }

    private static PinotQueryGenerator.GeneratedPinotQuery processDynamicFilterSqlQuery(
            SplitContext splitContext,
            PinotQueryGenerator.GeneratedPinotQuery brokerQuery,
            int maxPushdownDynamicFilterSize)
    {
        checkArgument(brokerQuery.getQuery().contains(DYNAMIC_FILTER_FUNCTION_TEMPLATE), "Dynamic filter is not present in the query");

        TupleDomain<ColumnHandle> dynamicFilter = splitContext.getDynamicFilterPredicate().get();
        Map<ColumnHandle, Domain> filterDomains = dynamicFilter.getDomains().get();

        String dynamicFilterString = getDynamicFilterSting(filterDomains, maxPushdownDynamicFilterSize);
        if (dynamicFilterString.isEmpty()) {
            return brokerQuery;
        }

        return new PinotQueryGenerator.GeneratedPinotQuery(
                brokerQuery.getTable(),
                brokerQuery.getQuery().replace(DYNAMIC_FILTER_FUNCTION_TEMPLATE, dynamicFilterString),
                brokerQuery.getExpectedColumnIndices(),
                brokerQuery.isHaveFilter(),
                brokerQuery.forBroker());
    }

    /**
     * Generates the dynamic filter string for the SQL query based on the domain conditions.
     *
     * @param filterDomains columns and their domains to be filtered
     * @param maxPushdownDynamicFilterSize maximum size of identifiers the dynamic filter will filter before using span instead
     * @return the dynamic filter string to be used in the query
     */
    private static String getDynamicFilterSting(Map<ColumnHandle, Domain> filterDomains, int maxPushdownDynamicFilterSize)
    {
        return filterDomains.entrySet().stream()
                .map(filterDomain -> {
                    String columnName = ((PinotColumnHandle) filterDomain.getKey()).getColumnName();
                    Domain domain = filterDomain.getValue();

                    checkArgument(domain.getValues() instanceof SortedRangeSet, "dynamic filtering pushdown currently only supports ranges");

                    Ranges ranges = domain.getValues().getRanges();
                    Range span = ranges.getSpan();

                    if (span.isSingleValue()) {
                        return ImmutableList.of(format("%s = %s", columnName, getSliceValue(span.getType(), span.getSingleValue())));
                    }
                    if (ranges.getRangeCount() > maxPushdownDynamicFilterSize) {
                        return ImmutableList.of(
                                format("%s <= %s", columnName, span.getHighBoundedValue()),
                                format("%s <= %s", columnName, span.getHighBoundedValue()));
                    }
                    String singleValues = ranges.getOrderedRanges().stream()
                            .filter(Range::isSingleValue)
                            .map(range -> getSliceValue(range.getType(), range.getSingleValue()))
                            .collect(Collectors.joining(","));
                    String rangeValues = ranges.getOrderedRanges().stream()
                            .filter(range -> !range.isSingleValue())
                            .map(range -> format("(%s <= %s AND %s >= %s)",
                                    columnName,
                                    getSliceValue(range.getType(), range.getHighBoundedValue()),
                                    columnName,
                                    getSliceValue(range.getType(), range.getLowBoundedValue()))
                            ).collect(Collectors.joining(" OR "));

                    String singleValueCondition = format("%s IN (%s)", columnName, singleValues);
                    String rangeValueCondition = format("(%s)", rangeValues);

                    if (singleValues.isEmpty()) {
                        return ImmutableList.of(rangeValueCondition);
                    }
                    if (rangeValues.isEmpty()) {
                        return ImmutableList.of(singleValueCondition);
                    }
                    return ImmutableList.of(format("(%s OR %s)", singleValueCondition, rangeValueCondition));
                })
                .flatMap(List::stream)
                .collect(Collectors.joining(" AND "));
    }

    private static String getSliceValue(Type type, Object value)
    {
        if (type instanceof JsonType) {
            throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "type '" + type + "' not supported");
        }
        if (type instanceof VarcharType) {
            return format("'%s'", ((Slice) value).toStringUtf8());
        }
        return value.toString();
    }

    @VisibleForTesting
    static GrpcConfig extractGrpcQueryClientConfig(PinotConfig config)
    {
        Map<String, Object> target = new HashMap<>();
        target.put(CONFIG_USE_PLAIN_TEXT, !config.isUseSecureConnection());
        target.put(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, config.getStreamingServerGrpcMaxInboundMessageBytes());
        if (config.isUseSecureConnection()) {
            setOrRemoveProperty(target, "tls.keystore.path", config.getGrpcTlsKeyStorePath());
            setOrRemoveProperty(target, "tls.keystore.password", config.getGrpcTlsKeyStorePassword());
            setOrRemoveProperty(target, "tls.keystore.type", config.getGrpcTlsKeyStoreType());
            setOrRemoveProperty(target, "tls.truststore.path", config.getGrpcTlsTrustStorePath());
            setOrRemoveProperty(target, "tls.truststore.password", config.getGrpcTlsTrustStorePassword());
            setOrRemoveProperty(target, "tls.truststore.type", config.getGrpcTlsTrustStoreType());
        }
        return new GrpcConfig(target);
    }

    // The method is created because Pinot Config does not like null as value, if value is null, we should
    // remove the key instead.
    private static void setOrRemoveProperty(Map<String, Object> prop, String key, Object value)
    {
        if (value == null) {
            prop.remove(key);
        }
        else {
            prop.put(key, value);
        }
    }
}
