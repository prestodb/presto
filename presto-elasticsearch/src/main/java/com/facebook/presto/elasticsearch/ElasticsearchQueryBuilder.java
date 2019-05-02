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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.elasticsearch.ElasticsearchClient.createTransportClient;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;
import static com.facebook.presto.spi.predicate.Marker.Bound.ABOVE;
import static com.facebook.presto.spi.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

public class ElasticsearchQueryBuilder
{
    private static final Logger LOG = Logger.get(ElasticsearchQueryBuilder.class);

    private final Duration scrollTimeout;
    private final int scrollSize;
    private final TransportClient client;
    private final int shard;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final List<ElasticsearchColumnHandle> columns;
    private final String index;
    private final String type;

    public ElasticsearchQueryBuilder(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchConnectorConfig config, ElasticsearchSplit split)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(config, "config is null");
        requireNonNull(split, "split is null");

        columns = columnHandles;
        tupleDomain = split.getTupleDomain();
        index = split.getIndex();
        shard = split.getShard();
        type = split.getType();
        InetAddress address;
        try {
            address = InetAddress.getByName(split.getSearchNode());
        }
        catch (UnknownHostException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Error connecting to search node (%s:%d)", split.getSearchNode(), split.getPort()), e);
        }
        client = createTransportClient(config, new TransportAddress(address, split.getPort()));
        scrollTimeout = config.getScrollTimeout();
        scrollSize = config.getScrollSize();
    }

    public void close()
    {
        client.close();
    }

    public SearchRequestBuilder buildScrollSearchRequest()
    {
        String indices = index != null && !index.isEmpty() ? index : "_all";
        List<String> fields = columns.stream()
                .map(ElasticsearchColumnHandle::getColumnName)
                .collect(toList());
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indices)
                .setTypes(type)
                .setSearchType(QUERY_THEN_FETCH)
                .setScroll(new TimeValue(scrollTimeout.toMillis()))
                .setFetchSource(fields.toArray(new String[0]), null)
                .setQuery(buildSearchQuery())
                .setPreference("_shards:" + shard)
                .setSize(scrollSize);
        LOG.debug("Elasticsearch Request: %s", searchRequestBuilder);
        return searchRequestBuilder;
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId)
    {
        return client.prepareSearchScroll(scrollId)
                .setScroll(new TimeValue(scrollTimeout.toMillis()));
    }

    private QueryBuilder buildSearchQuery()
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        for (ElasticsearchColumnHandle column : columns) {
            BoolQueryBuilder columnQueryBuilder = new BoolQueryBuilder();
            Type type = column.getColumnType();
            if (tupleDomain.getDomains().isPresent()) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    columnQueryBuilder.should(buildPredicate(column.getColumnJsonPath(), domain, type));
                }
            }
            boolQueryBuilder.must(columnQueryBuilder);
        }
        if (boolQueryBuilder.hasClauses()) {
            return boolQueryBuilder;
        }
        return new MatchAllQueryBuilder();
    }

    private QueryBuilder buildPredicate(String columnName, Domain domain, Type type)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        if (domain.getValues().isNone()) {
            boolQueryBuilder.mustNot(new ExistsQueryBuilder(columnName));
            return boolQueryBuilder;
        }

        if (domain.getValues().isAll()) {
            boolQueryBuilder.must(new ExistsQueryBuilder(columnName));
            return boolQueryBuilder;
        }

        return buildTermQuery(boolQueryBuilder, columnName, domain, type);
    }

    private QueryBuilder buildTermQuery(BoolQueryBuilder queryBuilder, String columnName, Domain domain, Type type)
    {
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            BoolQueryBuilder rangeQueryBuilder = new BoolQueryBuilder();
            Set<Object> valuesToInclude = new HashSet<>();
            checkState(!range.isAll(), "Invalid range for column: " + columnName);
            if (range.isSingleValue()) {
                valuesToInclude.add(range.getLow().getValue());
            }
            else {
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeQueryBuilder.must(new RangeQueryBuilder(columnName).gt(getValue(type, range.getLow().getValue())));
                            break;
                        case EXACTLY:
                            rangeQueryBuilder.must(new RangeQueryBuilder(columnName).gte(getValue(type, range.getLow().getValue())));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case EXACTLY:
                            rangeQueryBuilder.must(new RangeQueryBuilder(columnName).lte(getValue(type, range.getHigh().getValue())));
                            break;
                        case BELOW:
                            rangeQueryBuilder.must(new RangeQueryBuilder(columnName).lt(getValue(type, range.getHigh().getValue())));
                            break;
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
            }

            if (valuesToInclude.size() == 1) {
                rangeQueryBuilder.must(new TermQueryBuilder(columnName, getValue(type, getOnlyElement(valuesToInclude))));
            }
            queryBuilder.should(rangeQueryBuilder);
        }
        return queryBuilder;
    }

    private static Object getValue(Type type, Object value)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(DOUBLE) || type.equals(BOOLEAN)) {
            return value;
        }
        if (type.equals(VARCHAR)) {
            return ((Slice) value).toStringUtf8();
        }
        throw new IllegalArgumentException("Unhandled type: " + type);
    }
}
