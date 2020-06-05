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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_MAX_HITS_EXCEEDED;
import static com.facebook.presto.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static com.facebook.presto.elasticsearch.ElasticsearchUtils.serializeObject;
import static com.facebook.presto.elasticsearch.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.search.sort.SortOrder.ASC;

// TODO: clear scroll on close
public class ElasticsearchRecordCursor
        implements RecordCursor
{
    private static final JsonCodec<Object> VALUE_CODEC = jsonCodec(Object.class);

    private final TransportClient client;
    private final int scrollSize;
    private final Duration scrollTimeout;
    private final int maxAttempts;
    private final Duration maxRetryTime;
    private final Duration requestTimeout;

    private final List<ElasticsearchColumnHandle> columns;
    private final Map<String, Integer> jsonPathToIndex = new HashMap<>();
    private final int maxHits;
    private final TupleDomain<ColumnHandle> constraint;
    private final List<String> fieldsNames;
    private final String type;
    private final int shard;
    private final String indices;

    private long totalBytes;
    private List<Object> values;
    private SearchHits searchHits;
    private String scrollId;
    private int currentPosition;

    public ElasticsearchRecordCursor(TransportClient client, List<ElasticsearchColumnHandle> columns, ElasticsearchConfig config, ElasticsearchSplit split)
    {
        requireNonNull(client, "client is null");
        requireNonNull(columns, "columnHandle is null");
        requireNonNull(config, "config is null");

        this.client = client;
        this.columns = columns;
        this.type = split.getType();
        this.shard = split.getShard();
        this.constraint = split.getTupleDomain();
        this.maxHits = config.getMaxHits();

        this.scrollSize = config.getScrollSize();
        this.scrollTimeout = config.getScrollTimeout();
        this.maxAttempts = config.getMaxRequestRetries();
        this.maxRetryTime = config.getMaxRetryTime();
        this.requestTimeout = config.getRequestTimeout();

        for (int i = 0; i < columns.size(); i++) {
            jsonPathToIndex.put(columns.get(i).getColumnJsonPath(), i);
        }

        String index = split.getIndex();
        this.indices = index != null && !index.isEmpty() ? index : "_all";

        this.fieldsNames = columns.stream()
                .map(ElasticsearchColumnHandle::getColumnName)
                .collect(toList());
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columns.size(), "Invalid field index");
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (scrollId == null) {
            SearchResponse response = begin();
            scrollId = response.getScrollId();
            searchHits = response.getHits();
            currentPosition = 0;
        }
        else if (currentPosition == searchHits.getHits().length) {
            SearchResponse response = nextPage();
            scrollId = response.getScrollId();
            searchHits = response.getHits();
            currentPosition = 0;
        }

        if (currentPosition == searchHits.getHits().length) {
            return false;
        }

        if (searchHits.getTotalHits() > maxHits) {
            throw new PrestoException(ELASTICSEARCH_MAX_HITS_EXCEEDED,
                    "The number of hits for the query (" + searchHits.getTotalHits() + ") exceeds the configured max hits (" + maxHits + ")");
        }

        SearchHit hit = searchHits.getAt(currentPosition);
        currentPosition++;

        values = new ArrayList<>(Collections.nCopies(columns.size(), null));

        setFieldIfExists("_id", hit.getId());
        setFieldIfExists("_index", hit.getIndex());

        extractFromSource(hit);
        if (hit.getSourceRef() != null) {
            totalBytes += hit.getSourceRef().length();
        }
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, ImmutableSet.of(BOOLEAN));
        return Boolean.parseBoolean(getFieldValue(field).toString());
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, ImmutableSet.of(BIGINT, INTEGER));
        return Long.parseLong(getFieldValue(field).toString());
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, ImmutableSet.of(DOUBLE));
        return Double.parseDouble(getFieldValue(field).toString());
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, ImmutableSet.of(VARCHAR));

        Object value = getFieldValue(field);
        if (value instanceof Collection) {
            return utf8Slice(VALUE_CODEC.toJson(value));
        }
        if (value == null) {
            return EMPTY_SLICE;
        }
        return utf8Slice(value.toString());
    }

    @Override
    public Object getObject(int field)
    {
        return serializeObject(columns.get(field).getColumnType(), null, getFieldValue(field));
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columns.size(), "Invalid field index");
        return getFieldValue(field) == null;
    }

    private void checkFieldType(int field, Set<Type> expectedTypes)
    {
        checkArgument(expectedTypes.contains(getType(field)), "Field %s has unexpected type %s", field, getType(field));
    }

    public void close()
    {
    }

    private SearchResponse begin()
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("searchRequest", () -> client.prepareSearch(indices)
                            .setTypes(type)
                            .setSearchType(QUERY_THEN_FETCH)
                            .setFetchSource(fieldsNames.toArray(new String[0]), null)
                            .setQuery(buildSearchQuery(constraint, columns))
                            .setPreference("_shards:" + shard)
                            .addSort("_doc", ASC)
                            .setSize(scrollSize)
                            .setScroll(new TimeValue(scrollTimeout.toMillis()))
                            .execute()
                            .actionGet(requestTimeout.toMillis()));
        }
        catch (Exception e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    private SearchResponse nextPage()
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("scrollRequest", () -> client.prepareSearchScroll(scrollId)
                            .setScroll(new TimeValue(scrollTimeout.toMillis()))
                            .execute()
                            .actionGet(requestTimeout.toMillis()));
        }
        catch (Exception e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    private void setFieldIfExists(String jsonPath, Object jsonValue)
    {
        if (jsonPathToIndex.containsKey(jsonPath)) {
            values.set(jsonPathToIndex.get(jsonPath), jsonValue);
        }
    }

    private Object getFieldValue(int field)
    {
        checkState(values != null, "Cursor has not been advanced yet");
        return values.get(field);
    }

    private void extractFromSource(SearchHit hit)
    {
        List<Field> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : hit.getSourceAsMap().entrySet()) {
            fields.add(new Field(entry.getKey(), entry.getValue()));
        }
        Collections.sort(fields, Comparator.comparing(Field::getName));

        for (Map.Entry<String, Object> entry : unflatten(fields).entrySet()) {
            setFieldIfExists(entry.getKey(), entry.getValue());
        }
    }

    private static Map<String, Object> unflatten(List<Field> fields)
    {
        return unflatten(fields, 0, 0, fields.size());
    }

    private static Map<String, Object> unflatten(List<Field> fields, int level, int start, int length)
    {
        checkArgument(length > 0, "length must be > 0");

        int limit = start + length;

        Map<String, Object> result = new HashMap<>();
        int anchor = start;
        int current = start;

        do {
            Field field = fields.get(anchor);
            String name = field.getPathElement(level);

            current++;
            if (current == limit || !name.equals(fields.get(current).getPathElement(level))) {
                // We assume that fields can't be both leaves and intermediate nodes
                Object value;
                if (level < field.getDepth() - 1) {
                    value = unflatten(fields, level + 1, anchor, current - anchor);
                }
                else {
                    value = field.getValue();
                }
                result.put(name, value);
                anchor = current;
            }
        }
        while (current < limit);

        return result;
    }

    private static final class Field
    {
        private final String name;
        private final List<String> path;
        private final Object value;

        public Field(String name, Object value)
        {
            this.name = name;
            this.path = Arrays.asList(name.split("\\."));
            this.value = value;
        }

        public String getName()
        {
            return name;
        }

        public int getDepth()
        {
            return path.size();
        }

        public String getPathElement(int level)
        {
            return path.get(level);
        }

        public Object getValue()
        {
            return value;
        }
    }
}
