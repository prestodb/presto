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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_MAX_HITS_EXCEEDED;
import static com.facebook.presto.elasticsearch.ElasticsearchUtils.serializeObject;
import static com.facebook.presto.elasticsearch.RetryDriver.retry;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordCursor
        implements RecordCursor
{
    private static final JsonCodec<Object> VALUE_CODEC = jsonCodec(Object.class);

    private final List<ElasticsearchColumnHandle> columnHandles;
    private final Map<String, Integer> jsonPathToIndex = new HashMap<>();
    private final int maxHits;
    private final Iterator<SearchHit> searchHits;
    private final Duration requestTimeout;
    private final int maxAttempts;
    private final Duration maxRetryTime;
    private final ElasticsearchQueryBuilder builder;

    private long totalBytes;
    private List<Object> fields;

    public ElasticsearchRecordCursor(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchConnectorConfig config, ElasticsearchSplit split)
    {
        requireNonNull(columnHandles, "columnHandle is null");
        requireNonNull(config, "config is null");

        this.columnHandles = columnHandles;
        this.maxHits = config.getMaxHits();
        this.requestTimeout = config.getRequestTimeout();
        this.maxAttempts = config.getMaxRequestRetries();
        this.maxRetryTime = config.getMaxRetryTime();

        for (int i = 0; i < columnHandles.size(); i++) {
            jsonPathToIndex.put(columnHandles.get(i).getColumnJsonPath(), i);
        }
        this.builder = new ElasticsearchQueryBuilder(columnHandles, config, split);
        this.searchHits = sendElasticsearchQuery(builder).iterator();
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
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!searchHits.hasNext()) {
            return false;
        }

        SearchHit hit = searchHits.next();
        fields = new ArrayList<>(Collections.nCopies(columnHandles.size(), null));

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
        return serializeObject(columnHandles.get(field).getColumnType(), null, getFieldValue(field));
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getFieldValue(field) == null;
    }

    private void checkFieldType(int field, Set<Type> expectedTypes)
    {
        checkArgument(expectedTypes.contains(getType(field)), "Field %s has unexpected type %s", field, getType(field));
    }

    @Override
    public void close()
    {
        builder.close();
    }

    private SearchResponse getSearchResponse(ElasticsearchQueryBuilder queryBuilder)
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("searchRequest", () -> queryBuilder.buildScrollSearchRequest()
                            .execute()
                            .actionGet(requestTimeout.toMillis()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SearchResponse getScrollResponse(ElasticsearchQueryBuilder queryBuilder, String scrollId)
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("scrollRequest", () -> queryBuilder.prepareSearchScroll(scrollId)
                            .execute()
                            .actionGet(requestTimeout.toMillis()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<SearchHit> sendElasticsearchQuery(ElasticsearchQueryBuilder queryBuilder)
    {
        SearchResponse response = getSearchResponse(queryBuilder);

        if (response.getHits().getTotalHits() > maxHits) {
            throw new PrestoException(ELASTICSEARCH_MAX_HITS_EXCEEDED,
                    format("The number of hits for the query (%d) exceeds the configured max hits (%d)", response.getHits().getTotalHits(), maxHits));
        }

        ImmutableList.Builder<SearchHit> result = ImmutableList.builder();
        while (true) {
            for (SearchHit hit : response.getHits().getHits()) {
                result.add(hit);
            }
            response = getScrollResponse(queryBuilder, response.getScrollId());
            if (response.getHits().getHits().length == 0) {
                break;
            }
        }
        return result.build();
    }

    private void setFieldIfExists(String jsonPath, Object jsonValue)
    {
        if (jsonPathToIndex.containsKey(jsonPath)) {
            fields.set(jsonPathToIndex.get(jsonPath), jsonValue);
        }
    }

    private Object getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
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
