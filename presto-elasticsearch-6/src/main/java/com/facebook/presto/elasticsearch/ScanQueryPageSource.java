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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.elasticsearch.client.ElasticsearchClient;
import com.facebook.presto.elasticsearch.decoders.ArrayDecoder;
import com.facebook.presto.elasticsearch.decoders.BigintDecoder;
import com.facebook.presto.elasticsearch.decoders.BooleanDecoder;
import com.facebook.presto.elasticsearch.decoders.Decoder;
import com.facebook.presto.elasticsearch.decoders.DoubleDecoder;
import com.facebook.presto.elasticsearch.decoders.IdColumnDecoder;
import com.facebook.presto.elasticsearch.decoders.IntegerDecoder;
import com.facebook.presto.elasticsearch.decoders.IpAddressDecoder;
import com.facebook.presto.elasticsearch.decoders.RealDecoder;
import com.facebook.presto.elasticsearch.decoders.RowDecoder;
import com.facebook.presto.elasticsearch.decoders.ScoreColumnDecoder;
import com.facebook.presto.elasticsearch.decoders.SmallintDecoder;
import com.facebook.presto.elasticsearch.decoders.SourceColumnDecoder;
import com.facebook.presto.elasticsearch.decoders.TimestampDecoder;
import com.facebook.presto.elasticsearch.decoders.TinyintDecoder;
import com.facebook.presto.elasticsearch.decoders.VarbinaryDecoder;
import com.facebook.presto.elasticsearch.decoders.VarcharDecoder;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.elasticsearch.BuiltinColumns.ID;
import static com.facebook.presto.elasticsearch.BuiltinColumns.SCORE;
import static com.facebook.presto.elasticsearch.BuiltinColumns.SOURCE;
import static com.facebook.presto.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

public class ScanQueryPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(ScanQueryPageSource.class);

    private final List<Decoder> decoders;

    private final SearchHitIterator iterator;
    private final BlockBuilder[] columnBuilders;
    private final List<ElasticsearchColumnHandle> columns;
    private long totalBytes;
    private long readTimeNanos;
    private long completedPositions;

    public ScanQueryPageSource(
            ElasticsearchClient client,
            ConnectorSession session,
            ElasticsearchTableHandle table,
            ElasticsearchSplit split,
            List<ElasticsearchColumnHandle> columns)
    {
        requireNonNull(client, "client is null");
        requireNonNull(columns, "columns is null");

        this.columns = ImmutableList.copyOf(columns);

        decoders = createDecoders(session, columns);

        // When the _source field is requested, we need to bypass column pruning when fetching the document
        boolean needAllFields = columns.stream()
                .map(ElasticsearchColumnHandle::getName)
                .anyMatch(isEqual(SOURCE.getName()));

        // Columns to fetch as doc_fields instead of pulling them out of the JSON source
        // This is convenient for types such as DATE, TIMESTAMP, etc, which have multiple possible
        // representations in JSON, but a single normalized representation as doc_field.
        List<String> documentFields = flattenFields(columns).entrySet().stream()
                .filter(entry -> entry.getValue().equals(TIMESTAMP))
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        columnBuilders = columns.stream()
                .map(ElasticsearchColumnHandle::getType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);

        List<String> requiredFields = columns.stream()
                .map(ElasticsearchColumnHandle::getName)
                .filter(name -> !BuiltinColumns.NAMES.contains(name))
                .collect(toList());

        // sorting by _doc (index order) get special treatment in Elasticsearch and is more efficient
        Optional<String> sort = Optional.of("_doc");

        if (table.getQuery().isPresent()) {
            // However, if we're using a custom Elasticsearch query, use default sorting.
            // Documents will be scored and returned based on relevance
            sort = Optional.empty();
        }

        long start = System.nanoTime();
        SearchResponse searchResponse = client.beginSearch(
                split.getIndex(),
                split.getShard(),
                buildSearchQuery(session, split.getTupleDomain().transform(ElasticsearchColumnHandle.class::cast), table.getQuery()),
                needAllFields ? Optional.empty() : Optional.of(requiredFields),
                documentFields,
                sort);
        readTimeNanos += System.nanoTime() - start;
        this.iterator = new SearchHitIterator(client, () -> searchResponse);
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + iterator.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !iterator.hasNext();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        iterator.close();
    }

    @Override
    public Page getNextPage()
    {
        long size = 0;
        while (size < PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES && iterator.hasNext()) {
            SearchHit hit = iterator.next();
            Map<String, Object> document = hit.getSourceAsMap();

            for (int i = 0; i < decoders.size(); i++) {
                String field = columns.get(i).getName();
                decoders.get(i).decode(hit, () -> getField(document, field), columnBuilders[i]);
            }

            if (hit.getSourceRef() != null) {
                totalBytes += hit.getSourceRef().length();
            }

            completedPositions += 1;
            size = Arrays.stream(columnBuilders)
                    .mapToLong(BlockBuilder::getSizeInBytes)
                    .sum();
        }

        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }

        return new Page(blocks);
    }

    public static Object getField(Map<String, Object> document, String field)
    {
        Object value = document.get(field);
        if (value == null) {
            Map<String, Object> result = new HashMap<>();
            String prefix = field + ".";
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(prefix)) {
                    result.put(key.substring(prefix.length()), entry.getValue());
                }
            }

            if (!result.isEmpty()) {
                return result;
            }
        }

        return value;
    }

    private Map<String, Type> flattenFields(List<ElasticsearchColumnHandle> columns)
    {
        Map<String, Type> result = new HashMap<>();

        for (ElasticsearchColumnHandle column : columns) {
            flattenFields(result, column.getName(), column.getType());
        }

        return result;
    }

    private void flattenFields(Map<String, Type> result, String fieldName, Type type)
    {
        if (type instanceof RowType) {
            for (RowType.Field field : ((RowType) type).getFields()) {
                flattenFields(result, appendPath(fieldName, field.getName().get()), field.getType());
            }
        }
        else {
            result.put(fieldName, type);
        }
    }

    private List<Decoder> createDecoders(ConnectorSession session, List<ElasticsearchColumnHandle> columns)
    {
        return columns.stream()
                .map(column -> {
                    if (column.getName().equals(ID.getName())) {
                        return new IdColumnDecoder();
                    }

                    if (column.getName().equals(SCORE.getName())) {
                        return new ScoreColumnDecoder();
                    }

                    if (column.getName().equals(SOURCE.getName())) {
                        return new SourceColumnDecoder();
                    }

                    return createDecoder(session, column.getName(), column.getType());
                })
                .collect(toImmutableList());
    }

    private Decoder createDecoder(ConnectorSession session, String path, Type type)
    {
        if (type.equals(VARCHAR)) {
            return new VarcharDecoder(path);
        }
        else if (type.equals(VARBINARY)) {
            return new VarbinaryDecoder(path);
        }
        else if (type.equals(TIMESTAMP)) {
            return new TimestampDecoder(session, path);
        }
        else if (type.equals(BOOLEAN)) {
            return new BooleanDecoder(path);
        }
        else if (type.equals(DOUBLE)) {
            return new DoubleDecoder(path);
        }
        else if (type.equals(REAL)) {
            return new RealDecoder(path);
        }
        else if (type.equals(TINYINT)) {
            return new TinyintDecoder(path);
        }
        else if (type.equals(SMALLINT)) {
            return new SmallintDecoder(path);
        }
        else if (type.equals(INTEGER)) {
            return new IntegerDecoder(path);
        }
        else if (type.equals(BIGINT)) {
            return new BigintDecoder(path);
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.IPADDRESS)) {
            return new IpAddressDecoder(path, type);
        }
        else if (type instanceof RowType) {
            RowType rowType = (RowType) type;

            List<Decoder> decoders = rowType.getFields().stream()
                    .map(field -> createDecoder(session, appendPath(path, field.getName().get()), field.getType()))
                    .collect(toImmutableList());

            List<String> fieldNames = rowType.getFields().stream()
                    .map(RowType.Field::getName)
                    .map(Optional::get)
                    .collect(toImmutableList());

            return new RowDecoder(path, fieldNames, decoders);
        }
        else if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();

            return new ArrayDecoder(path, createDecoder(session, path, elementType));
        }

        throw new UnsupportedOperationException("Type not supported: " + type);
    }

    private static String appendPath(String base, String element)
    {
        if (base.isEmpty()) {
            return element;
        }

        return base + "." + element;
    }

    private static class SearchHitIterator
            extends AbstractIterator<SearchHit>
    {
        private final ElasticsearchClient client;
        private final Supplier<SearchResponse> first;

        private SearchHits searchHits;
        private String scrollId;
        private int currentPosition;

        private long readTimeNanos;

        public SearchHitIterator(ElasticsearchClient client, Supplier<SearchResponse> first)
        {
            this.client = client;
            this.first = first;
        }

        public long getReadTimeNanos()
        {
            return readTimeNanos;
        }

        @Override
        protected SearchHit computeNext()
        {
            if (scrollId == null) {
                long start = System.nanoTime();
                SearchResponse response = first.get();
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }
            else if (currentPosition == searchHits.getHits().length) {
                long start = System.nanoTime();
                SearchResponse response = client.nextPage(scrollId);
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }

            if (currentPosition == searchHits.getHits().length) {
                return endOfData();
            }

            SearchHit hit = searchHits.getAt(currentPosition);
            currentPosition++;

            return hit;
        }

        private void reset(SearchResponse response)
        {
            scrollId = response.getScrollId();
            searchHits = response.getHits();
            currentPosition = 0;
        }

        public void close()
        {
            if (scrollId != null) {
                try {
                    client.clearScroll(scrollId);
                }
                catch (Exception e) {
                    // ignore
                    LOG.debug("Error clearing scroll", e);
                }
            }
        }
    }
}
