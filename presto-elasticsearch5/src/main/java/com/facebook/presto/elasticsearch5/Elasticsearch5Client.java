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
package com.facebook.presto.elasticsearch5;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.facebook.presto.elasticsearch.BaseClient;
import com.facebook.presto.elasticsearch.ElasticsearchTable;
import com.facebook.presto.elasticsearch.EsTypeManager;
import com.facebook.presto.elasticsearch.conf.ElasticsearchConfig;
import com.facebook.presto.elasticsearch.conf.ElasticsearchSessionProperties;
import com.facebook.presto.elasticsearch.io.Document;
import com.facebook.presto.elasticsearch.io.SearchResult;
import com.facebook.presto.elasticsearch.metadata.EsField;
import com.facebook.presto.elasticsearch.metadata.EsIndex;
import com.facebook.presto.elasticsearch.metadata.IndexResolution;
import com.facebook.presto.elasticsearch.metadata.MappingException;
import com.facebook.presto.elasticsearch.metadata.Types;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchSplit;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableLayoutHandle;
import com.facebook.presto.elasticsearch5.functions.MatchQueryFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ES_DSL_ERROR;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ES_MAPPING_ERROR;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.IO_ERROR;
import static com.facebook.presto.elasticsearch.Types.isArrayType;
import static com.facebook.presto.elasticsearch.Types.isMapType;
import static com.facebook.presto.elasticsearch.Types.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Elasticsearch5Client
        implements BaseClient
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Client client;
    private EsTypeManager typeManager;

    @javax.inject.Inject
    public Elasticsearch5Client(
            EsTypeManager typeManager,
            Client client,
            ElasticsearchConfig elasticsearchConfig)
    {
        this.typeManager = typeManager;
        this.client = requireNonNull(client, "elasticsearch client is null");
    }

    @Override
    public Set<String> getSchemaNames()
    {
        return ImmutableSet.of("default");
    }

    @Override
    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        String[] indexs = client.admin().indices().prepareGetIndex().get().indices();
        return ImmutableSet.copyOf(indexs);
    }

    @Override
    public List<ElasticsearchSplit> getTabletSplits(
            ConnectorSession session,
            ElasticsearchTableHandle tableHandle,
            ElasticsearchTableLayoutHandle layoutHandle, ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy)
    {
        final String index = tableHandle.getTableName();

        final long timeValue = ElasticsearchSessionProperties.getScrollSearchTimeout(session);  //Scroll duration 1m
        final boolean splitShardsEnabled = ElasticsearchSessionProperties.isOptimizeSplitShardsEnabled(session);
        final int batchSize = ElasticsearchSessionProperties.getScrollSearchBatchSize(session);
        final Map<String, String> queryDsl = getQueryDsl(layoutHandle.getConstraint());
        final ImmutableList.Builder<SearchRequest> splitBuilder = ImmutableList.builder();

        if (splitShardsEnabled && splitSchedulingStrategy == GROUPED_SCHEDULING) {
            for (ClusterSearchShardsGroup shardsGroup : client.admin().cluster().prepareSearchShards(index).get().getGroups()) {
                int shardId = shardsGroup.getShardId().getId();
                SearchRequestBuilder requestBuilder = client.prepareSearch(index)
                        .setQuery(QueryBuilders.wrapperQuery(queryDsl.get("_allDsl")))
                        .setPreference("_shards:" + shardId)   //demo: _shards:2,3
                        .setSize(batchSize)  //max of 100 hits will be returned for each scroll
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)   //es 5.x
                        .setScroll(new TimeValue(timeValue));  //Scroll duration 1m

                splitBuilder.add(requestBuilder.request());
            }
        }
        else {
            SearchRequestBuilder requestBuilder = client.prepareSearch(index)
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                    .addSort(SortBuilders.fieldSort("_doc"))   //es 5.x
                    .setScroll(new TimeValue(timeValue))  //Scroll duration 1m
                    .setQuery(QueryBuilders.wrapperQuery(queryDsl.get("_allDsl")))
                    .setSize(batchSize);  //max of 100 hits will be returned for each scroll

            splitBuilder.add(requestBuilder.request());
        }

        return splitBuilder.build().stream().map(searchRequest -> {
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                searchRequest.writeTo(output);

                return new ElasticsearchSplit(
                        tableHandle.getConnectorId(),
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        output.bytes().toBytesRef().bytes,
                        timeValue,
                        queryDsl,
                        Optional.empty());
            }
            catch (IOException e) {
                throw new PrestoException(IO_ERROR, e);
            }
        }).collect(Collectors.toList());
    }

    static NamedWriteableRegistry getNamedWriteableRegistry()
    {
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    private static Map<String, String> getQueryDsl(TupleDomain<ColumnHandle> constraint)
    {
        final Map<String, Object> mergeDslMap = new HashMap<>();
        Map<String, String> dslCacher = new HashMap<>();

        if (constraint.getColumnDomains().isPresent()) {
            for (TupleDomain.ColumnDomain<ColumnHandle> cd : constraint.getColumnDomains().get()) {
                ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) cd.getColumn();
                String columnName = column.getName();

                if ("_type".equals(columnName)) {
                    throw new UnsupportedOperationException("this _type filter have't support!");
                }
                else if (columnName.startsWith("_")) {
                    getRangesFromDomain(cd.getDomain()).forEach(range -> {
                        checkArgument(range.isSingleValue(), "dsl is must [=] demo where _dsl = \"..dsl string\"");
                        checkArgument(range.getType() instanceof VarcharType, "_dsl filter is not string");
                        String dsl = ((Slice) range.getSingleValue()).toStringUtf8();
                        dslCacher.put(columnName, dsl);
                        if (!"_dsl".equals(columnName)) {
                            dsl = dsl.replace(MatchQueryFunction.MATCH_COLUMN_SEP, columnName.substring(1));
                        }
                        addEsQueryFilter(mergeDslMap, dsl);
                    });
                }
                else {
                    getRangesFromDomain(cd.getDomain()).forEach(range -> {
                        checkArgument(column.getType().equals(range.getType()), "filter type is " + range.getType() + " but column [" + columnName + "] type is " + column.getType());
                        QueryBuilder queryBuilder = getQueryBuilderFromPrestoRange(columnName, range);
                        addEsQueryFilter(mergeDslMap, queryBuilder.toString());
                    });
                }
            }
        }
        try {
            String allDsl = mergeDslMap.isEmpty() ? QueryBuilders.boolQuery().toString() :
                    MAPPER.writeValueAsString(mergeDslMap.get("query"));   //Es5 and 6.x can only use the child nodes of query
            dslCacher.put("_allDsl", allDsl);
            return dslCacher;
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(ES_DSL_ERROR, e);
        }
    }

    private static void addEsQueryFilter(Map<String, Object> mergeDslMap, String dsl)
    {
        //-------get query-----
        Map<String, Object> queryDsl;
        try {
            final Map<String, Object> dslMap = MAPPER.readValue(dsl, Map.class);
            queryDsl = (Map<String, Object>) dslMap.getOrDefault("query", dslMap);
        }
        catch (IOException e) {
            throw new PrestoException(ES_DSL_ERROR, e);
        }

        Map<String, Object> query = (Map<String, Object>) mergeDslMap.computeIfAbsent("query", k -> new HashMap<>());
        Map bool = (Map) query.computeIfAbsent("bool", k -> new HashMap<>());
        List must = (List) bool.computeIfAbsent("must", k -> new ArrayList<>());
        //--merge dsl--
        must.add(queryDsl);
    }

    private static QueryBuilder getQueryBuilderFromPrestoRange(String columnName, Range prestoRange)
            throws TableNotFoundException
    {
        Type type = prestoRange.getType();
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        if (prestoRange.isAll()) { //scan all _id
        }
        else if (prestoRange.isSingleValue()) {
            Object value = prestoRange.getSingleValue();
            qb.must(QueryBuilders.termQuery(columnName, EsTypeManager.getTypeValue(type, value)));
        }
        else {
            if (prestoRange.getHigh().isUpperUnbounded()) {
                // If high is unbounded, then create a range from (value, +inf), checking inclusivity
                Object value = prestoRange.getLow().getValue();
                qb.must(QueryBuilders.rangeQuery(columnName).gte(EsTypeManager.getTypeValue(type, value)));
            }
            else if (prestoRange.getLow().isLowerUnbounded()) {
                // If low is unbounded, then create a range from (-inf, value), checking inclusivity
                Object value = prestoRange.getHigh().getValue();
                qb.must(QueryBuilders.rangeQuery(columnName).lte(EsTypeManager.getTypeValue(type, value)));
            }
            else {
                // If high is unbounded, then create a range from low to high, checking inclusivity
                //Type type = prestoRange.getType();
                Object startSplit = EsTypeManager.getTypeValue(type, prestoRange.getLow().getValue());
                Object endSplit = EsTypeManager.getTypeValue(type, prestoRange.getHigh().getValue());
                //------- set start and stop -----
                qb.must(QueryBuilders.rangeQuery(columnName).gte(startSplit).lte(endSplit));
            }
        }

        return qb;
    }

    /**
     * Gets a collection of Hbase Range objects from the given Presto domain.
     * This maps the column constraints of the given Domain to an Hbase Range scan.
     *
     * @param domain Domain, can be null (returns (-inf, +inf) Range)
     * @return A collection of Elasticsearch search objects
     * @throws TableNotFoundException If the Elasticsearch index is not found
     */
    public static Collection<Range> getRangesFromDomain(Domain domain)
            throws TableNotFoundException
    {
        Collection<Range> rangeBuilder = domain.getValues().getRanges().getOrderedRanges();

        return rangeBuilder;
    }

    @Override
    public SearchResult<Map<String, Object>> execute(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columns)
    {
        byte[] slice = split.getSearchRequest();
        final SearchRequest deserializedRequest = new SearchRequest();
        StreamInput streamInput = new ByteBufferStreamInput(ByteBuffer.wrap(slice));
        NamedWriteableRegistry namedWriteableRegistry = getNamedWriteableRegistry();
        try (StreamInput in = new NamedWriteableAwareStreamInput(streamInput, namedWriteableRegistry)) {
            deserializedRequest.readFrom(in);
        }
        catch (IOException e) {
            throw new PrestoException(IO_ERROR, e);
        }

        return new SearchResult<Map<String, Object>>()
        {
            private final Function<SearchResponse, Iterator<ImmutableMap<String, Object>>> func = (scrollResp) -> {
                SearchHit[] searchHits = scrollResp.getHits().getHits();
                return Arrays.stream(searchHits).map(searchHitFields -> {
                    ImmutableMap.Builder<String, Object> sourceLine = ImmutableMap.builder();
                    sourceLine.putAll(searchHitFields.sourceAsMap());
                    sourceLine.put("_type", searchHitFields.getType());
                    sourceLine.put("_id", searchHitFields.getId());
                    sourceLine.put("_score", searchHitFields.getScore());
                    sourceLine.putAll(split.getPushDownDsl());  // add PushDown dsl
                    return sourceLine.build();
                }).iterator();
            };
            private SearchResponse firstScrollResp = client.search(deserializedRequest).actionGet();
            private Iterator<ImmutableMap<String, Object>> batchHitIterator = func.apply(firstScrollResp);

            @Override
            public boolean hasNext()
            {
                if (batchHitIterator.hasNext()) {
                    return true;
                }
                //---- For the first time above es5.x, Scroll has data. ----
                final SearchResponse scrollResp = client.prepareSearchScroll(firstScrollResp.getScrollId())
                        .setScroll(new TimeValue(split.getTimeValue()))
                        .execute().actionGet();

                this.batchHitIterator = func.apply(scrollResp);
                return batchHitIterator.hasNext();
            }

            @Override
            public Map<String, Object> next()
            {
                return batchHitIterator.next();
            }

            @Override
            public void close()
                    throws IOException
            {
                client.prepareClearScroll().addScrollId(firstScrollResp.getScrollId()).execute().actionGet();
            }
        };
    }

    @Override
    public ElasticsearchTable getTable(SchemaTableName tableName)
    {
        String indexWildcard = tableName.getTableName();
        GetIndexRequest getIndexRequest = createGetIndexRequest(indexWildcard);
        //----es scher error --
        Thread.currentThread().setName("getTable_001");
        GetIndexResponse response = client.admin().indices()
                .getIndex(getIndexRequest).actionGet();
        if (response.getIndices() == null || response.getIndices().length == 0) {
            return null;
        }

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = response.getMappings();
        List<IndexResolution> resolutions;
        if (mappings.size() > 0) {
            resolutions = new ArrayList<>(mappings.size());
            for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexMappings : mappings) {
                resolutions.add(buildGetIndexResult(indexMappings.key, indexMappings.value));
            }
        }
        else {
            resolutions = emptyList();
        }
        // In the es index name can be used to match *, may return multiple index mapping,
        // so the following need to merge merge demo: table = test1 "*"
        IndexResolution indexWithMerged = merge(resolutions, indexWildcard);
        return new ElasticsearchTable(typeManager, tableName.getSchemaName(), tableName.getTableName(), indexWithMerged.get());
    }

    @Override
    public void insertMany(List<Document> docs)
    {
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (Document doc : docs) {
            bulkRequestBuilder.add(new IndexRequest()
                    .index(doc.getIndex())
                    .type(doc.getType())
                    .id(doc.getId())
                    .source(doc.getSource()));
        }
        BulkResponse response = bulkRequestBuilder.execute().actionGet();
        if (response.hasFailures()) {
            throw new PrestoException(IO_ERROR, response.buildFailureMessage());
        }
    }

    @Override
    public boolean existsTable(SchemaTableName schemaTableName)
    {
        return client.admin().indices().prepareExists(schemaTableName.getTableName())
                .execute().actionGet().isExists();
    }

    @Override
    public void dropTable(SchemaTableName schemaTableName)
    {
        client.admin().indices().prepareDelete(schemaTableName.getTableName()).execute().actionGet();
    }

    @Override
    public void createTable(ConnectorTableMetadata tableMetadata)
    {
        XContentBuilder mapping = getMapping(tableMetadata.getColumns());
        String index = tableMetadata.getTable().getTableName();
        try {
            //TODO: default type value is presto
            client.admin().indices().prepareCreate(index)
                    .addMapping("presto", mapping)
                    .execute().actionGet();
        }
        catch (MapperParsingException e) {
            throw new PrestoException(ES_MAPPING_ERROR, "Failed create index:" + index, e);
        }
    }

    private static XContentBuilder getMapping(List<ColumnMetadata> columns)
    {
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject().startObject("properties");
            for (ColumnMetadata columnMetadata : columns) {
                String columnName = columnMetadata.getName();
                Type type = columnMetadata.getType();
                if ("@timestamp".equals(columnName)) {    //break @timestamp field
                    continue;
                }
                mapping.startObject(columnName)
                        .field("type", getEsType(type))
                        //.field("index", "not_analyzed")
                        .endObject();
            }
            mapping.endObject().endObject();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }

    private static String getEsType(Type type)
    {
        //Type mapping
        // see:https://www.elastic.co/guide/en/elasticsearch/reference/6.3/mapping-types.html
        if (type.equals(BooleanType.BOOLEAN)) {
            return "boolean";
        }
        if (type.equals(BigintType.BIGINT)) {
            return "long";
        }
        if (type.equals(IntegerType.INTEGER)) {
            return "integer";
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return "short";
        }
        if (type.equals(TinyintType.TINYINT)) {
            return "byte";
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return "double";
        }
        if (isVarcharType(type)) {
            //TODO: text or keyword ?
            return "text";
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return "binary";
        }
        if (type.equals(DateType.DATE)) {
            return "date";
        }
        if (type.equals(TimeType.TIME)) {
            return "date";
        }
        if (type.equals(TimestampType.TIMESTAMP)) {
            return "date";
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
            //TODO: TIMESTAMP_WITH_TIME_ZONE
            return "date";
        }
        if (type instanceof DecimalType) {
            return "double";
        }
        if (isArrayType(type)) {
            Type elementType = type.getTypeParameters().get(0);
            if (isArrayType(elementType) || isMapType(elementType) || isRowType(elementType)) {
                throw new PrestoException(NOT_SUPPORTED, "sorry unsupported type: " + type);
            }
            return getEsType(elementType);
        }
        if (isMapType(type)) {
            throw new PrestoException(NOT_SUPPORTED, "sorry unsupported type: " + type);
        }
        if (isRowType(type)) {
            throw new PrestoException(NOT_SUPPORTED, "sorry unsupported type: " + type);
        }

        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    private static IndexResolution buildGetIndexResult(String indexOrAlias,
            ImmutableOpenMap<String, MappingMetaData> mappings)
    {
        // Make sure that the index contains only a single type
        MappingMetaData singleType = null;
        List<String> typeNames = null;
        for (ObjectObjectCursor<String, MappingMetaData> type : mappings) {
            //Default mappings are ignored as they are applied to each type. Each type alone holds all of its fields.
            if ("_default_".equals(type.key)) {
                continue;
            }
            if (singleType != null) {
                // There are more than one types
                if (typeNames == null) {
                    typeNames = new ArrayList<>();
                    typeNames.add(singleType.type());
                }
                typeNames.add(type.key);
            }
            singleType = type.value;
        }

        if (singleType == null) {
            return IndexResolution.invalid("[" + indexOrAlias + "] doesn't have any types so it is incompatible with sql");
        }
        else if (typeNames != null) {
            Collections.sort(typeNames);
            //Es5 does not support multiple types--
            return IndexResolution.invalid(
                    "[" + indexOrAlias + "] contains more than one type " + typeNames + " so it is incompatible with sql");
        }
        else {
            try {
                Map<String, EsField> mapping = Types.fromEs(singleType.sourceAsMap());
                return IndexResolution.valid(new EsIndex(indexOrAlias, mapping));
            }
            catch (MappingException | IOException ex) {
                return IndexResolution.invalid(ex.getMessage());
            }
        }
    }

    private static IndexResolution merge(List<IndexResolution> resolutions, String indexWildcard)
    {
        IndexResolution merged = null;
        for (IndexResolution resolution : resolutions) {
            // everything that follows gets compared
            if (!resolution.isValid()) {
                return resolution;
            }
            // initialize resolution on first run
            if (merged == null) {
                merged = resolution;
            }
            // need the same mapping across all resolutions
            if (!merged.get().mapping().equals(resolution.get().mapping())) {
                return IndexResolution.invalid(
                        "[" + indexWildcard + "] points to indices [" + resolution.get().name() + "] "
                                + "and [" + resolution.get().name() + "] which have different mappings. "
                                + "When using multiple indices, the mappings must be identical.");
            }
        }
        if (merged != null) {
            // at this point, we are sure there's the same mapping across all (if that's the case) indices
            // to keep things simple, use the given pattern as index name
            merged = IndexResolution.valid(new EsIndex(indexWildcard, merged.get().mapping()));
        }
        else {
            merged = IndexResolution.notFound(indexWildcard);
        }
        return merged;
    }

    private static GetIndexRequest createGetIndexRequest(String index)
    {
        return new GetIndexRequest()
                .local(true)
                .indices(index)
                .features(GetIndexRequest.Feature.MAPPINGS)
                //lenient because we throw our own errors looking at the response e.g. if something was not resolved
                //also because this way security doesn't throw authorization exceptions but rather honours ignore_unavailable
                .indicesOptions(IndicesOptions.lenientExpandOpen());
    }
}
