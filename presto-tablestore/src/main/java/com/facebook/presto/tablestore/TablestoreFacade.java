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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse.RowResult;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeRequest;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeResponse;
import com.alicloud.openservices.tablestore.model.ComputeSplitsRequest;
import com.alicloud.openservices.tablestore.model.ComputeSplitsResponse;
import com.alicloud.openservices.tablestore.model.DataBlockType;
import com.alicloud.openservices.tablestore.model.DefinedColumnSchema;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.ListTableResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RangeIteratorParameter;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.SearchIndexSplitsOptions;
import com.alicloud.openservices.tablestore.model.SimpleRowMatrixBlockRowIterator;
import com.alicloud.openservices.tablestore.model.Split;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter.LogicOperator;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter;
import com.alicloud.openservices.tablestore.model.iterator.RowIterator;
import com.alicloud.openservices.tablestore.model.iterator.SearchRowIterator;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.SearchIndexInfo;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchRequest.ColumnsToGet;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.ExistsQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.Ranges;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder.createPrimaryKeyBuilder;
import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MAX;
import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MIN;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.EXIST;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.GREATER_EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.GREATER_THAN;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.LESS_EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.LESS_THAN;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.NOT_EXIST;
import static com.facebook.presto.common.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.common.predicate.Marker.Bound.EXACTLY;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.tablestore.IndexSelectionStrategy.Type.THRESHOLD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class TablestoreFacade
        extends TablestoreSessionProperties
        implements AutoCloseable
{
    private static final Logger log = Logger.get(TablestoreFacade.class);
    public static final int NANO_SECS_IN_ONE_MS = 1000_000;
    public static final int MAX_RANGE_TO_POINT_SIZE = 100;
    private final SyncClient syncClient;
    private final TablestoreConfig config;

    public TablestoreFacade(TablestoreConfig config)
    {
        this(config, TablestoreClientHelper.createSyncClient(config, Optional.empty()));
        log.info("create a new SyncClient, connectorId=tablestore, config=%s", config);
    }

    public TablestoreFacade(TablestoreConfig config, SyncClient syncClient)
    {
        this.config = config;
        this.syncClient = syncClient;
    }

    public <R> Triple<R, Long, Long> executeRemote(Function<SyncClient, R> call,
            String method, Object... params)
    {
        Exception cause;
        String type;
        String traceId = null;
        String requestId = null;
        try {
            long t1 = nanoTime();
            long t2 = nanoTime();
            R r = call.apply(syncClient);
            long t3 = nanoTime();
            log.info(
                    "Getting a SyncClient from cache and remote call %s(), clientCosts=%sns apiCost=%sns.",
                    method, (t2 - t1), (t3 - t2));
            return Triple.of(r, (t2 - t1), t3 - t2);
        }
        catch (TableStoreException e) {
            type = "table_store_error";
            traceId = e.getTraceId();
            requestId = e.getRequestId();
            cause = e;
        }
        catch (ClientException e) {
            type = "network_abnormal_error";
            traceId = e.getTraceId();
            cause = e;
        }
        catch (Exception e) {
            type = "others_error";
            cause = e;
        }
        String error = "Error communicate with TableStore. Detailed message: %s() failed, type=%s, traceId=%s, requestId=%s, params={%s}, because: %s";
        String paramsStr = Joiner.on(",").join(params);
        error = format(error, method, type, traceId, requestId, paramsStr, cause.getMessage());
        log.error("%s() failed, type=%s, traceId=%s, requestId=%s, params=[%s], because: %s",
                method, type, traceId, requestId, paramsStr, cause.getMessage(), cause);
        throw new PrestoException(GENERIC_INTERNAL_ERROR, error, cause);
    }

    public TablestoreTableHandle getTableHandle(ConnectorSession session, SchemaTableName stn)
    {
        String queryId = session.getQueryId();
        String ramUserId = session.getUser();
        log.info("getTableHandle() start, queryId=%s ramUserId=%s table=%s.", queryId, ramUserId, stn);
        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(stn.getTableName());
        DescribeTableResponse response = syncClient.describeTable(request);
        TableMeta tableMeta = response.getTableMeta();
        List<TablestoreColumnHandle> orderedHandles = new ArrayList<>();
        List<PrimaryKeySchema> primaryKeySchemas = tableMeta.getPrimaryKeyList();
        int pkPosition = 0;
        for (PrimaryKeySchema schema : primaryKeySchemas) {
            orderedHandles.add(new TablestoreColumnHandle(schema.getName(), true, pkPosition++, convertToPrestoType(schema.getType().name())));
        }

        for (DefinedColumnSchema schema : tableMeta.getDefinedColumnsList()) {
            orderedHandles.add(new TablestoreColumnHandle(schema.getName(), false, 0, convertToPrestoType(schema.getType().name())));
        }

        log.info("getTableHandle() end, queryId=%s", queryId);
        TablestoreTableHandle tableHandle = new TablestoreTableHandle(stn, orderedHandles);
        return tableHandle;
    }

    public static Type convertToPrestoType(String columnType)
    {
        TypeSignature ts = TypeSignature.parseTypeSignature(columnType.toLowerCase(Locale.getDefault()));
        switch (ts.getBase()) {
            case "boolean":
                return BOOLEAN;
            case "integer":
                return INTEGER;
            case "double":
                return DOUBLE;
            case "string":
                return VARCHAR;
            case "binary":
                return VARBINARY;
        }
        throw new IllegalArgumentException("Unsupported type: " + columnType);
    }

    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(config.getInstance());
    }

    public List<SchemaTableName> listTables(ConnectorSession session, @Nullable String schemaNameOrNull)
    {
        ListTableResponse listTableResponse = syncClient.listTable();
        return listTableResponse.getTableNames().stream()
                .map(tableName -> new SchemaTableName(config.getInstance(), tableName))
                .collect(toList());
    }

    public List<? extends ConnectorSplit> getSplits(ConnectorSession session, TablestoreTableLayoutHandle layoutHandle)
    {
        String stn = layoutHandle.getTable().getPrintableStn();
        String queryId = session.getQueryId();
        String ramUserId = session.getUser();
        try {
            log.info("getSplits() start, queryId=%s ramUserId=%s table=%s", queryId, ramUserId, stn);
            TablestoreTableHandle th = layoutHandle.getTable();
            TupleDomain<ColumnHandle> constraint = layoutHandle.getTupleDomain().orElseThrow(NullPointerException::new);
            if (constraint.isNone()) {
                return emptyList();
            }

            Map<ColumnHandle, Domain> domains = constraint.getDomains().orElseThrow(NullPointerException::new);
            Optional<ColumnHandle> pkIsNull = domains.entrySet().stream().filter(k -> {
                TablestoreColumnHandle kk = (TablestoreColumnHandle) k.getKey();
                Domain dv = k.getValue();
                return kk.isPrimaryKey() && dv.isOnlyNull();
            }).findFirst().map(Entry::getKey);

            String tupleDomainStr = constraint.toString(session.getSqlFunctionProperties());
            if (pkIsNull.isPresent()) {
                log.info("The nullable value of primary key[%s] isn't allowed in Tablestore any time, " +
                                "so we ignores this tuple domain, queryId=%s tupleDomain=%s",
                        ((TablestoreColumnHandle) pkIsNull.get()).getColumnName(), queryId, tupleDomainStr);
                return emptyList();
            }

            Map<String, Set<String>> matchedSearchIndexes = findMatchedSearchIndex(session, layoutHandle);

            List<TablestoreSplit> x;
            long t1 = currentTimeMillis();
            if (matchedSearchIndexes.size() > 0) {
                boolean isParallelScanMode = isParallelScanMode(session);
                log.info("query %s is using ParallelScanMode: %s, matched search index: %s", queryId, isParallelScanMode, matchedSearchIndexes);

                if (isParallelScanMode) {
                    x = computeSplitsFromSearchIndex(session, layoutHandle, constraint, tupleDomainStr, matchedSearchIndexes);
                }
                else {
                    x = querySplitsFromSearchIndex(session, th, constraint, tupleDomainStr, matchedSearchIndexes);
                    t1 = currentTimeMillis() - t1;
                    log.info("getSplits() end, found %s indexes, query=%s splitsSize=%s apiCost=%sms.",
                            matchedSearchIndexes.size(), queryId, x.size(), t1);
                }
            }
            else {
                x = querySplitsFromMainTable(session, th, constraint);
                t1 = currentTimeMillis() - t1;
                log.info("getSplits() end, using main table, query=%s splitsSize=%s apiCost=%sms.", queryId, x.size(), t1);
            }
            return x;
        }
        catch (Exception e) {
            log.error("getSplits() failed, queryId=%s ramUserId=%s table=%s", queryId, ramUserId, stn, e);
            throw e;
        }
    }

    public Map<String, Set<String>> findMatchedSearchIndex(ConnectorSession session, TablestoreTableLayoutHandle ol)
    {
        String queryId = session.getQueryId();
        TablestoreTableHandle table = ol.getTable();
        SchemaTableName stn = table.getSchemaTableName();
        String stnStr = table.getPrintableStn();

        log.info("queryId=%s Starting to findMatchedSearchIndex()", queryId);
        TupleDomain<ColumnHandle> constraint = ol.getTupleDomain().orElseThrow(NullPointerException::new);

        IndexSelectionStrategy iF = extractIndexFirst(session);
        String backStr = iF.backToString();
        if (!iF.isContained(stn) && !isParallelScanMode(session)) {
            log.info("Don't use index first[%s] for table[%s] of query:%s", backStr, stnStr, queryId);
            return emptyMap();
        }

        List<TupleDomain.ColumnDomain<ColumnHandle>> columnDomains = constraint.getColumnDomains()
                .orElseThrow(NullPointerException::new);

        Map<String, Set<String>> matchedIndexes = listSearchIndex(session, table.getTableName(), columnDomains, constraint, iF);
        return matchedIndexes;
    }

    private Map<String, Set<String>> listSearchIndex(
            ConnectorSession session,
            String tableName,
            List<TupleDomain.ColumnDomain<ColumnHandle>> columns,
            TupleDomain<ColumnHandle> domain,
            IndexSelectionStrategy indexSelectionStrategy)
    {
        log.info("Searching indexes for table[%s] with column handles [%s], tuple domain [%s] and indexFirstSet [%s]",
                tableName, columns, domain, indexSelectionStrategy);
        ListSearchIndexRequest request = new ListSearchIndexRequest();
        request.setTableName(tableName);

        Triple<ListSearchIndexResponse, Long, Long> response = executeRemote(
                c -> c.listSearchIndex(request),
                "listSearchIndex",
                tableName);
        List<SearchIndexInfo> indexInfos = response.getLeft().getIndexInfos();
        log.info("Found %s indexes for table [%s]", indexInfos.size(), tableName);

        Map<String, Set<String>> result = new HashMap<>();
        for (SearchIndexInfo info : indexInfos) {
            String indexName = info.getIndexName();

            DescribeSearchIndexRequest dsRequest = new DescribeSearchIndexRequest();
            dsRequest.setTableName(tableName);
            dsRequest.setIndexName(indexName);

            Triple<DescribeSearchIndexResponse, Long, Long> dsResponse = executeRemote(
                    c -> c.describeSearchIndex(dsRequest),
                    "describeSearchIndex",
                    info);
            IndexSchema schema = dsResponse.getLeft().getSchema();

            Set<String> usefulFields = filterUsefulFields(schema.getFieldSchemas());

            if (ifAllTupleDomainColumnsInIndex(columns, usefulFields)) {
                if (isParallelScanMode(session) || indexSelectionStrategy.getType() != THRESHOLD) {
                    result.put(info.getIndexName(), usefulFields);
                    log.info("The index [%s] of table [%s] is appropriate for the query", indexName, tableName);
                }
                else if (meetHeuristicRequirement(session, indexSelectionStrategy, domain, info)) {
                    result.put(info.getIndexName(), usefulFields);
                    log.info("The index [%s] of table [%s] is appropriate for the query and heuristic type", indexName, tableName);
                }
            }
        }
        return result;
    }

    public boolean meetHeuristicRequirement(ConnectorSession session,
            IndexSelectionStrategy indexSelectionStrategy,
            TupleDomain<ColumnHandle> tupleDomain,
            SearchIndexInfo sii)
    {
        String queryId = session.getQueryId();
        String index = sii.getIndexName();

        SearchRequest sr = buildSearchRequestForMatchedCount(tupleDomain, sii.getTableName(), sii.getIndexName());
        log.info("Starting first search request of index[%s] for query[%s], SQL=%s", index, queryId, printSearchRequest(sr));
        Triple<RowIterator, Long, Long> x = executeRemote(
                c -> c.createSearchIterator(sr), "createSearchIterator");

        checkState(!x.getLeft().hasNext(), "This api should't return any rows");
        long rows = x.getLeft().getTotalCount();
        String using = "so we use the index[%s] for current query[%s]";
        String notUsing = "so we DO NOT use the index[%s] for current query[%s]";
        if (indexSelectionStrategy.isMaxRowsMode()) {
            int mr = indexSelectionStrategy.getMaxRows();
            if (rows <= mr) {
                log.info("The matched rows[%s] is <= specified max rows[%s], " + using, rows, mr, index, queryId);
                return true;
            }
            else {
                log.info("The matched rows[%s] is > specified max rows[%s], " + notUsing, rows, mr, index, queryId);
                return false;
            }
        }

        SearchRequest sr1 = buildSearchRequestForTotalCount(sii);
        log.info("Starting second search request of index[%s] for query[%s], SQL=%s", index, queryId, printSearchRequest(sr1));
        x = executeRemote(c1 -> c1.createSearchIterator(sr1), "createSearchIterator");
        checkState(!x.getLeft().hasNext(), "This api should't return any rows");
        long rows1 = x.getLeft().getTotalCount();
        if (rows1 == 0) {
            log.info("There are %s total rows of index[%s] for current query[%s]", rows1, index, queryId);
            return true;
        }

        int mp = indexSelectionStrategy.getMaxPercent();
        double proportion = (rows + 0.0) / rows1;
        if (proportion <= mp / 100.0) {
            log.info("The proportion[%s] of matched rows[%s] / total rows[%s] is <= percentage[%s%] with index [%s] and query [%s]",
                    proportion, rows, rows1, mp, index, queryId);
            return true;
        }
        else {
            log.info("The proportion[%s] of matched rows[%s] / total rows[%s] is > percentage[%s%] with index [%s] and query [%s]",
                    proportion, rows, rows1, mp, index, queryId);
            return false;
        }
    }

    public static boolean ifAllTupleDomainColumnsInIndex(List<TupleDomain.ColumnDomain<ColumnHandle>> columnDomains,
            Set<String> usefulFields)
    {
        return columnDomains.stream().map(TupleDomain.ColumnDomain::getColumn)
                .map(TablestoreColumnHandle.class::cast)
                .map(TablestoreColumnHandle::getColumnName)
                .allMatch(usefulFields::contains);
    }

    protected static Set<String> filterUsefulFields(List<FieldSchema> list)
    {
        return list.stream()
                .filter(f -> isFieldTypeAccepted(f.getFieldType()))
                .map(FieldSchema::getFieldName)
                .collect(Collectors.toSet());
    }

    private static boolean isFieldTypeAccepted(FieldType fieldType)
    {
        return fieldType == FieldType.KEYWORD || fieldType == FieldType.LONG
                || fieldType == FieldType.DOUBLE || fieldType == FieldType.BOOLEAN;
    }

    public List<TablestoreSplit> querySplitsFromSearchIndex(ConnectorSession session,
            TablestoreTableHandle tableHandle,
            TupleDomain<ColumnHandle> constraint,
            String tupleDomainStr,
            Map<String, Set<String>> indexCandidates)
    {
        String queryId = session.getQueryId();
        String stn = tableHandle.getPrintableStn();
        TablestoreSplit split = new TablestoreSplit(tableHandle, constraint, tupleDomainStr, indexCandidates);
        log.info("queryId=%s Created a new search index split for mappedTable[%s] and table[%s], indexes=%s",
                queryId, tableHandle.getTableName(), stn, indexCandidates);
        return ImmutableList.of(split);
    }

    public RecordCursor createRecordCursor(ConnectorSession session, TablestoreSplit split, List<TablestoreColumnHandle> columnHandles)
    {
        if (split.getIndexCandidates().size() > 0) {
            log.info("Creating a new search index record cursor, queryId=%s", session.getQueryId());
            return new TablestoreRecordCursor4SearchIndex(this, session, split, columnHandles);
        }
        int v = getQueryVersion(session);
        if (v == 2) {
            return new TablestoreRecordCursor4RangeBlock(this, session, split, columnHandles);
        }
        return new TablestoreRecordCursor(this, session, split, columnHandles);
    }

    public WrappedRowIterator rowIterator(ConnectorSession session, TablestoreSplit split, List<TablestoreColumnHandle> handles)
    {
        String queryId = session.getQueryId();
        String td = split.getTupleDomainString();
        try {
            log.info("rowIterator() start, queryId=%s tupleDomain=%s", queryId, td);

            TablestoreTableHandle th = split.getTableHandle();
            RangeIteratorParameter p = transformToTableStoreQuery(session, split, handles);
            Triple<Iterator<Row>, Long, Long> x = executeRemote(
                    c -> c.createRangeIterator(p),
                    "rowIterator",
                    p.getTableName());
            int rows = -1;
            Iterator<Row> rowIterator = x.getLeft();
            checkArgument(rowIterator instanceof RowIterator);
            Field field = ReflectionUtils.findField(RowIterator.class, "result");
            if (field != null) {
                ReflectionUtils.makeAccessible(field);
                GetRangeResponse grr = (GetRangeResponse) ReflectionUtils.getField(field, rowIterator);
                if (grr != null && grr.getRows() != null) {
                    rows = grr.getRows().size();
                }
            }
            log.info(
                    "rowIterator() end, queryId=%s firstFetchRows=%s clientCost=%sms apiCost=%sms",
                    queryId,
                    rows,
                    x.getMiddle() / NANO_SECS_IN_ONE_MS,
                    x.getRight() / NANO_SECS_IN_ONE_MS);
            return new WrappedRowIterator(x.getLeft(), x.getMiddle(), x.getRight());
        }
        catch (Exception e) {
            log.error("rowIterator() failed, queryId=%s tupleDomain=%s", queryId, td, e);
            throw e;
        }
    }

    public Triple<SimpleRowMatrixBlockRowIterator, Long, Long> rowIterator4RangeBlock(
            ConnectorSession session, TablestoreSplit split, List<TablestoreColumnHandle> handles)
    {
        String queryId = session.getQueryId();
        String tupleDomain = split.getTupleDomainString();
        log.info("rowIterator4RangeBlock() start, queryId=%s tupleDomain=%s", queryId, tupleDomain);

        try {
            TablestoreTableHandle tableHandle = split.getTableHandle();
            BulkExportQueryCriteria criteria = transformToBulkExportQuery(session, split, handles);
            long t1 = nanoTime();
            long t2 = nanoTime() - t1;
            t1 = nanoTime();
            SimpleRowMatrixBlockRowIterator simpleRowMatrixBlockRowIterator =
                    new SimpleRowMatrixBlockRowIterator(syncClient, criteria, false);
            long t3 = nanoTime() - t1;
            return Triple.of(simpleRowMatrixBlockRowIterator, t2, t3);
        }
        catch (Exception e) {
            log.error("rowIterator4RangeBlock() failed, queryId=%s tupleDomain=%s", queryId, tupleDomain, e);
            throw new RuntimeException(e);
        }
    }

    public WrappedRowIterator rowIterator4SearchIndex(ConnectorSession session,
            TablestoreSplit split,
            TablestoreColumnHandle[] columnHandles)
    {
        String queryId = session.getQueryId();
        String td = split.getTupleDomainString();
        log.info("rowIterator4SearchIndex() start, queryId=%s tupleDomain=%s", queryId, td);

        TablestoreTableHandle th = split.getTableHandle();
        if (isParallelScanMode(session)) {
            return parallelScanResult(queryId, split, columnHandles);
        }

        return searchResult(queryId, td, split, columnHandles);
    }

    private WrappedRowIterator searchResult(
            String queryId,
            String tupleDomainString,
            TablestoreSplit split,
            TablestoreColumnHandle[] columnHandles)
    {
        WrappedSearchRequest wrappedSearchRequest = findBestMatchedIndexAndBuildRequest(queryId, split, columnHandles);
        SearchRequest sr = wrappedSearchRequest.getSearchRequest();
        String expr = printSearchRequest(sr);
        log.info("Built SearchRequest for queryId=%s tupleDomain=%s expression=%s", queryId, tupleDomainString, expr);

        Triple<RowIterator, Long, Long> x = executeRemote(
                c -> c.createSearchIterator(sr),
                "createSearchIterator");
        SearchRowIterator searchRowIterator = (SearchRowIterator) x.getLeft();
        Iterator<Row> rowIterator;
        // optimize for count(*)
        if (wrappedSearchRequest.isOnlyFetchRowCount()) {
            rowIterator = new CountStarRowIterator(searchRowIterator);
        }
        else {
            rowIterator = searchRowIterator;
        }
        WrappedRowIterator wrappedRowIterator = new WrappedRowIterator(rowIterator, x.getMiddle(), x.getRight());

        int firstFetchRows = 0;
        Field field = ReflectionUtils.findField(SearchRowIterator.class, "result");
        if (field != null) {
            ReflectionUtils.makeAccessible(field);
            SearchResponse searchResponse = (SearchResponse) ReflectionUtils.getField(field, searchRowIterator);
            if (searchResponse != null && searchResponse.getRows() != null) {
                firstFetchRows = searchResponse.getRows().size();
            }
        }
        log.info(
                "rowIterator4SearchIndex() end, queryId=%s firstFetchRows=%s totalMatchedCount=%s clientCost=%sms apiCost=%sms",
                queryId,
                firstFetchRows,
                searchRowIterator.getTotalCount(),
                x.getMiddle() / NANO_SECS_IN_ONE_MS,
                x.getRight() / NANO_SECS_IN_ONE_MS);
        return wrappedRowIterator;
    }

    private WrappedRowIterator parallelScanResult(
            String queryId,
            TablestoreSplit split,
            TablestoreColumnHandle[] columnHandles)
    {
        log.info("parallelScanResult start, queryId %s", queryId);
        Map<String, Set<String>> candidateIndex = split.getIndexCandidates();
        if (candidateIndex.isEmpty()) {
            throw new IllegalArgumentException("Could not found suitable index in ParallelScan mode for query " + queryId);
        }

        // Columns to get
        List<String> columns = Arrays.stream(columnHandles)
                .map(TablestoreColumnHandle::getColumnName)
                .collect(toList());

        String indexName = null;
        for (Map.Entry<String, Set<String>> entry : candidateIndex.entrySet()) {
            if (entry.getValue().containsAll(columns)) {
                indexName = entry.getKey();
                break;
            }
        }
        if (indexName == null) {
            throw new IllegalArgumentException("Failed to find suitable index for query " + queryId + ". Please make sure all columns [" + columns +
                    "] are in a single index, current indexes: " + candidateIndex);
        }

        TablestoreTableHandle tableHandle = split.getTableHandle();
        TupleDomain<ColumnHandle> tupleDomain = split.getMergedTupleDomain();
        String tableName = tableHandle.getTableName();

        ParallelScanRequest parallelScanRequest = ParallelScanRequest.newBuilder()
                .tableName(tableName)
                .indexName(indexName)
                .scanQuery(ScanQuery.newBuilder()
                        .query(buildQueryForCurrentConditions(tupleDomain))
                        .limit(2000)
                        .currentParallelId(split.getCurrentParallel())
                        .maxParallel(split.getMaxParallel())
                        .build())
                .addColumnsToGet(columns)
                .sessionId(split.getSessionId())
                .build();
        log.info("Built ParallelScanRequest for query [%s] with tupleDomain [%s] and expression [%s]",
                queryId, tupleDomain.getDomains(), printParallelScanRequest(parallelScanRequest));

        Triple<RowIterator, Long, Long> response = executeRemote(
                c -> c.createParallelScanIterator(parallelScanRequest),
                "parallelScan");
        RowIterator rowIterator = response.getLeft();
        log.info("parallelScanResult end, queryId %s clientCost=%s apiCost=%s",
                queryId, response.getMiddle() / NANO_SECS_IN_ONE_MS, response.getRight() / NANO_SECS_IN_ONE_MS);
        return new WrappedRowIterator(rowIterator, response.getMiddle(), response.getRight());
    }

    /**
     * The best index is the Covering Index which contains all the columns we need.
     * <p>
     * We support the following three scenarios:
     * <ul>
     * <li>The requested columns is empty, return any index</li>
     * <li>The requested columns cann't be covered by any index, randomly return one index</li>
     * <li>The request columns are covered by one index, return the index</li>
     * </ul>
     *
     * @return key: whether we only fetch the row count；value: the corresponding SearchRequest
     */
    protected static WrappedSearchRequest findBestMatchedIndexAndBuildRequest(String queryId, TablestoreSplit split,
            TablestoreColumnHandle[] columnHandles)
    {
        Map<String, Set<String>> indexCandidates = split.getIndexCandidates();
        TablestoreTableHandle th = split.getTableHandle();
        TupleDomain<ColumnHandle> td = split.getMergedTupleDomain();
        String anyIndex = indexCandidates.keySet().iterator().next();

        // select count(*)
        if (columnHandles.length == 0) {
            return new WrappedSearchRequest(true, buildSearchRequestForMatchedCount(td, th.getTableName(), anyIndex));
        }

        // filter out the column names we want to be covered
        List<String> cs = Arrays.stream(columnHandles)
                .filter(c -> !c.isPrimaryKey())// pk is always in index, so exclude it from matching
                .map(TablestoreColumnHandle::getColumnName)
                .collect(toList());

        // looking for the covering index
        Optional<String> coveringIndex = indexCandidates.entrySet().stream()
                .filter(e -> e.getValue().containsAll(cs))
                .findFirst().map(Entry::getKey);

        coveringIndex.ifPresent(s -> log.info("Find covering index[%s] for query, queryId=%s", s, queryId));

        // If no covering index is found, take a random index
        String index = coveringIndex.orElse(anyIndex);

        return new WrappedSearchRequest(false, buildSearchRequestForData(td, th.getTableName(), index, columnHandles));
    }

    public static SearchRequest buildSearchRequestForData(TupleDomain<ColumnHandle> mergedTupleDomain,
            String tableName, String index,
            TablestoreColumnHandle[] columnHandles)
    {
        SearchQuery sq = new SearchQuery();

        Query query = buildQueryForCurrentConditions(mergedTupleDomain);
        sq.setQuery(query);

        ColumnsToGet ctg = new ColumnsToGet();
        List<String> columns = Arrays.stream(columnHandles)
                .map(TablestoreColumnHandle::getColumnName)
                .collect(toList());
        ctg.setColumns(columns);

        SearchRequest sr = new SearchRequest(tableName, index, sq);
        sr.setColumnsToGet(ctg);
        return sr;
    }

    /**
     * Convert TupleDomain into TableStore Query
     */
    public static Query buildQueryForCurrentConditions(TupleDomain<ColumnHandle> conditions)
    {
        checkState(!conditions.isNone(), "Unsupported state to build: isNone() == true");

        List<Query> andList = conditions.getColumnDomains()
                .orElseThrow(IllegalArgumentException::new)
                .stream()
                .sorted((c1, c2) -> {
                    TablestoreColumnHandle oc1 = (TablestoreColumnHandle) c1.getColumn();
                    TablestoreColumnHandle oc2 = (TablestoreColumnHandle) c2.getColumn();
                    return oc1.getColumnName().compareTo(oc2.getColumnName());
                })
                .map(TablestoreFacade::buildQueryForSingleColumn)
                .collect(toList());

        if (andList.size() > 1) {
            BoolQuery bq = new BoolQuery();
            bq.setMustQueries(andList);
            bq.setFilterQueries(andList);
            return bq;
        }
        else if (andList.size() == 1) {
            return andList.get(0);
        }
        else {
            return new MatchAllQuery();
        }
    }

    public static SearchRequest buildSearchRequestForMatchedCount(TupleDomain<ColumnHandle> tupleDomain, String mappedTable, String index)
    {
        Query condition = buildQueryForCurrentConditions(tupleDomain);
        return buildSearchRequestJustForCount(condition, mappedTable, index);
    }

    private static SearchRequest buildSearchRequestJustForCount(Query condition, String mappedTable, String index)
    {
        SearchQuery sq = new SearchQuery();

        // Only get records count, not the column data
        sq.setGetTotalCount(true);
        sq.setLimit(0);

        sq.setQuery(condition);

        ColumnsToGet ctg = new ColumnsToGet();
        ctg.setReturnAll(false);
        ctg.setColumns(emptyList());

        SearchRequest sr = new SearchRequest(mappedTable, index, sq);
        sr.setColumnsToGet(ctg);
        return sr;
    }

    public static SearchRequest buildSearchRequestForTotalCount(SearchIndexInfo sii)
    {
        Query condition = new MatchAllQuery();
        return buildSearchRequestJustForCount(condition, sii.getTableName(), sii.getIndexName());
    }

    public static String printSearchRequest(SearchRequest sr)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        ColumnsToGet ctg = sr.getColumnsToGet();
        if (ctg.isReturnAll()) {
            sb.append("*");
        }
        else if (ctg.getColumns().size() == 0) {
            sb.append("count(*)");
        }
        else {
            AtomicBoolean first = new AtomicBoolean(true);
            ctg.getColumns().forEach(c -> {
                if (!first.get()) {
                    sb.append(", ");
                }
                sb.append(c);
                first.set(false);
            });
        }
        sb.append(" FROM ").append(sr.getTableName()).append(":").append(sr.getIndexName());
        sb.append(" WHERE ");
        SearchQuery sq = sr.getSearchQuery();
        printSearchQuery(sq.getQuery(), sb);
        if (sq.getLimit() != null) {
            sb.append(" LIMIT ").append(sq.getLimit());
        }
        return sb.toString();
    }

    public static String printParallelScanRequest(ParallelScanRequest request)
    {
        StringBuilder sql = new StringBuilder("SELECT ");
        ColumnsToGet columns = request.getColumnsToGet();
        if (columns.isReturnAll()) {
            sql.append("*");
        }
        else if (columns.getColumns().size() == 0) {
            sql.append("count(*)");
        }
        else {
            AtomicBoolean first = new AtomicBoolean(true);
            columns.getColumns().forEach(c -> {
                if (!first.get()) {
                    sql.append(", ");
                }
                sql.append(c);
                first.set(false);
            });
        }

        sql.append(" FROM ")
                .append(request.getTableName()).append(":")
                .append(request.getIndexName())
                .append(" WHERE ");
        ScanQuery scanQuery = request.getScanQuery();
        printSearchQuery(scanQuery.getQuery(), sql);
        if (scanQuery.getLimit() != null) {
            sql.append(" LIMIT ").append(scanQuery.getLimit());
        }
        return sql.toString();
    }

    public static String printSearchQuery(Query query)
    {
        StringBuilder sb = new StringBuilder();
        printSearchQuery(query, sb);
        return sb.toString();
    }

    public static void printSearchQuery(Query query, StringBuilder sb)
    {
        if (query instanceof MatchAllQuery) {
            sb.append("true");
        }
        else if (query instanceof ExistsQuery) {
            ExistsQuery eq = (ExistsQuery) query;
            sb.append(eq.getFieldName()).append(" is not null");
        }
        else if (query instanceof TermQuery) {
            TermQuery tq = (TermQuery) query;
            sb.append(tq.getFieldName()).append(" = ").append(tq.getTerm().toString());
        }
        else if (query instanceof TermsQuery) {
            TermsQuery tq = (TermsQuery) query;
            sb.append(tq.getFieldName()).append(" in (");
            sb.append(tq.getTerms().stream().map(ColumnValue::toString).reduce((a, b) -> a + ", " + b).get());
            sb.append(')');
        }
        else if (query instanceof RangeQuery) {
            RangeQuery rq = (RangeQuery) query;
            if (rq.getFrom() != null) {
                sb.append(rq.getFieldName());
                sb.append(" >");
                if (rq.isIncludeLower()) {
                    sb.append("=");
                }
                sb.append(' ').append(rq.getFrom().toString());
            }

            if (rq.getTo() != null) {
                if (rq.getFrom() != null) {
                    sb.append(" AND ");
                }
                sb.append(rq.getFieldName());
                sb.append(" <");
                if (rq.isIncludeUpper()) {
                    sb.append("=");
                }
                sb.append(' ').append(rq.getTo().toString());
            }
        }
        else if (query instanceof BoolQuery) {
            BoolQuery bq = (BoolQuery) query;
            List<Query> mqs = bq.getMustQueries();
            if (mqs != null) { //AND
                if (mqs.size() == 1) {
                    printSearchQuery(mqs.get(0), sb);
                }
                else {
                    boolean x = false;
                    for (Query mq : mqs) {
                        if (x) {
                            sb.append(" AND ");
                        }
                        printSearchQuery(mq, sb);
                        x = true;
                    }
                }
            }
            else if (bq.getMustNotQueries() != null) { //NOT
                List<Query> nqs = bq.getMustNotQueries();
                sb.append("NOT (");
                printSearchQuery(nqs.get(0), sb);
                sb.append(')');
            }
            else { // OR
                List<Query> nqs = bq.getShouldQueries();
                if (nqs.size() == 1) {
                    printSearchQuery(nqs.get(0), sb);
                }
                else {
                    boolean x = false;
                    for (Query mq : nqs) {
                        if (x) {
                            sb.append(" OR ");
                        }
                        printSearchQuery(mq, sb);
                        x = true;
                    }
                }
            }
        }
        else {
            throw new UnsupportedOperationException("Query type is not supported:" + query.getClass());
        }
    }

    protected static Query buildQueryForSingleColumn(TupleDomain.ColumnDomain<ColumnHandle> cd)
    {
        TablestoreColumnHandle ch = (TablestoreColumnHandle) cd.getColumn();
        String columnName = ch.getColumnName();
        Domain d = cd.getDomain();

        if (d.isNone()) {
            throw new IllegalStateException("Impossible state(isNone=true): " + ch.getColumnName());
        }

        // xx is not null
        if (!d.isNullAllowed() && d.getValues().isAll()) {
            return buildForIsNotNull(columnName);
        }

        List<Query> orList = d.getValues().getRanges()
                .getOrderedRanges().stream()
                .map(r -> transformRangeToSearchQuery(columnName, r))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        // xx is null or ...
        if (d.isNullAllowed()) {
            orList.add(buildForIsNull(columnName));
        }

        // merge term query to In clause: a in (1,2,3)
        List<ColumnValue> cvs = orList.stream().filter(TermQuery.class::isInstance)
                .map(TermQuery.class::cast)
                .map(TermQuery::getTerm)
                .collect(toList());
        if (cvs.size() > 1) {
            TermsQuery tq = new TermsQuery();
            tq.setFieldName(columnName);
            tq.setTerms(cvs);

            orList = orList.stream().filter(t -> !(t instanceof TermQuery)).collect(toList());
            orList.add(tq);
        }

        // performance optimize: unwrap if only one element
        if (orList.size() == 1) {
            return orList.get(0);
        }

        BoolQuery bq = new BoolQuery();
        bq.setShouldQueries(orList);
        bq.setMinimumShouldMatch(1);
        return bq;
    }

    private static Query buildForIsNull(String columnName)
    {
        BoolQuery bq1 = new BoolQuery();
        ExistsQuery eq = new ExistsQuery();
        eq.setFieldName(columnName);
        bq1.setMustNotQueries(Lists.newArrayList(eq));
        return bq1;
    }

    private static Query buildForIsNotNull(String columnName)
    {
        ExistsQuery eq = new ExistsQuery();
        eq.setFieldName(columnName);
        return eq;
    }

    /**
     * Transfer range to TableStore query.
     */
    public static Optional<Query> transformRangeToSearchQuery(String columnName, Range range)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();

        if (range.isAll()) {
            return Optional.empty();
        }
        else if (range.isSingleValue()) {
            ColumnValue x = TablestoreValueUtil.toColumnValue(low);
            TermQuery tq = new TermQuery();
            tq.setFieldName(columnName);
            tq.setTerm(x);
            return Optional.of(tq);
        }
        else {
            RangeQuery rq = new RangeQuery();
            rq.setFieldName(columnName);
            if (!low.isLowerUnbounded()) {
                ColumnValue cv = TablestoreValueUtil.toColumnValue(low);
                rq.setFrom(cv);
                if (low.getBound() == Marker.Bound.ABOVE) {
                    rq.setIncludeLower(false);
                }
                else if (low.getBound() == EXACTLY) {
                    rq.setIncludeLower(true);
                }
                else {
                    throw new IllegalArgumentException("low Marker should never use BELOW bound: " + range);
                }
            }
            if (!high.isUpperUnbounded()) {
                ColumnValue cv = TablestoreValueUtil.toColumnValue(high);
                rq.setTo(cv);
                if (high.getBound() == EXACTLY) {
                    rq.setIncludeUpper(true);
                }
                else if (high.getBound() == BELOW) {
                    rq.setIncludeUpper(false);
                }
                else {
                    throw new IllegalArgumentException("high Marker should never use ABOVE bound: " + range);
                }
            }
            return Optional.of(rq);
        }
    }

    /**
     * Computes splits from matched indexes
     *
     * @param session Connector session
     * @param indexes matched indexes
     * @return TablestoreSplit list
     */
    public List<TablestoreSplit> computeSplitsFromSearchIndex(
            ConnectorSession session,
            TablestoreTableLayoutHandle tableLayoutHandle,
            TupleDomain<ColumnHandle> constraint,
            String tupleDomainStr,
            Map<String, Set<String>> indexes)
    {
        final TablestoreTableHandle table = tableLayoutHandle.getTable();
        final String tableName = table.getTableName();

        TupleDomain<ColumnHandle> tupleDomain = tableLayoutHandle.getTupleDomain().orElseThrow(NullPointerException::new);
        List<TupleDomain.ColumnDomain<ColumnHandle>> columnDomains = tupleDomain.getColumnDomains().orElseThrow(NullPointerException::new);

        List<String> columns = columnDomains.stream()
                .map(TupleDomain.ColumnDomain::getColumn)
                .map(TablestoreColumnHandle.class::cast)
                .map(TablestoreColumnHandle::getColumnName)
                .collect(toList());

        Optional<String> coveredIndex = indexes.entrySet().stream()
                .filter(e -> e.getValue().containsAll(columns))
                .findFirst()
                .map(Entry::getKey);
        if (coveredIndex.isPresent()) {
            log.info("Found index [%s] for query [%s]", coveredIndex, session.getQueryId());
        }
        else {
            throw new IllegalArgumentException("Could not find right index for query" + session.getQueryId()
                    + " with columns: " + columns + ", current indexes: " + indexes);
        }

        final String indexName = coveredIndex.get();
        Map<String, Set<String>> matchedIndex = new HashMap<String, Set<String>>()
        {
            {
                put(indexName, indexes.get(indexName));
            }
        };

        ComputeSplitsRequest request = new ComputeSplitsRequest();
        request.setTableName(tableName);
        request.setSplitsOptions(new SearchIndexSplitsOptions(coveredIndex.get()));
        Triple<ComputeSplitsResponse, Long, Long> response = executeRemote(
                c -> c.computeSplits(request),
                "computeSplits",
                tableName);
        Integer maxParallel = response.getLeft().getSplitsSize();
        byte[] sessionId = response.getLeft().getSessionId();
        log.info("computeSplits() end, request = %s, maxParallel = %s", request.toString(), maxParallel);

        List<TablestoreSplit> splits = new ArrayList<>(maxParallel);
        for (int i = 0; i < maxParallel; i++) {
            splits.add(new TablestoreSplit(table, constraint, tupleDomainStr, matchedIndex, maxParallel, i, sessionId));
        }
        return splits;
    }

    public List<TablestoreSplit> querySplitsFromMainTable(ConnectorSession session,
            TablestoreTableHandle th, TupleDomain<ColumnHandle> constraint)
    {
        String originalTable = th.getTableName();
        String queryId = session.getQueryId();

        long splitUnitBytes = getSplitUnitBytes(session);
        ComputeSplitsBySizeRequest req = new ComputeSplitsBySizeRequest(originalTable, 1, splitUnitBytes);
        Triple<ComputeSplitsBySizeResponse, Long, Long> r1 = executeRemote(
                c -> c.computeSplitsBySize(req),
                "computeSplitsBySize",
                originalTable);
        ComputeSplitsBySizeResponse r = r1.getLeft();
        List<Split> splits = r.getSplits();

        log.info(
                "splits got: queryId=%s splitUnitBytes=%s originalTable=%s RequestId=%s TraceId=%s " +
                        "splitSize=%s clientCost=%sms apiCost=%sms",
                queryId,
                splitUnitBytes,
                originalTable,
                r.getRequestId(),
                r.getTraceId(),
                splits.size(),
                r1.getMiddle() / NANO_SECS_IN_ONE_MS,
                r1.getRight() / NANO_SECS_IN_ONE_MS);

        if (enableSplitOptimization(session)) {
            long t1 = currentTimeMillis();
            double percentage = getSplitSizeRatio(session);
            int expectedTotalSplitSize = (int) Math.ceil(splits.size() * percentage);
            splits = divideAndMergeAndShuffle(expectedTotalSplitSize, splits);
            t1 = currentTimeMillis() - t1;
            log.info("splits optimized: queryId=%s expectedSplitSizeRatio=%s expectedTotalSplitSize=%s newSplitSize=%s cost=%sms",
                    queryId, percentage, expectedTotalSplitSize, splits.size(), t1);
        }

        AtomicInteger idx = new AtomicInteger();
        List<TablestoreSplit> x = splits.stream()
                .map(s -> mergeAndTransform(session, config.getInstance(), th, s, constraint, idx.getAndIncrement()))
                .flatMap(Collection::stream)
                .collect(toList());
        log.info("queryId=%s splitIndex=%s too many splits, ignore others ......", queryId, idx.get());
        return x;
    }

    protected static List<Split> divideAndMergeAndShuffle(int expectedTotalSplitSize, List<Split> rawSplits)
    {
        if (rawSplits.size() > expectedTotalSplitSize) {
            Split[] array = rawSplits.toArray(new Split[] {});
            int length = array.length;
            int leaving = rawSplits.size();
            boolean found = true;
            while (found && leaving > expectedTotalSplitSize) {
                found = false;
                for (int i = 0; i < length - 1 && leaving > expectedTotalSplitSize; i++) {
                    if (array[i] == null) {
                        continue;
                    }
                    Split a = array[i];
                    for (int j = i + 1; j < length; j++) {
                        if (array[j] == null) {
                            continue;
                        }
                        Split b = array[j];
                        a.setUpperBound(b.getUpperBound());
                        array[j] = null;
                        i = j;
                        leaving--;
                        found = true;

                        break;
                    }
                }
            }

            rawSplits = Arrays.stream(array).filter(Objects::nonNull).collect(toList());
        }

        Map<String, Queue<Split>> map = new TreeMap<>();
        rawSplits.forEach(s -> map.computeIfAbsent(s.getLocation(), s1 -> new LinkedList<>()).add(s));

        List<Split> newList = new ArrayList<>(rawSplits.size());
        Collection<Queue<Split>> ss = map.values();
        while (!ss.isEmpty()) {
            Iterator<Queue<Split>> i = ss.iterator();
            while (i.hasNext()) {
                Queue<Split> x = i.next();
                Split e = x.poll();
                if (e != null) {
                    newList.add(e);
                }
                else {
                    i.remove();
                }
            }
        }
        checkArgument(newList.size() == rawSplits.size());
        return newList;
    }

    /**
     * Convert TableStore Split into Presto Split, and combine the tupledomain into the filters in TableStore Split.
     */
    protected static List<TablestoreSplit> mergeAndTransform(ConnectorSession session, String instance, TablestoreTableHandle th,
            Split split, TupleDomain<ColumnHandle> tupleDomain, int splitIndex)
    {
        String stn = th.getPrintableStn();
        String tableName = th.getTableName();
        String qid = session.getQueryId();
        List<TablestoreColumnHandle> orderedPrimaryKeys = th.getOrderedPrimaryKeyColumns();
        int s1 = orderedPrimaryKeys.size();
        int s2 = split.getLowerBound().getPrimaryKeyColumnsMap().size();
        checkArgument(s1 == s2, "The sizes of primary keys in the table " +
                "handle[table=%s,pkSize=%s] and split[pkSize=%s] are NOT the same.", stn, s1, s2);

        // Extract the constraints from Split
        Builder<ColumnHandle, Domain> nowConstraint = ImmutableMap.builder();

        boolean matchMostLeftPrefix = true;
        for (TablestoreColumnHandle och : orderedPrimaryKeys) {
            PrimaryKeyValue lowPk = getPrimaryKeyValue(och, split.getLowerBound(), "lowerBound", instance, stn, tableName);
            PrimaryKeyValue higPk = getPrimaryKeyValue(och, split.getUpperBound(), "upperBound", instance, stn, tableName);

            Type type = och.getColumnType();
            ValueSet vs;
            if (!matchMostLeftPrefix) {
                vs = ValueSet.all(type);
            }
            else if (isSingleValue(lowPk, higPk)) {
                Marker low = toSingleValueMarker(type, lowPk);
                vs = ValueSet.of(type, low);
            }
            else {
                Marker low = toRangeMarker(type, lowPk, false);
                Marker hig = toRangeMarker(type, higPk, true);
                vs = ValueSet.ofRanges(new Range(low, hig));
                matchMostLeftPrefix = false;
            }
            Domain domain = Domain.create(vs, false);
            nowConstraint.put(och, domain);
        }

        // Merge the tupledomains
        ImmutableMap<ColumnHandle, Domain> map = nowConstraint.build();
        TupleDomain<ColumnHandle> mergedConstraint = tupleDomain.intersect(TupleDomain.withColumnDomains(map));
        String mergedTupleDomainStr = mergedConstraint.toString(session.getSqlFunctionProperties());

        if (splitIndex < 10) {
            log.info("queryId=%s splitIndex=%s split merge done tablestoreRange=[<%s> , <%s>) --> tupleDomain=%s",
                    qid, splitIndex, split.getLowerBound(), split.getUpperBound(), mergedTupleDomainStr);
        }

        // Skip the entire split
        if (mergedConstraint.isNone()) {
            return emptyList();
        }

        if (enableSplitOptimization(session) || !enablePartitionPruning(session)) {
            return Lists.newArrayList(new TablestoreSplit(th, mergedConstraint, mergedTupleDomainStr, emptyMap()));
        }

        Map<ColumnHandle, Domain> ds = mergedConstraint.getDomains().get();
        // By default only the first primary key is the partition key
        // So we use the first primary key to do 'partition pruning'
        TablestoreColumnHandle partitionKey = orderedPrimaryKeys.get(0);
        Domain y = ds.get(partitionKey);
        Ranges ranges = y.getValues().getRanges();
        if (y.isNullAllowed()) {
            log.info("Nullable partition key isn't allowed in TableStore, we ignore the condition, queryId=%s", qid);
        }

        int size = ranges.getRangeCount();
        if (size == 1) {
            return Lists.newArrayList(new TablestoreSplit(th, mergedConstraint, mergedTupleDomainStr, emptyMap()));
        }
        if (size > MAX_RANGE_TO_POINT_SIZE) {
            log.info("Too many small ranges[count=%s], we don't separate the tuple domain, queryId=%s", size, qid);
            return Lists.newArrayList(new TablestoreSplit(th, mergedConstraint, mergedTupleDomainStr, emptyMap()));
        }

        Map<ColumnHandle, Domain> m = new HashMap<>(ds);
        return ranges.getOrderedRanges().stream().map(r -> {
            m.put(partitionKey, Domain.create(ValueSet.ofRanges(r), false));

            TupleDomain<ColumnHandle> td = TupleDomain.withColumnDomains(m);
            String tdStr = td.toString(session.getSqlFunctionProperties());
            return new TablestoreSplit(th, td, tdStr, emptyMap());
        }).collect(toList());
    }

    public static boolean isSingleValue(PrimaryKeyValue lowPk, PrimaryKeyValue higPk)
    {
        return lowPk != INF_MIN && lowPk != INF_MAX && higPk != INF_MIN && higPk != INF_MAX && lowPk.equals(higPk);
    }

    public static Marker toSingleValueMarker(Type type, PrimaryKeyValue pk)
    {
        switch (pk.getType()) {
            case STRING:
                return Marker.exactly(type, utf8Slice(pk.asString()));
            case INTEGER:
                return Marker.exactly(type, pk.asLong());
            case BINARY:
                return Marker.exactly(type, wrappedBuffer(pk.asBinary()));
            default:
                throw new UnsupportedOperationException("unsupported type: " + pk.getType());
        }
    }

    protected static Marker toRangeMarker(Type type, PrimaryKeyValue pk, boolean rightOpen)
    {
        if (pk.isInfMin()) {
            return Marker.lowerUnbounded(type);
        }
        if (pk.isInfMax()) {
            return Marker.upperUnbounded(type);
        }
        switch (pk.getType()) {
            case STRING:
                Slice v = utf8Slice(pk.asString());
                return rightOpen ? Marker.below(type, v) : Marker.exactly(type, v);
            case INTEGER:
                long v1 = pk.asLong();
                return rightOpen ? Marker.below(type, v1) : Marker.exactly(type, v1);
            case BINARY:
                Slice v2 = wrappedBuffer(pk.asBinary());
                return rightOpen ? Marker.below(type, v2) : Marker.exactly(type, v2);
            default:
                throw new UnsupportedOperationException("unsupported type: " + pk.getType());
        }
    }

    protected static BulkExportQueryCriteria transformToBulkExportQuery(ConnectorSession session, TablestoreSplit tablestoreSplit, List<TablestoreColumnHandle> columnHandles)
    {
        String queryId = session.getQueryId();
        TablestoreTableHandle tableHandle = tablestoreSplit.getTableHandle();
        List<TablestoreColumnHandle> orderedPrimaryKeys = tableHandle.getOrderedPrimaryKeyColumns();
        TupleDomain<ColumnHandle> mergedConstraint = tablestoreSplit.getMergedTupleDomain();
        BulkExportQueryCriteria criteria = new BulkExportQueryCriteria(tableHandle.getTableName());

        PrimaryKeyBuilder spk = createPrimaryKeyBuilder();
        PrimaryKeyBuilder epk = createPrimaryKeyBuilder();
        Map<ColumnHandle, Domain> newDomains = mergedConstraint.getDomains().orElseThrow(NullPointerException::new);

        List<Pair<TablestoreColumnHandle, Range>> rs = orderedPrimaryKeys.stream().map(c -> {
            Domain d = newDomains.get(c);
            Range r = findMinMax(d);
            return Pair.of(c, r);
        }).collect(toList());

        AtomicBoolean mathMostLeftPrefix = new AtomicBoolean(true);
        rs.stream().reduce((first, second) -> {
            TablestoreColumnHandle c = first.getLeft();
            Range r = first.getRight();
            if (mathMostLeftPrefix.get()) {
                if (r.isSingleValue()) {
                    transformRangeForPrimaryKeyJustSingleValue(c.getColumnName(), r, spk, epk);
                }
                else {
                    transformRangeForPrimaryKeyWithRightPlus(c.getColumnName(), r, spk, epk);
                    mathMostLeftPrefix.set(false);
                }
            }
            else {
                transformRangeForPrimaryKeyWithInfMin(c.getColumnName(), spk, epk);
            }
            return second;
        }).ifPresent(x -> {
            TablestoreColumnHandle c = x.getLeft();
            Range r = x.getRight();
            if (mathMostLeftPrefix.get()) {
                transformRangeForPrimaryKeyWithRightPlus(c.getColumnName(), r, spk, epk);
            }
            else {
                transformRangeForPrimaryKeyWithInfMin(c.getColumnName(), spk, epk);
            }
        });

        PrimaryKey p1 = spk.build();
        PrimaryKey p2 = epk.build();
        criteria.setInclusiveStartPrimaryKey(p1);
        criteria.setExclusiveEndPrimaryKey(p2);

        StringBuilder sb = new StringBuilder();
        ColumnValueFilter finalFilter;
        int fv = getFilterVersion(session);
        if (fv == 2) {
            finalFilter = transformToFilterV2(newDomains);
        }
        else {
            finalFilter = transformToFilter(newDomains);
        }

        if (finalFilter != null) {
            criteria.setFilter(finalFilter);
            printFilter(finalFilter, sb);
        }

        int version = getQueryVersion(session);
        if (version == 2) {
            for (TablestoreColumnHandle ch : columnHandles) {
                criteria.addColumnsToGet(ch.getColumnName());
            }

            if (columnHandles.size() == 0) {
                String leastCostPk = null;
                for (PrimaryKeyColumn c : p1.getPrimaryKeyColumns()) {
                    if (c.getValue().getType() == PrimaryKeyType.INTEGER) {
                        leastCostPk = c.getName();
                        break;
                    }
                }
                if (leastCostPk == null) {
                    leastCostPk = p1.getPrimaryKeyColumn(0).getName();
                }
                criteria.addColumnsToGet(leastCostPk);
            }
            criteria.setDataBlockType(DataBlockType.DBT_SIMPLE_ROW_MATRIX);
        }
        else {
            columnHandles.stream().filter(ch -> !ch.isPrimaryKey())
                    .forEach(ch -> criteria.addColumnsToGet(ch.getColumnName()));
        }

        log.info("Transform BulkExportQueryCriteria done, queryId=%s selectedColumns=%s, pkRange=[<%s>, <%s>), filters=<%s>",
                queryId, criteria.getColumnsToGet(), p1, p2, sb.toString());

        return criteria;
    }

    protected static RangeIteratorParameter transformToTableStoreQuery(ConnectorSession session,
            TablestoreSplit split, List<TablestoreColumnHandle> columnHandles)
    {
        String queryId = session.getQueryId();
        TablestoreTableHandle th = split.getTableHandle();
        List<TablestoreColumnHandle> orderedPrimaryKeys = th.getOrderedPrimaryKeyColumns();
        TupleDomain<ColumnHandle> mergedConstraint = split.getMergedTupleDomain();
        RangeIteratorParameter param = new RangeIteratorParameter(th.getTableName());

        PrimaryKeyBuilder spk = createPrimaryKeyBuilder();
        PrimaryKeyBuilder epk = createPrimaryKeyBuilder();
        Map<ColumnHandle, Domain> newDomains = mergedConstraint.getDomains().orElseThrow(NullPointerException::new);

        List<Pair<TablestoreColumnHandle, Range>> rs = orderedPrimaryKeys.stream().map(c -> {
            Domain d = newDomains.get(c);
            Range r = findMinMax(d);
            return Pair.of(c, r);
        }).collect(toList());

        AtomicBoolean matchMostLeftPrefix = new AtomicBoolean(true);
        rs.stream().reduce((first, second) -> {
            TablestoreColumnHandle c = first.getLeft();
            Range r = first.getRight();
            if (matchMostLeftPrefix.get()) {
                if (r.isSingleValue()) {
                    transformRangeForPrimaryKeyJustSingleValue(c.getColumnName(), r, spk, epk);
                }
                else {
                    transformRangeForPrimaryKeyWithRightPlus(c.getColumnName(), r, spk, epk);
                    matchMostLeftPrefix.set(false);
                }
            }
            else {
                transformRangeForPrimaryKeyWithInfMin(c.getColumnName(), spk, epk);
            }
            return second;
        }).ifPresent(x -> {
            TablestoreColumnHandle c = x.getLeft();
            Range r = x.getRight();
            if (matchMostLeftPrefix.get()) {
                transformRangeForPrimaryKeyWithRightPlus(c.getColumnName(), r, spk, epk);
            }
            else {
                transformRangeForPrimaryKeyWithInfMin(c.getColumnName(), spk, epk);
            }
        });

        PrimaryKey p1 = spk.build();
        PrimaryKey p2 = epk.build();
        param.setInclusiveStartPrimaryKey(p1);
        param.setExclusiveEndPrimaryKey(p2);

        StringBuilder sb = new StringBuilder();
        ColumnValueFilter finalFilter;
        int fv = getFilterVersion(session);
        if (fv == 2) {
            finalFilter = transformToFilterV2(newDomains);
        }
        else {
            finalFilter = transformToFilter(newDomains);
        }
        if (finalFilter != null) {
            param.setFilter(finalFilter);
            printFilter(finalFilter, sb);
        }

        int version = getQueryVersion(session);
        if (version == 2) {
            for (TablestoreColumnHandle ch : columnHandles) {
                param.addColumnsToGet(ch.getColumnName());
            }

            if (columnHandles.size() == 0) {
                String leastCostPk = null;
                for (PrimaryKeyColumn pkc : p1.getPrimaryKeyColumns()) {
                    if (pkc.getValue().getType() == PrimaryKeyType.INTEGER) {
                        leastCostPk = pkc.getName();
                        break;
                    }
                }
                if (leastCostPk == null) {
                    leastCostPk = p1.getPrimaryKeyColumn(0).getName();
                }
                param.addColumnsToGet(leastCostPk);
            }
        }
        else {
            columnHandles.stream().filter(ch -> !ch.isPrimaryKey())
                    .forEach(c -> param.addColumnsToGet(c.getColumnName()));
        }

        log.info("transform params done, queryId=%s selectedColumns=%s, pkRange=[<%s> , <%s>), filters=<%s>",
                queryId, param.getColumnsToGet(), p1, p2, sb.toString());

        param.setMaxVersions(1);
        param.setBufferSize(getFetchSize(session));

        getTimeRange(session).ifPresent(param::setTimeRange);

        return param;
    }

    /**
     * The second version of filter tree
     */
    @Nullable
    protected static ColumnValueFilter transformToFilterV2(Map<ColumnHandle, Domain> newDomains)
    {
        CompositeColumnValueFilter and = new CompositeColumnValueFilter(LogicOperator.AND);
        for (Entry<ColumnHandle, Domain> e : newDomains.entrySet()) {
            TablestoreColumnHandle och = (TablestoreColumnHandle) e.getKey();
            String columnName = och.getColumnName();
            Domain d = e.getValue();
            if (d.isAll()) { // ALL does not need filtering
                continue;
            }

            CompositeColumnValueFilter or = new CompositeColumnValueFilter(LogicOperator.OR);
            // primary key cann't be null, so we don't pushdown this filter
            if (!och.isPrimaryKey() && d.isNullAllowed()) {
                // sql: column_xxx is null
                or.addFilter(new SingleColumnValueRegexFilter(columnName, NOT_EXIST));
            }

            d.getValues().getRanges().getOrderedRanges().stream()
                    .map(r -> transformFilterForColumnV2(columnName, r))
                    .filter(Objects::nonNull)
                    .forEach(or::addFilter);

            // sql: column_xxx is not null
            if (!och.isPrimaryKey() && or.getSubFilters().size() == 0) {
                or.addFilter(new SingleColumnValueRegexFilter(columnName, EXIST));
            }
            and.addFilter(or);
        }

        return unwrap(and);
    }

    @Nullable
    protected static ColumnValueFilter transformToFilter(Map<ColumnHandle, Domain> newDomains)
    {
        CompositeColumnValueFilter and = new CompositeColumnValueFilter(LogicOperator.AND);
        for (Entry<ColumnHandle, Domain> e : newDomains.entrySet()) {
            TablestoreColumnHandle och = (TablestoreColumnHandle) e.getKey();
            String mappedName = och.getColumnName();
            Domain d = e.getValue();
            if (d.isAll() || d.isNullAllowed()) {
                continue;
            }

            CompositeColumnValueFilter or = new CompositeColumnValueFilter(LogicOperator.OR);

            d.getValues().getRanges().getOrderedRanges().stream()
                    .map(r -> transformFilterForColumn(mappedName, r))
                    .filter(Objects::nonNull)
                    .forEach(or::addFilter);

            and.addFilter(or);
        }

        return unwrap(and);
    }

    public static Range findMinMax(Domain d)
    {
        List<Range> x = d.getValues().getRanges().getOrderedRanges();
        Marker low = x.get(0).getLow();
        Marker high = x.get(x.size() - 1).getHigh();
        return new Range(low, high);
    }

    public static void transformRangeForPrimaryKeyJustSingleValue(String originalName, Range range, PrimaryKeyBuilder spk,
            PrimaryKeyBuilder epk)
    {
        spk.addPrimaryKeyColumn(originalName, TablestoreValueUtil.toPrimaryKeyValue(range.getLow()));
        epk.addPrimaryKeyColumn(originalName, TablestoreValueUtil.toPrimaryKeyValue(range.getHigh()));
    }

    public static void transformRangeForPrimaryKeyWithInfMin(String originalName, PrimaryKeyBuilder spk,
            PrimaryKeyBuilder epk)
    {
        spk.addPrimaryKeyColumn(originalName, INF_MIN);
        epk.addPrimaryKeyColumn(originalName, INF_MIN);
    }

    public static void transformRangeForPrimaryKeyWithRightPlus(String originalName, Range range,
            PrimaryKeyBuilder spk, PrimaryKeyBuilder epk)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();

        if (range.isAll()) {
            spk.addPrimaryKeyColumn(originalName, INF_MIN);
            epk.addPrimaryKeyColumn(originalName, INF_MAX);
            return;
        }
        if (!low.isLowerUnbounded()) {
            if (low.getBound() == Marker.Bound.ABOVE || low.getBound() == EXACTLY) {
                spk.addPrimaryKeyColumn(originalName, TablestoreValueUtil.toPrimaryKeyValue(low));
            }
            else {
                throw new IllegalArgumentException("low Marker should never use BELOW bound: " + range);
            }
        }
        else {
            spk.addPrimaryKeyColumn(originalName, PrimaryKeyValue.INF_MIN);
        }
        if (!high.isUpperUnbounded()) {
            if (high.getBound() == BELOW) {
                epk.addPrimaryKeyColumn(originalName, TablestoreValueUtil.toPrimaryKeyValue(high));
            }
            else if (high.getBound() == EXACTLY) {
                PrimaryKeyValue v = TablestoreValueUtil.toPrimaryKeyValue(high);
                PrimaryKeyValue v1 = TablestoreValueUtil.addOneForPrimaryKeyValue(v);
                epk.addPrimaryKeyColumn(originalName, v1);
            }
            else {
                throw new IllegalArgumentException("high Marker should never use ABOVE bound: " + range);
            }
        }
        else {
            epk.addPrimaryKeyColumn(originalName, PrimaryKeyValue.INF_MAX);
        }
    }

    public static void printFilter(ColumnValueFilter f, StringBuilder sb)
    {
        if (f == null) {
            return;
        }
        if (f instanceof SingleColumnValueRegexFilter) {
            SingleColumnValueRegexFilter sf = (SingleColumnValueRegexFilter) f;
            sb.append(sf.getColumnName()).append(' ');
            sb.append(mapColumnOperatorV2(sf.getOperator()));
            if (sf.getOperator() != EXIST && sf.getOperator() != NOT_EXIST) {
                sb.append(' ').append(sf.getColumnValue().toString());
            }
            return;
        }
        if (f instanceof SingleColumnValueFilter) {
            SingleColumnValueFilter sf = (SingleColumnValueFilter) f;
            sb.append(sf.getColumnName()).append(' ');
            sb.append(mapColumnOperator(sf.getOperator()));
            return;
        }
        CompositeColumnValueFilter ccf = (CompositeColumnValueFilter) f;

        if (ccf.getOperationType() == LogicOperator.NOT) {
            ColumnValueFilter c = ccf.getSubFilters().get(0);
            sb.append("NOT ");
            if (c instanceof CompositeColumnValueFilter) {
                sb.append('(');
            }
            printFilter(c, sb);
            if (c instanceof CompositeColumnValueFilter) {
                sb.append(")");
            }
            return;
        }

        List<ColumnValueFilter> sfs = ccf.getSubFilters();
        boolean first = true;
        for (ColumnValueFilter sf : sfs) {
            if (!first) {
                sb.append(' ').append(ccf.getOperationType()).append(' ');
            }
            first = false;
            if (sf instanceof CompositeColumnValueFilter) {
                sb.append('(');
            }
            printFilter(sf, sb);
            if (sf instanceof CompositeColumnValueFilter) {
                sb.append(')');
            }
        }
    }

    private static String mapColumnOperator(SingleColumnValueFilter.CompareOperator o)
    {
        switch (o) {
            case EQUAL:
                return "=";
            case LESS_THAN:
                return "<";
            case NOT_EQUAL:
                return "!=";
            case LESS_EQUAL:
                return "<=";
            case GREATER_THAN:
                return ">";
            case GREATER_EQUAL:
                return ">=";
            default:
                return "<unknown:" + o + ">";
        }
    }

    private static String mapColumnOperatorV2(SingleColumnValueRegexFilter.CompareOperator o)
    {
        switch (o) {
            case EQUAL:
                return "=";
            case LESS_THAN:
                return "<";
            case NOT_EQUAL:
                return "!=";
            case LESS_EQUAL:
                return "<=";
            case GREATER_THAN:
                return ">";
            case GREATER_EQUAL:
                return ">=";
            case EXIST:
                return "IS NOT NULL";
            case NOT_EXIST:
                return "IS NULL";
            default:
                return "<unknown:" + o + ">";
        }
    }

    @Nullable
    private static ColumnValueFilter unwrap(ColumnValueFilter f)
    {
        if (!(f instanceof CompositeColumnValueFilter)) {
            return f;
        }
        CompositeColumnValueFilter ff = (CompositeColumnValueFilter) f;

        List<ColumnValueFilter> news = ff.getSubFilters().stream()
                .map(TablestoreFacade::unwrap).filter(Objects::nonNull).collect(toList());

        if (news.size() == 0) {
            return null;
        }

        LogicOperator ot = ff.getOperationType();
        if (ot != LogicOperator.AND && ot != LogicOperator.OR || news.size() > 1) {
            ff.getSubFilters().clear();
            ff.getSubFilters().addAll(news);
            return ff;
        }
        return news.get(0); //ot == LogicOperator.NOT
    }

    private static PrimaryKeyValue getPrimaryKeyValue(TablestoreColumnHandle ch, PrimaryKey bound, String boundType,
            String instance, String stn, String mappedTable)
    {
        String columnName = ch.getColumnName();
        Set<String> pks = bound.getPrimaryKeyColumnsMap().keySet();
        PrimaryKeyColumn pkc = bound.getPrimaryKeyColumn(columnName);
        String str = "%s PrimaryKeyColumn of column %s in instance %s and table %s is null, tableName=%s, columnName=%s, tablestore-pks=%s";
        checkNotNull(pkc, str, boundType, columnName, instance, mappedTable, stn, columnName, pks);

        PrimaryKeyValue pk = pkc.getValue();
        str = "%s PrimaryKeyValue of column %s in instance %s and table %s is null, tableName=%s, columnName=%s, tablestore-pks=%s";
        checkNotNull(pk, str, boundType, columnName, instance, mappedTable, stn, columnName, pks);

        return pk;
    }

    @Nullable
    public static ColumnValueFilter transformFilterForColumn(String mappedName, Range range)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();

        if (range.isAll()) {
            return null;
        }
        else if (range.isSingleValue()) {
            ColumnValue x = TablestoreValueUtil.toColumnValue(low);
            return new SingleColumnValueFilter(mappedName, SingleColumnValueFilter.CompareOperator.EQUAL, x);
        }
        else {
            CompositeColumnValueFilter filter = new CompositeColumnValueFilter(LogicOperator.AND);
            if (!low.isLowerUnbounded()) {
                ColumnValue cv = TablestoreValueUtil.toColumnValue(low);
                if (low.getBound() == Marker.Bound.ABOVE) {
                    filter.addFilter(new SingleColumnValueFilter(mappedName, SingleColumnValueFilter.CompareOperator.GREATER_THAN, cv));
                }
                else if (low.getBound() == EXACTLY) {
                    filter.addFilter(new SingleColumnValueFilter(mappedName, SingleColumnValueFilter.CompareOperator.GREATER_EQUAL, cv));
                }
                else {
                    throw new IllegalArgumentException("low Marker should never use BELOW bound: " + range);
                }
            }
            if (!high.isUpperUnbounded()) {
                ColumnValue cv = TablestoreValueUtil.toColumnValue(high);
                if (high.getBound() == EXACTLY) {
                    filter.addFilter(new SingleColumnValueFilter(mappedName, SingleColumnValueFilter.CompareOperator.LESS_EQUAL, cv));
                }
                else if (high.getBound() == BELOW) {
                    filter.addFilter(new SingleColumnValueFilter(mappedName, SingleColumnValueFilter.CompareOperator.LESS_THAN, cv));
                }
                else {
                    throw new IllegalArgumentException("high Marker should never use ABOVE bound: " + range);
                }
            }
            return unwrap(filter);
        }
    }

    /**
     * Convert Presto range into TableStore filter.
     */
    @Nullable
    public static ColumnValueFilter transformFilterForColumnV2(String columnName, Range range)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();

        if (range.isAll()) {
            return null;
        }
        else if (range.isSingleValue()) {
            ColumnValue x = TablestoreValueUtil.toColumnValue(low);
            return new SingleColumnValueRegexFilter(columnName, EQUAL, x);
        }
        else {
            CompositeColumnValueFilter filter = new CompositeColumnValueFilter(LogicOperator.AND);
            if (!low.isLowerUnbounded()) {
                ColumnValue cv = TablestoreValueUtil.toColumnValue(low);
                if (low.getBound() == Marker.Bound.ABOVE) {
                    filter.addFilter(new SingleColumnValueRegexFilter(columnName, GREATER_THAN, cv));
                }
                else if (low.getBound() == EXACTLY) {
                    filter.addFilter(new SingleColumnValueRegexFilter(columnName, GREATER_EQUAL, cv));
                }
                else {
                    throw new IllegalArgumentException("low Marker should never use BELOW bound: " + range);
                }
            }
            if (!high.isUpperUnbounded()) {
                ColumnValue cv = TablestoreValueUtil.toColumnValue(high);
                if (high.getBound() == EXACTLY) {
                    filter.addFilter(new SingleColumnValueRegexFilter(columnName, LESS_EQUAL, cv));
                }
                else if (high.getBound() == BELOW) {
                    filter.addFilter(new SingleColumnValueRegexFilter(columnName, LESS_THAN, cv));
                }
                else {
                    throw new IllegalArgumentException("high Marker should never use ABOVE bound: " + range);
                }
            }
            return unwrap(filter);
        }
    }

    public boolean updateBatchRows(ConnectorSession session, SchemaTableName stn, BatchWriteRowRequest batchWrite)
    {
        String queryId = session.getQueryId();
        String ramUserId = session.getUser();
        long t1 = currentTimeMillis();
        BatchWriteRowResponse x;
        try {
            Triple<BatchWriteRowResponse, Long, Long> x1 = executeRemote(
                    c -> c.batchWriteRow(batchWrite),
                    "updateBatchRows",
                    stn.toString());
            x = x1.getLeft();
            if (x1.getRight() >= 500 * NANO_SECS_IN_ONE_MS) {
                log.warn(
                        "updateBatchRows() too slow, queryId=%s ramUserId=%s requestId=%s traceId=%s table=%s " +
                                "rowCount=%s clientCost=%sms apiCost=%sms",
                        queryId,
                        ramUserId,
                        x.getRequestId(),
                        x.getTraceId(),
                        stn,
                        batchWrite.getRowsCount(),
                        x1.getMiddle() / NANO_SECS_IN_ONE_MS,
                        x1.getRight() / NANO_SECS_IN_ONE_MS);
            }
        }
        catch (Exception e) {
            log.error("updateBatchRows() failed, queryId=%s ramUserId=%s table=%s rowCount=%s totalCost=%sms",
                    queryId, ramUserId, stn, batchWrite.getRowsCount(), t1, e);
            throw e;
        }
        if (x.isAllSucceed()) {
            return true;
        }
        List<RowResult> failed = x.getFailedRows();
        RowResult first = failed.get(0);
        Row rowData = first.getRow();
        Error error = first.getError();
        Object firstPk = rowData == null ? null : rowData.getPrimaryKey().toString();
        String str = "updateBatchRows() failed, queryId=%s ramUserId=%s requestId=%s traceId=%s table=%s " +
                "updateRowCount=%d apiCost=%dms failedRows=%d error={%s} pkForFirstFailedRows=%s";

        t1 = currentTimeMillis() - t1;
        String msg = String.format(str, queryId, ramUserId, x.getRequestId(), x.getTraceId(), stn,
                batchWrite.getRowsCount(), t1, failed.size(), error, firstPk);

        log.error(msg, error);
        throw new PrestoException(GENERIC_INTERNAL_ERROR, msg);
    }

    @Override
    public void close()
            throws Exception
    {
    }
}
