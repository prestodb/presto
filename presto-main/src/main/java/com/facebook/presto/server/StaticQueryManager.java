/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.hive.ImportClient;
import com.facebook.presto.hive.SchemaField;
import com.facebook.presto.importer.ImportField;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.ingest.DelimitedRecordIterable;
import com.facebook.presto.ingest.ImportSchemaUtil;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.RecordProjections;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.LegacyStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class StaticQueryManager
        implements QueryManager
{
    private final int pageBufferMax;
    private final ExecutorService executor;
    private final ImportClient importClient;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final LegacyStorageManager storageManager;

    @GuardedBy("this")
    private int nextQueryId;

    // todo this is a potential memory leak if the query objects are not removed
    @GuardedBy("this")
    private final Map<String, QueryState> queries = new HashMap<>();

    @Inject
    public StaticQueryManager(ImportClient importClient, ImportManager importManager, Metadata metadata, LegacyStorageManager storageManager)
    {
        this(20, importClient, importManager, metadata, storageManager);
    }

    public StaticQueryManager(int pageBufferMax, ImportClient importClient, ImportManager importManager, Metadata metadata, LegacyStorageManager storageManager)
    {
        Preconditions.checkArgument(pageBufferMax > 0, "blockBufferMax must be at least 1");
        this.pageBufferMax = pageBufferMax;
        executor = Executors.newFixedThreadPool(50, new ThreadFactoryBuilder().setNameFormat("http-query-processor-%d").build());
        this.importClient = importClient;
        this.importManager = importManager;
        this.metadata = metadata;
        this.storageManager = storageManager;
    }

    @Override
    public synchronized QueryInfo createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkArgument(query.length() > 0, "query must not be empty string");

        String queryId = String.valueOf(nextQueryId++);
        final QueryState queryState = new QueryState(1, pageBufferMax);

        // TODO: fix how we get our query parameters. typically we will have a query parser/planner to drive executions, but in the meantime, just use this ghetto-ness
        ImmutableList<String> strings = ImmutableList.copyOf(Splitter.on(":").split(query));
        String queryBase = strings.get(0);

        QueryTask queryTask;
        switch (queryBase) {
            case "sum":
                queryTask = new SumQuery(queryState, executor,
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),

                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),

                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),

                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"));
                break;
            case "sum-partial":
                queryTask = new PartialSumQuery(metadata, storageManager, queryState);
                break;

            // e.g.: import-delimited:default:hivedba_query_stats:string,long,string,long:/tmp/myfile.csv:,
            case "import-delimited":
                List<Type> types = ImmutableList.copyOf(Iterables.transform(Splitter.on(",").split(strings.get(2)), new Function<String, Type>()
                {
                    @Override
                    public Type apply(String input)
                    {
                        return Type.fromName(input);
                    }
                }));
                queryTask = new ImportDelimited(queryState,
                        storageManager,
                        strings.get(1),
                        strings.get(2),
                        new TupleInfo(types),
                        new File(strings.get(3)),
                        Splitter.on(strings.get(4)));
                break;

            // e.g.: import-table:hive:default:hivedba_query_stats
            case "import-table":
                queryTask = new ImportTableQuery(queryState, importClient, importManager, metadata, strings.get(1), strings.get(2), strings.get(3));
                break;

            default:
                throw new IllegalArgumentException("Unsupported query " + query);
        }

        queries.put(queryId, queryState);
        executor.submit(queryTask);

        return new QueryInfo(queryId, queryTask.getTupleInfos());
    }

    @Override
    public List<Page> getQueryResults(String queryId, int maxPageCount)
            throws InterruptedException
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkArgument(maxPageCount > 0, "maxPageCount must be at least 1");

        return getQuery(queryId).getNextPages(maxPageCount);
    }

    @Override
    public void destroyQuery(String queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        QueryState query;
        synchronized (this) {
            query = queries.remove(queryId);
        }
        if (query != null && !query.isDone()) {
            query.cancel();
        }
    }

    public synchronized QueryState getQuery(String queryId)
            throws NoSuchElementException
    {
        QueryState queryState = queries.get(queryId);
        if (queryState == null) {
            throw new NoSuchElementException();
        }
        return queryState;
    }

    private static interface QueryTask
            extends Runnable
    {
        List<TupleInfo> getTupleInfos();
    }

    private static class SumQuery
            implements QueryTask
    {
        private static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64));

        private final QueryState queryState;
        private final List<URI> servers;
        private final AsyncHttpClient asyncHttpClient;

        public SumQuery(QueryState queryState, ExecutorService executor, URI... servers)
        {
            this(queryState, executor, ImmutableList.copyOf(servers));
        }

        public SumQuery(QueryState queryState, ExecutorService executor, Iterable<URI> servers)
        {
            this.queryState = queryState;
            this.servers = ImmutableList.copyOf(servers);
            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(1, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(1, TimeUnit.MINUTES)));
            asyncHttpClient = new AsyncHttpClient(httpClient, executor);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
        }

        @Override
        public void run()
        {
            try {
                QueryDriversOperator operator = new QueryDriversOperator(10,
                        Iterables.transform(servers, new Function<URI, QueryDriverProvider>()
                        {
                            @Override
                            public QueryDriverProvider apply(URI uri)
                            {
                                return new HttpQueryProvider("sum-partial", asyncHttpClient, uri, ImmutableList.of(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64)));
                            }
                        })
                );

                HashAggregationOperator aggregation = new HashAggregationOperator(operator,
                        0,
                        ImmutableList.of(LongSumAggregation.provider(1, 0)),
                        ImmutableList.of(concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 2, 0))));

                for (Page page : aggregation) {
                    queryState.addPage(page);
                }
                queryState.sourceFinished();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }
    }

    private static class PartialSumQuery
            implements QueryTask
    {
        private static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64));

        private final Metadata metadata;
        private final LegacyStorageManager storageManager;
        private final QueryState queryState;

        public PartialSumQuery(Metadata metadata, LegacyStorageManager storageManager, QueryState queryState)
        {
            this.metadata = metadata;
            this.storageManager = storageManager;
            this.queryState = queryState;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
        }

        @Override
        public void run()
        {
            try {
                BlockIterable groupBySource = getColumn("hivedba_query_stats", "pool_name");
                BlockIterable aggregateSource = getColumn("hivedba_query_stats", "cpu_msec");

                AlignmentOperator alignmentOperator = new AlignmentOperator(groupBySource, aggregateSource);
                HashAggregationOperator sumOperator = new HashAggregationOperator(alignmentOperator,
                        0,
                        ImmutableList.of(LongSumAggregation.provider(0, 0)),
                        ImmutableList.of(concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0))));

                for (Page page : sumOperator) {
                    queryState.addPage(page);
                }
                queryState.sourceFinished();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }

        private BlockIterable getColumn(String tableName, String columnName)
        {
            TableMetadata tableMetadata = metadata.getTable("default", "default", tableName);
            int index = 0;
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                if (columnName.equals(columnMetadata.getName())) {
                    break;
                }
                ++index;
            }
            final int columnIndex = index;
            return storageManager.getBlocks("default", tableName, columnIndex);
        }
    }

    private static class ImportTableQuery
            implements QueryTask
    {
        private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);

        private final QueryState queryState;
        private final ImportClient importClient;
        private final ImportManager importManager;
        private final Metadata metadata;
        private final String sourceName;
        private final String databaseName;
        private final String tableName;

        private ImportTableQuery(
                QueryState queryState,
                ImportClient importClient,
                ImportManager importManager,
                Metadata metadata,
                String sourceName,
                String databaseName,
                String tableName)
        {
            this.queryState = queryState;
            this.importClient = importClient;
            this.importManager = importManager;
            this.metadata = metadata;
            this.sourceName = sourceName;
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
        }

        @Override
        public void run()
        {
            try {
                String catalogName = "default";
                String schemaName = "default";

                List<SchemaField> schema = importClient.getTableSchema(databaseName, tableName);
                List<ColumnMetadata> columns = ImportSchemaUtil.createColumnMetadata(schema);
                TableMetadata table = new TableMetadata(catalogName, schemaName, tableName, columns);
                metadata.createTable(table);

                table = metadata.getTable(catalogName, schemaName, tableName);
                long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
                List<ImportField> fields = getImportFields(table.getColumns());

                importManager.importTable(tableId, sourceName, databaseName, tableName, fields);

                queryState.addPage(new Page(new BlockBuilder(TupleInfo.SINGLE_LONG).append(0).build()));
                queryState.sourceFinished();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }

        private static List<ImportField> getImportFields(List<ColumnMetadata> columns)
        {
            ImmutableList.Builder<ImportField> fields = ImmutableList.builder();
            for (ColumnMetadata column : columns) {
                long columnId = ((NativeColumnHandle) column.getColumnHandle().get()).getColumnId();
                fields.add(new ImportField(columnId, column.getType(), column.getName()));
            }
            return fields.build();
        }
    }

    private static class ImportDelimited
            implements QueryTask
    {
        private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);

        private final QueryState queryState;
        private final LegacyStorageManager storageManager;
        private final String databaseName;
        private final String tableName;
        private final RecordProjectOperator source;

        public ImportDelimited(
                QueryState queryState,
                LegacyStorageManager storageManager,
                String databaseName,
                String tableName,
                TupleInfo tupleInfo,
                File delimitedFile,
                Splitter splitter)
        {
            this.queryState = queryState;
            this.storageManager = storageManager;
            this.databaseName = databaseName;
            this.tableName = tableName;

            ImmutableList.Builder<RecordProjection> builder = ImmutableList.builder();
            for (int i = 0; i < tupleInfo.getFieldCount(); i++) {
                builder.add(RecordProjections.createProjection(i, tupleInfo.getTypes().get(i)));
            }
            List<RecordProjection> recordProjections = builder.build();

            InputSupplier<InputStreamReader> readerSupplier = Files.newReaderSupplier(delimitedFile, Charsets.UTF_8);
            DelimitedRecordIterable records = new DelimitedRecordIterable(readerSupplier, splitter);
            source = new RecordProjectOperator(records, recordProjections);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
        }

        @Override
        public void run()
        {
            try {
                // TODO: fix this
//                long rowCount = storageManager.importShard(source, databaseName, tableName);
                long rowCount = 0;
                queryState.addPage(new Page(new BlockBuilder(SINGLE_LONG).append(rowCount).build()));
                queryState.sourceFinished();
                throw new UnsupportedOperationException();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }
    }
}
