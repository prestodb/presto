/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.importer.ImportField;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.ingest.DelimitedRecordIterable;
import com.facebook.presto.ingest.ImportSchemaUtil;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.RecordProjections;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.LegacyStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.PlanFragment;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.finalAggregation;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.partialAggregation;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.longSumAggregation;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.Threads.threadsNamed;

public class StaticQueryManager
        implements QueryManager
{
    private final int pageBufferMax;
    private final ExecutorService masterExecutor;
    private final ExecutorService slaveExecutor;
    private final ImportClient importClient;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final LegacyStorageManager storageManager;
    private final DataStreamProvider dataStreamProvider;
    private final SplitManager splitManager;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestJsonCodec;

    @GuardedBy("this")
    private int nextQueryId;

    // todo this is a potential memory leak if the query objects are not removed
    @GuardedBy("this")
    private final Map<String, QueryState> queries = new HashMap<>();

    @Inject
    public StaticQueryManager(
            ImportClient importClient,
            ImportManager importManager,
            Metadata metadata,
            LegacyStorageManager storageManager,
            DataStreamProvider dataStreamProvider,
            SplitManager splitManager,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestJsonCodec)
    {
        this(20, importClient, importManager, metadata, storageManager, dataStreamProvider, splitManager, queryFragmentRequestJsonCodec);
    }

    public StaticQueryManager(
            int pageBufferMax,
            ImportClient importClient,
            ImportManager importManager,
            Metadata metadata,
            LegacyStorageManager storageManager,
            DataStreamProvider dataStreamProvider,
            SplitManager splitManager,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestJsonCodec)
    {
        Preconditions.checkArgument(pageBufferMax > 0, "blockBufferMax must be at least 1");
        this.pageBufferMax = pageBufferMax;
        masterExecutor = Executors.newFixedThreadPool(10, threadsNamed("master-query-processor-%d"));
        slaveExecutor = Executors.newFixedThreadPool(50, threadsNamed("slave-query-processor-%d"));
        this.importClient = importClient;
        this.importManager = importManager;
        this.metadata = metadata;
        this.storageManager = storageManager;
        this.dataStreamProvider = dataStreamProvider;
        this.splitManager = splitManager;
        this.queryFragmentRequestJsonCodec = queryFragmentRequestJsonCodec;
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

            case "sum-frag":
                queryTask = new SumFragmentMaster(queryState, masterExecutor, metadata, splitManager, queryFragmentRequestJsonCodec);
                break;

            default:
                throw new IllegalArgumentException("Unsupported query " + query);
        }

        queries.put(queryId, queryState);
        masterExecutor.submit(queryTask);

        return new QueryInfo(queryId, queryTask.getTupleInfos());
    }

    @Override
    public synchronized QueryInfo createQueryFragment(Split split, PlanFragment planFragment)
    {
        String queryId = String.valueOf(nextQueryId++);
        QueryState queryState = new QueryState(1, pageBufferMax);


        QueryTask queryTask;
        switch (planFragment.getQuery()) {
            case "sum-frag-worker":
                queryTask = new SumFragmentWorker(queryState, split, planFragment.getColumnHandles(), dataStreamProvider);
                break;
            default:
                throw new IllegalArgumentException("Unsupported fragment query: " + planFragment.getQuery());
        }

        queries.put(queryId, queryState);
        slaveExecutor.submit(queryTask);

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

    private static class SumFragmentMaster
            implements QueryTask
    {
        private final QueryState queryState;
        private JsonCodec<QueryFragmentRequest> codec;
        private final AsyncHttpClient asyncHttpClient;
        private final Metadata metadata;
        private final SplitManager splitManager;

        public SumFragmentMaster(QueryState queryState, ExecutorService executor, Metadata metadata, SplitManager splitManager, JsonCodec<QueryFragmentRequest> codec)
        {
            this.queryState = queryState;
            this.codec = codec;
            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(5, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(5, TimeUnit.MINUTES)));
            asyncHttpClient = new AsyncHttpClient(httpClient, executor);
            this.metadata = metadata;
            this.splitManager = splitManager;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return ImmutableList.of(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64));
        }

        @Override
        public void run()
        {
            try {
                final TableMetadata table = metadata.getTable("hive", "default", "hivedba_query_stats");
                Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(table.getTableHandle().get());

                QueryDriversOperator operator = new QueryDriversOperator(10,
                        Iterables.transform(splitAssignments, new Function<SplitAssignments, QueryDriverProvider>()
                        {
                            @Override
                            public QueryDriverProvider apply(SplitAssignments splits)
                            {
                                QueryFragmentRequest queryFragmentRequest = new QueryFragmentRequest(
                                        splits.getSplit(),
                                        new PlanFragment("sum-frag-worker",
                                                ImmutableList.of(
                                                        table.getColumns().get(2).getColumnHandle().get(),
                                                        table.getColumns().get(6).getColumnHandle().get()
                                                )
                                        )
                                );
                                return new HttpQueryProvider(
                                        JsonBodyGenerator.jsonBodyGenerator(codec, queryFragmentRequest),
                                        Optional.of(MediaType.APPLICATION_JSON),
                                        asyncHttpClient,
                                        splits.getNodes().get(0).getHttpUri().resolve("/v1/presto/query"),
                                        ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG)
                                );

                            }
                        })
                );

                HashAggregationOperator aggregation = new HashAggregationOperator(operator,
                        0,
                        ImmutableList.of(finalAggregation(longSumAggregation(1, 0))),
                        ImmutableList.of(concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0))));

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

    private static class SumFragmentWorker
            implements QueryTask
    {
        private final QueryState queryState;
        private final Split split;
        private final List<ColumnHandle> columnHandles;
        private final DataStreamProvider dataStreamProvider;

        private SumFragmentWorker(QueryState queryState, Split split, List<ColumnHandle> columnHandles, DataStreamProvider dataStreamProvider)
        {
            this.queryState = queryState;
            this.split = split;
            this.columnHandles = ImmutableList.copyOf(columnHandles);
            this.dataStreamProvider = dataStreamProvider;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG);
        }

        @Override
        public void run()
        {
            try {
                Operator dataStream = dataStreamProvider.createDataStream(split, columnHandles);
                HashAggregationOperator sumOperator = new HashAggregationOperator(
                        dataStream,
                        0,
                        ImmutableList.of(partialAggregation(longSumAggregation(1, 0))),
                        ImmutableList.of(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0)));

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
