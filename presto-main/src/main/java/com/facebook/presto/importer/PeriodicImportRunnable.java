package com.facebook.presto.importer;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.importer.JobStateFactory.JobState;
import com.facebook.presto.metadata.QualifiedTableName;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;
import static java.lang.String.format;

public class PeriodicImportRunnable
        extends AbstractPeriodicImportRunnable
{
    private static final Logger log = Logger.get(PeriodicImportRunnable.class);

    private final HttpServerInfo serverInfo;
    private final AsyncHttpClient httpClient;
    private final JsonCodec<QueryResults> queryResultsCodec;

    PeriodicImportRunnable(
            PeriodicImportManager periodicImportManager,
            JobState jobState,
            HttpServerInfo serverInfo,
            AsyncHttpClient httpClient,
            JsonCodec<QueryResults> queryResultsCodec)
    {
        super(jobState, periodicImportManager);

        this.serverInfo = serverInfo;
        this.httpClient = httpClient;
        this.queryResultsCodec = queryResultsCodec;
    }

    @Override
    public void doRun()
            throws Exception
    {
        final PersistentPeriodicImportJob job = jobState.getJob();

        QualifiedTableName dstTable = job.getDstTable();
        String sql = String.format("REFRESH MATERIALIZED VIEW %s", dstTable.getTableName());

        ClientSession session = new ClientSession(serverUri(), "periodic-import", dstTable.getCatalogName(), dstTable.getSchemaName(), false);
        StatementClient client = new StatementClient(httpClient, queryResultsCodec, session, sql);

        // don't delete this line, it is what actually pulls the data from the query...
        List<List<Object>> result = ImmutableList.copyOf(flatten(new ResultsPageIterator(client)));

        log.debug("Query: %s, Result: %s", sql, result);
    }

    public static final class PeriodicImportRunnableFactory
    {
        private final PeriodicImportManager periodicImportManager;

        private final HttpServerInfo serverInfo;
        private final AsyncHttpClient httpClient;
        private final JsonCodec<QueryResults> queryResultsCodec;

        @Inject
        public PeriodicImportRunnableFactory(
                PeriodicImportManager periodicImportManager,
                HttpServerInfo serverInfo,
                @ForPeriodicImport AsyncHttpClient httpClient,
                JsonCodec<QueryResults> queryResultsCodec)
        {
            this.serverInfo = checkNotNull(serverInfo, "serverInfo is null");
            this.httpClient = checkNotNull(httpClient, "httpClient is null");
            this.queryResultsCodec = checkNotNull(queryResultsCodec, "queryResultsCodec is null");
            this.periodicImportManager = checkNotNull(periodicImportManager, "periodicImportManager is null");
        }

        public Runnable create(JobState jobState)
        {
            return new PeriodicImportRunnable(periodicImportManager,
                    jobState,
                    serverInfo,
                    httpClient,
                    queryResultsCodec);
        }
    }

    private URI serverUri()
    {
        checkState(serverInfo.getHttpUri() != null, "No HTTP URI for this server (HTTP disabled?)");
        return serverInfo.getHttpUri();
    }

    private static <T> Iterator<T> flatten(Iterator<Iterable<T>> iterator)
    {
        return concat(transform(iterator, new Function<Iterable<T>, Iterator<T>>()
        {
            @Override
            public Iterator<T> apply(Iterable<T> input)
            {
                return input.iterator();
            }
        }));
    }

    private static class ResultsPageIterator
            extends AbstractIterator<Iterable<List<Object>>>
    {
        private final StatementClient client;

        private ResultsPageIterator(StatementClient client)
        {
            this.client = checkNotNull(client, "client is null");
        }

        @Override
        protected Iterable<List<Object>> computeNext()
        {
            while (client.isValid()) {
                Iterable<List<Object>> data = client.current().getData();
                client.advance();
                if (data != null) {
                    return data;
                }
            }

            if (client.isFailed()) {
                throw new IllegalStateException(failureMessage(client.finalResults()));
            }

            return endOfData();
        }
    }

    private static String failureMessage(QueryResults results)
    {
        return format("Query failed (#%s): %s", results.getId(), results.getError().getMessage());
    }

}
