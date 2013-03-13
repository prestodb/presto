package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.QueryInfo.QueryInfoFactory;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.FailureInfo.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class SimpleSqlExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SimpleSqlExecution.class);

    protected final String queryId;
    protected final URI selfLocation;
    protected final QueryMonitor queryMonitor;
    protected final QueryInfoFactory queryInfoFactory;

    protected final QueryStats queryStats = new QueryStats();

    protected final AtomicReference<QueryState> queryState = new AtomicReference<>(QueryState.QUEUED);

    protected final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    protected SimpleSqlExecution(String queryId,
            URI selfLocation,
            QueryMonitor queryMonitor,
            QueryInfoFactory queryInfoFactory)
    {
        this.queryId = checkNotNull(queryId, "queryId is null");
        this.selfLocation = checkNotNull(selfLocation, "selfLocation is null");
        this.queryInfoFactory = checkNotNull(queryInfoFactory, "queryInfoFactory is null");
        this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return queryInfoFactory.buildQueryInfo(queryState.get(),
                selfLocation,
                ImmutableList.<String>of(),
                queryStats,
                null,
                toFailures(failureCauses));
    }

    @Override
    public final void start()
    {
        queryStats.recordExecutionStart();
        checkState(queryState.compareAndSet(QueryState.QUEUED, QueryState.STARTING), "Execution has already been started");
        try {
            doStart();
        }
        catch (Exception e) {
            fail(e);
        }
    }

    protected abstract void doStart()
            throws Exception;

    @Override
    public void updateState()
    {
    }

    @Override
    public void cancel()
    {
        // transition to canceled state, only if not already finished
        if (!queryState.get().isDone()) {
            queryStats.recordEnd();
            queryState.set(QueryState.CANCELED);
            queryMonitor.completionEvent(getQueryInfo());
            log.debug("Cancelling query %s", queryId);
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        // transition to canceled state, only if not already finished
        if (!queryState.get().isDone()) {
            failureCauses.add(cause);
            queryStats.recordEnd();
            queryState.set(QueryState.FAILED);
            queryMonitor.completionEvent(getQueryInfo());
            log.debug("Failing query %s", queryId);
        }
    }

    public static abstract class SimpleSqlExecutionFactory<T extends SimpleSqlExecution>
    {
        protected final QueryMonitor queryMonitor;

        protected SimpleSqlExecutionFactory(QueryMonitor queryMonitor)
        {
            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
        }

        public abstract T createQueryExecution(String queryId,
                Statement statement,
                Session session,
                URI selfLocation,
                QueryInfoFactory queryInfoFactory);

    }
}
