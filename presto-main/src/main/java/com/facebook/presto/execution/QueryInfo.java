/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.Session;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Immutable
public class QueryInfo
{
    public static QueryInfo createQueryInfo(String queryId,
            Session session,
            URI self,
            String query)
    {
        return new QueryInfo(queryId,
                session,
                QueryState.QUEUED,
                self,
                ImmutableList.<String>of(),
                query,
                new QueryStats(),
                null,
                ImmutableList.<FailureInfo>of());
    }

    private final String queryId;
    private final Session session;
    private final QueryState state;
    private final URI self;
    private final List<String> fieldNames;
    private final String query;
    private final QueryStats queryStats;
    private final StageInfo outputStage;
    private final List<FailureInfo> failures;

    @JsonCreator
    public QueryInfo(@JsonProperty("queryId") String queryId,
            @JsonProperty("session") Session session,
            @JsonProperty("state") QueryState state,
            @JsonProperty("self") URI self,
            @JsonProperty("fieldNames") List<String> fieldNames,
            @JsonProperty("query") String query,
            @JsonProperty("queryStats") QueryStats queryStats,
            @JsonProperty("outputStage") StageInfo outputStage,
            @JsonProperty("failures") List<FailureInfo> failures)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(self, "self is null");
        Preconditions.checkNotNull(fieldNames, "fieldNames is null");
        Preconditions.checkNotNull(queryStats, "queryStats is null");
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkNotNull(failures, "failures is null");

        this.queryId = queryId;
        this.session = session;
        this.state = state;
        this.self = self;
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.query = query;
        this.queryStats = queryStats;
        this.outputStage = outputStage;
        this.failures = failures;
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public Session getSession()
    {
        return session;
    }

    @JsonProperty
    public QueryState getState()
    {
        return state;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public QueryStats getQueryStats()
    {
        return queryStats;
    }

    @JsonProperty
    public StageInfo getOutputStage()
    {
        return outputStage;
    }

    @JsonProperty
    public List<FailureInfo> getFailures()
    {
        return failures;
    }

    @JsonIgnore
    public QueryInfo queryState(QueryState queryState)
    {
        return new QueryInfo(this.queryId,
                this.session,
                queryState,
                this.self,
                this.fieldNames,
                this.query,
                this.queryStats,
                this.outputStage,
                this.failures);
    }

    @JsonIgnore
    public QueryInfo stageInfo(StageInfo stageInfo)
    {
        return new QueryInfo(this.queryId,
                this.session,
                this.state,
                this.self,
                this.fieldNames,
                this.query,
                this.queryStats,
                stageInfo,
                this.failures);
    }

    @JsonIgnore
    public QueryInfo fieldNames(List<String> fieldNames)
    {
        return new QueryInfo(this.queryId,
                this.session,
                this.state,
                this.self,
                ImmutableList.copyOf(fieldNames),
                this.query,
                this.queryStats,
                this.outputStage,
                this.failures);
    }

    @JsonIgnore
    public QueryInfo addFailure(FailureInfo failureInfo)
    {
        ImmutableList.Builder<FailureInfo> builder = ImmutableList.builder();
        builder.addAll(this.failures);
        builder.add(failureInfo);

        return new QueryInfo(this.queryId,
                this.session,
                this.state,
                this.self,
                this.fieldNames,
                this.query,
                this.queryStats,
                this.outputStage,
                builder.build());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("queryId", queryId)
                .add("state", state)
                .add("fieldNames", fieldNames)
                .toString();
    }

    public boolean resultsPending()
    {
        if (outputStage != null) {
            List<TaskInfo> outStage = outputStage.getTasks();
            for (TaskInfo outputTask : outStage) {
                for (BufferInfo outputBuffer : outputTask.getOutputBuffers().getBuffers()) {
                    if (outputBuffer.getBufferedPages() > 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    //
    // lame static helpers
    //

    public static final void updateFieldNames(AtomicReference<QueryInfo> queryInfoHolder, List<String> fieldNames)
    {
        while (true) {
            QueryInfo currentQueryInfo = queryInfoHolder.get();
            QueryInfo newQueryInfo = currentQueryInfo.fieldNames(fieldNames);

            if (queryInfoHolder.compareAndSet(currentQueryInfo, newQueryInfo)) {
                return;
            }
        }
    }

    public static final void addFailure(AtomicReference<QueryInfo> queryInfoHolder, Throwable t)
    {
        while (true) {
            QueryInfo currentQueryInfo = queryInfoHolder.get();
            QueryInfo newQueryInfo = currentQueryInfo.addFailure(FailureInfo.toFailure(t));

            if (queryInfoHolder.compareAndSet(currentQueryInfo, newQueryInfo)) {
                return;
            }
        }
    }

    public static final QueryStats getQueryStats(AtomicReference<QueryInfo> queryInfoHolder)
    {
        return queryInfoHolder.get().getQueryStats();
    }

    public static final Session getSession(AtomicReference<QueryInfo> queryInfoHolder)
    {
        return queryInfoHolder.get().getSession();
    }

    public static final String getQueryId(AtomicReference<QueryInfo> queryInfoHolder)
    {
        return queryInfoHolder.get().getQueryId();
    }
}
