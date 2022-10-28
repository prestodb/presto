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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.presto.Session;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryManagerStats;
import com.facebook.presto.execution.QueryTracker;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.warnings.WarningCollectorFactory;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.resourcemanager.ClusterStatusSender;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.SessionSupplier;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AuthorizedIdentity;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.AnalyzerOptions;
import com.facebook.presto.sql.analyzer.AnalyzerProvider;
import com.facebook.presto.sql.analyzer.PreparedQuery;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.presto.SystemSessionProperties.getAnalyzerType;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static com.facebook.presto.util.AnalyzerUtil.createAnalyzerOptions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DispatchManager
{
    private final QueryIdGenerator queryIdGenerator;
    private final AnalyzerProvider analyzerProvider;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final WarningCollectorFactory warningCollectorFactory;
    private final DispatchQueryFactory dispatchQueryFactory;
    private final FailedDispatchQueryFactory failedDispatchQueryFactory;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionSupplier sessionSupplier;
    private final SessionPropertyDefaults sessionPropertyDefaults;

    private final int maxQueryLength;

    private final Executor queryExecutor;
    private final BoundedExecutor boundedQueryExecutor;

    private final ClusterStatusSender clusterStatusSender;

    private final QueryTracker<DispatchQuery> queryTracker;

    private final QueryManagerStats stats = new QueryManagerStats();

    private final SecurityConfig securityConfig;

    @Inject
    public DispatchManager(
            QueryIdGenerator queryIdGenerator,
            AnalyzerProvider analyzerProvider,
            @SuppressWarnings("rawtypes") ResourceGroupManager resourceGroupManager,
            WarningCollectorFactory warningCollectorFactory,
            DispatchQueryFactory dispatchQueryFactory,
            FailedDispatchQueryFactory failedDispatchQueryFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionSupplier sessionSupplier,
            SessionPropertyDefaults sessionPropertyDefaults,
            QueryManagerConfig queryManagerConfig,
            DispatchExecutor dispatchExecutor,
            ClusterStatusSender clusterStatusSender,
            SecurityConfig securityConfig)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.analyzerProvider = requireNonNull(analyzerProvider, "analyzerClient is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.warningCollectorFactory = requireNonNull(warningCollectorFactory, "warningCollectorFactory is null");
        this.dispatchQueryFactory = requireNonNull(dispatchQueryFactory, "dispatchQueryFactory is null");
        this.failedDispatchQueryFactory = requireNonNull(failedDispatchQueryFactory, "failedDispatchQueryFactory is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");

        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();

        this.queryExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getExecutor();
        this.boundedQueryExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getBoundedExecutor();

        this.clusterStatusSender = requireNonNull(clusterStatusSender, "clusterStatusSender is null");

        this.queryTracker = new QueryTracker<>(queryManagerConfig, dispatchExecutor.getScheduledExecutor());

        this.securityConfig = requireNonNull(securityConfig, "securityConfig is null");
    }

    @PostConstruct
    public void start()
    {
        queryTracker.start();
    }

    @PreDestroy
    public void stop()
    {
        queryTracker.stop();
    }

    @Managed
    @Flatten
    public QueryManagerStats getStats()
    {
        return stats;
    }

    public QueryId createQueryId()
    {
        return queryIdGenerator.createNextQueryId();
    }

    public ListenableFuture<?> createQuery(QueryId queryId, String slug, int retryCount, SessionContext sessionContext, String query)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionFactory is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");
        checkArgument(!queryTracker.tryGetQuery(queryId).isPresent(), "query %s already exists", queryId);

        DispatchQueryCreationFuture queryCreationFuture = new DispatchQueryCreationFuture();
        boundedQueryExecutor.execute(() -> {
            try {
                createQueryInternal(queryId, slug, retryCount, sessionContext, query, resourceGroupManager);
            }
            finally {
                queryCreationFuture.set(null);
            }
        });
        return queryCreationFuture;
    }

    /**
     * Creates and registers a dispatch query with the query tracker.  This method will never fail to register a query with the query
     * tracker.  If an error occurs while, creating a dispatch query a failed dispatch will be created and registered.
     */
    private <C> void createQueryInternal(QueryId queryId, String slug, int retryCount, SessionContext sessionContext, String query, ResourceGroupManager<C> resourceGroupManager)
    {
        Session session = null;
        PreparedQuery preparedQuery;
        try {
            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new PrestoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            // check permissions if needed
            checkPermissions(queryId, sessionContext);

            // get authorized identity if possible
            Optional<AuthorizedIdentity> authorizedIdentity = getAuthorizedIdentity(queryId, sessionContext);

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext, warningCollectorFactory, authorizedIdentity);

            // prepare query
            AnalyzerOptions analyzerOptions = createAnalyzerOptions(session, session.getWarningCollector());
            preparedQuery = analyzerProvider.getQueryPreparer(getAnalyzerType(session)).prepareQuery(analyzerOptions, query, session.getPreparedStatements(), session.getWarningCollector());
            query = preparedQuery.getFormattedQuery().orElse(query);

            // select resource group
            Optional<QueryType> queryType = preparedQuery.getQueryType();
            SelectionContext<C> selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    Optional.ofNullable(sessionContext.getSource()),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType.map(Enum::name),
                    Optional.ofNullable(sessionContext.getClientInfo())));

            // apply system default session properties (does not override user set properties)
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType.map(Enum::name), Optional.of(selectionContext.getResourceGroupId()));

            // mark existing transaction as active
            transactionManager.activateTransaction(session, preparedQuery.isTransactionControlStatement(), accessControl);

            DispatchQuery dispatchQuery = dispatchQueryFactory.createDispatchQuery(
                    session,
                    query,
                    preparedQuery,
                    slug,
                    retryCount,
                    selectionContext.getResourceGroupId(),
                    queryType,
                    session.getWarningCollector(),
                    (dq) -> resourceGroupManager.submit(dq, selectionContext, queryExecutor));

            boolean queryAdded = queryCreated(dispatchQuery);
            if (queryAdded && !dispatchQuery.isDone()) {
                try {
                    clusterStatusSender.registerQuery(dispatchQuery);
                    dispatchQuery.startWaitingForPrerequisites();
                }
                catch (Throwable e) {
                    // dispatch query has already been registered, so just fail it directly
                    dispatchQuery.fail(e);
                }
            }
        }
        catch (Throwable throwable) {
            // creation must never fail, so register a failed query in this case
            if (session == null) {
                session = Session.builder(new SessionPropertyManager())
                        .setQueryId(queryId)
                        .setIdentity(sessionContext.getIdentity())
                        .setSource(sessionContext.getSource())
                        .build();
            }
            DispatchQuery failedDispatchQuery = failedDispatchQueryFactory.createFailedDispatchQuery(session, query, Optional.empty(), throwable);
            queryCreated(failedDispatchQuery);
        }
    }

    /**
     * When selectAuthorizedIdentity API is not enabled, we check the delegation permission
     */
    private void checkPermissions(QueryId queryId, SessionContext sessionContext)
    {
        Identity identity = sessionContext.getIdentity();
        if (!securityConfig.isAuthorizedIdentitySelectionEnabled()) {
            accessControl.checkCanSetUser(
                    identity,
                    new AccessControlContext(
                            queryId,
                            Optional.ofNullable(sessionContext.getClientInfo()),
                            Optional.ofNullable(sessionContext.getSource())),
                    identity.getPrincipal(),
                    identity.getUser());
        }
    }

    /**
     * When selectAuthorizedIdentity API is enabled,
     * 1. Check the delegation permission, which is inside the API call
     * 2. Select and return the authorized identity
     */
    private Optional<AuthorizedIdentity> getAuthorizedIdentity(QueryId queryId, SessionContext sessionContext)
    {
        if (securityConfig.isAuthorizedIdentitySelectionEnabled()) {
            Identity identity = sessionContext.getIdentity();
            AuthorizedIdentity authorizedIdentity = accessControl.selectAuthorizedIdentity(
                    identity,
                    new AccessControlContext(
                            queryId,
                            Optional.ofNullable(sessionContext.getClientInfo()),
                            Optional.ofNullable(sessionContext.getSource())),
                    identity.getUser(),
                    sessionContext.getCertificates());
            return Optional.of(authorizedIdentity);
        }
        return Optional.empty();
    }

    private boolean queryCreated(DispatchQuery dispatchQuery)
    {
        boolean queryAdded = queryTracker.addQuery(dispatchQuery);

        // only add state tracking if this query instance will actually be used for the execution
        if (queryAdded) {
            dispatchQuery.addStateChangeListener(newState -> {
                if (newState.isDone()) {
                    // execution MUST be added to the expiration queue or there will be a leak
                    queryTracker.expireQuery(dispatchQuery.getQueryId());
                }
            });
            stats.trackQueryStats(dispatchQuery);
        }

        return queryAdded;
    }

    public ListenableFuture<?> waitForDispatched(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchedFuture();
                })
                .orElseGet(() -> immediateFuture(null));
    }

    public List<BasicQueryInfo> getQueries()
    {
        return queryTracker.getAllQueries().stream()
                .map(DispatchQuery::getBasicQueryInfo)
                .collect(toImmutableList());
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    public Optional<DispatchInfo> getDispatchInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchInfo();
                });
    }

    public boolean isQueryPresent(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).isPresent();
    }

    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> query.fail(cause));
    }

    public void cancelQuery(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(DispatchQuery::cancel);
    }

    private static class DispatchQueryCreationFuture
            extends AbstractFuture<QueryInfo>
    {
        @Override
        protected boolean set(QueryInfo value)
        {
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // query submission can not be canceled
            return false;
        }
    }
}
