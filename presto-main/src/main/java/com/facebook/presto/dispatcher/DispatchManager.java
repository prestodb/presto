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
import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryManagerStats;
import com.facebook.presto.execution.QueryTracker;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.warnings.WarningCollectorFactory;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.resourcemanager.ClusterQueryTrackerService;
import com.facebook.presto.resourcemanager.ClusterStatusSender;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.SessionSupplier;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.analyzer.AnalyzerOptions;
import com.facebook.presto.spi.analyzer.AnalyzerProvider;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AuthorizedIdentity;
import com.facebook.presto.sql.analyzer.AnalyzerProviderManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.presto.SystemSessionProperties.getAnalyzerType;
import static com.facebook.presto.security.AccessControlUtils.checkPermissions;
import static com.facebook.presto.security.AccessControlUtils.getAuthorizedIdentity;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static com.facebook.presto.util.AnalyzerUtil.createAnalyzerOptions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class defines the Query dispatch process handled by Dispatch Manager
 */
public class DispatchManager
{
    private final QueryIdGenerator queryIdGenerator;
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
    private final AnalyzerProviderManager analyzerProviderManager;

    /**
     * Dispatch Manager is used for the pre-queuing part of queries prior to the query execution phase.
     *
     * Dispatch Manager object is instantiated when the presto server is launched by server bootstrap time. It is a critical component in resource management section of the query.
     *
     * @param queryIdGenerator query ID generator for generating a new query ID when a query is created
     * @param analyzerProviderManager provides access to registered analyzer providers
     * @param resourceGroupManager the resource group manager to select corresponding resource group for query to retrieve basic information from session context for selection context
     * @param warningCollectorFactory the warning collector factory to collect presto warning in a query session
     * @param dispatchQueryFactory the dispatch query factory is used to create a {@link DispatchQuery} object.  The dispatch query is submitted to the {@link ResourceGroupManager} which enqueues the query.
     * @param failedDispatchQueryFactory the failed dispatch query factory is used to register a failed query
     * @param transactionManager the transaction manager is used to active existing transaction if this is a transaction control statement
     * @param accessControl the access control is used as part of activate transaction operation
     * @param sessionSupplier the session supplier to create a query session
     * @param sessionPropertyDefaults allow dispatch manager to apply system default session properties
     * @param queryManagerConfig contains all query manager config properties
     * @param dispatchExecutor the dispatch executor contains both pre-queued query executor {@link BoundedExecutor} and post-queued query executor {@link Executor}
     * @param clusterStatusSender An API to register a created query to resource manager for sending heartbeat and start task execution
     */
    @Inject
    public DispatchManager(
            QueryIdGenerator queryIdGenerator,
            AnalyzerProviderManager analyzerProviderManager,
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
            SecurityConfig securityConfig,
            Optional<ClusterQueryTrackerService> clusterQueryTrackerService)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.analyzerProviderManager = requireNonNull(analyzerProviderManager, "analyzerProviderManager is null");
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

        this.queryTracker = new QueryTracker<>(queryManagerConfig, dispatchExecutor.getScheduledExecutor(), clusterQueryTrackerService);

        this.securityConfig = requireNonNull(securityConfig, "securityConfig is null");
    }

    /**
     * Start query tracker as a background task.
     */
    @PostConstruct
    public void start()
    {
        queryTracker.start();
    }

    /**
     * Stop any running queries and cancel background tasks if any.
     */
    @PreDestroy
    public void stop()
    {
        queryTracker.stop();
    }

    /**
     * This method returns the statistics from query manager
     *
     * @return {@link QueryManagerStats}
     */
    @Managed
    @Flatten
    public QueryManagerStats getStats()
    {
        return stats;
    }

    /**
     * Create a query id
     *
     * This method is called when a {@code Query} object is created
     *
     * @return {@link QueryId}
     */
    public QueryId createQueryId()
    {
        return queryIdGenerator.createNextQueryId();
    }

    /**
     * Create a listenable future to start executing a query for a given queryID and slug
     * <br>
     * This method instantiates a dispatch query with the query tracker. The logic flow is as follows:
     * <ol>
     *     <li> Check to see if the query is too big. This is to protect the coordinator not be overwhelmed </li>
     *     <li> Take the raw session information from {@code sessionContext} into a genuine session object of the query that can be used to check for access control,
     *     privacy/security guarded by session properties, check for user query id, etc </li>
     *     <li> {@code prepareQuery} is responsible for calling SQL parsing and generate abstract syntax tree (AST). This wil return a query object with placeholders
     *     for prepared statement to fill in the actual query execution</li>
     *     <li> Select corresponding resource group {@link ResourceGroupManager} for the query which is done in two steps
     *     <ul>
     *         <li> Retrieve basic information from the session context and use this to prepare for selection context.</li>
     *         <li> The selection context will then be used by the {@link ResourceGroupManager} to figure out what resource group to go to
     *         and which resource group the query should belong to. </li>
     *     </ul>
     *     <li> Enhance the session with session property defaults. User may use the plugin feature to provide default session property overrides
     *     for dynamically configurable feature-toggle type of use cases. </li>
     *     <li> Create a {@link DispatchQuery} object. The dispatch query is submitted to the {@link ResourceGroupManager} which enqueues the query. </li>
     *     <li> The event of creating the dispatch query is logged after registering to the query tracker which is used to keep track of the state of the query.
     *     The log is done by adding a state change listener to the query.
     *     The state transition listener is useful to understand the state when a query has moved from created to running, running to error completed. </li>
     *     <li> Once dispatch query object is created and it's registered with the query tracker, start sending heard beat to indicate that this query is now running
     *     to the {@link ResourceGroupManager}. This is no-op for no disaggregated coordinator setup</li>
     *     <li> invoke query prerequisite manager by {@code startWaitingForResources} to start process pre-resource management stage.
     *     <ul>
     *         <li>This is to allow user to add a plugin and custom functionality to the query prior to it getting queued so that user may for instance prepare for something
     *         prior to that query getting queued. By default this is a no-op</li>
     *         <li> proceed to queue the query {@code queueQuery()} which internally change the state machine of the local dispatch query as {@code QUEUED},
     *         and then call query queuer to submit the query to the {@link ResourceGroupManager} </li>
     *     </ul>
     *     </li>
     * </ol>
     *
     * @param queryId the query id
     * @param slug the query slug
     * @param retryCount per-query retry limit due to communication failures
     * @param sessionContext the raw session context
     * @param query the query in String
     * @return the listenable future
     * @see ResourceGroupManager <a href="https://prestodb.io/docs/current/admin/resource-groups.html">Resource Groups</a>
     */
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
     * tracker. If an error occurs while creating a dispatch query, a failed dispatch will be created and registered.
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
            checkPermissions(accessControl, securityConfig, queryId, sessionContext);

            // get authorized identity if possible
            Optional<AuthorizedIdentity> authorizedIdentity = getAuthorizedIdentity(accessControl, securityConfig, queryId, sessionContext);

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext, warningCollectorFactory, authorizedIdentity);

            // prepare query
            AnalyzerOptions analyzerOptions = createAnalyzerOptions(session, session.getWarningCollector());
            AnalyzerProvider analyzerProvider = analyzerProviderManager.getAnalyzerProvider(getAnalyzerType(session));
            preparedQuery = analyzerProvider.getQueryPreparer().prepareQuery(analyzerOptions, query, session.getPreparedStatements(), session.getWarningCollector());
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
                    Optional.ofNullable(sessionContext.getClientInfo()),
                    Optional.ofNullable(sessionContext.getSchema()),
                    sessionContext.getIdentity().getPrincipal().map(Principal::getName)));

            // apply system default session properties (does not override user set properties)
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType.map(Enum::name), Optional.of(selectionContext.getResourceGroupId()));

            // mark existing transaction as active
            transactionManager.activateTransaction(session, preparedQuery.isTransactionControlStatement(), accessControl);

            DispatchQuery dispatchQuery = dispatchQueryFactory.createDispatchQuery(
                    session,
                    analyzerProvider,
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

    /**
     * Wait for dispatched listenable future.
     *
     * @param queryId the query id
     * @return the listenable future
     */
    public ListenableFuture<?> waitForDispatched(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchedFuture();
                })
                .orElseGet(() -> immediateFuture(null));
    }

    /**
     * Return a list of {@link BasicQueryInfo}.
     *
     */
    public List<BasicQueryInfo> getQueries()
    {
        return queryTracker.getAllQueries().stream()
                .map(DispatchQuery::getBasicQueryInfo)
                .collect(toImmutableList());
    }

    /**
     * Return a lightweight query info.
     *
     * @param queryId the query id
     * @return {@link BasicQueryInfo}
     */
    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    /**
     * Return dispatch info
     *
     * @param queryId the query id
     * @return an optional of {@link DispatchInfo}
     */
    public Optional<DispatchInfo> getDispatchInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchInfo();
                });
    }

    /**
     * Check if a given queryId exists in query tracker
     *
     * @param queryId the query id
     */
    public boolean isQueryPresent(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).isPresent();
    }

    /**
     * For a given queryId, trigger immediate query failure if exists in query tracker along with a given reason
     *
     * @param queryId the query id
     * @param cause the cause
     */
    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> query.fail(cause));
    }

    /**
     * For a given queryId, make the query state Cancel and trigger immediate query failure.
     *
     * @param queryId the query id
     */
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
