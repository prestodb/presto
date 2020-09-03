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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.presto.Session;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.memory.ForRemoteDispatch;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import javax.inject.Inject;

import java.util.Optional;

public class RemoteDispatchQueryFactory
        implements DispatchQueryFactory
{
    private HttpClient httpClient;
    private final SessionPropertyManager sessionPropertyManager;
    private final DispatchExecutor dispatchExecutor;
    private final LocationFactory httpLocationFactory;
    private final InternalNodeManager internalNodeManager;
    private final ClusterSizeMonitor clusterSizeMonitor;
    private final QueryInfoFetcherFactory queryInfoFetcherFactory;

    @Inject
    public RemoteDispatchQueryFactory(
            @ForRemoteDispatch HttpClient httpClient,
            SessionPropertyManager sessionPropertyManager,
            DispatchExecutor dispatchExecutor,
            LocationFactory httpLocationFactory,
            InternalNodeManager internalNodeManager,
            ClusterSizeMonitor clusterSizeMonitor,
            QueryInfoFetcherFactory queryInfoFetcherFactory)
    {
        this.httpClient = httpClient;
        this.sessionPropertyManager = sessionPropertyManager;
        this.dispatchExecutor = dispatchExecutor;
        this.httpLocationFactory = httpLocationFactory;
        this.internalNodeManager = internalNodeManager;
        this.clusterSizeMonitor = clusterSizeMonitor;
        this.queryInfoFetcherFactory = queryInfoFetcherFactory;
    }

    @Override
    public DispatchQuery createDispatchQuery(
            Session session,
            String query,
            PreparedQuery preparedQuery,
            String slug,
            ResourceGroupId resourceGroup,
            Optional<QueryType> queryType,
            WarningCollector warningCollector)
    {
        return new RemoteDispatchQuery(
                session,
                query,
                slug,
                httpClient,
                sessionPropertyManager,
                dispatchExecutor.getExecutor(),
                httpLocationFactory,
                internalNodeManager,
                clusterSizeMonitor,
                queryInfoFetcherFactory);
    }
}
