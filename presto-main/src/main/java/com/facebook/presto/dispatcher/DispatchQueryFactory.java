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

import com.facebook.presto.Session;
import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AnalyzerProvider;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * The interface for Dispatch query factory
 */
public interface DispatchQueryFactory
{
    /**
     * This interface API is defined to setting up all preparation works for query before it being executed.
     *
     * @param session the session
     * @param analyzerProvider the analyzer provider
     * @param query the query
     * @param preparedQuery the prepared query
     * @param slug the unique query slug for each {@code Query} object
     * @param retryCount the query retry count
     * @param resourceGroup the resource group to be used
     * @param queryType the query type derived from the {@code PreparedQuery statement}
     * @param warningCollector the warning collector
     * @param queryQueuer the query queuer is invoked when a query is to submit to the {@link com.facebook.presto.execution.resourceGroups.ResourceGroupManager}
     * @return {@link DispatchQuery}
     */
    DispatchQuery createDispatchQuery(
            Session session,
            AnalyzerProvider analyzerProvider,
            String query,
            PreparedQuery preparedQuery,
            String slug,
            int retryCount,
            ResourceGroupId resourceGroup,
            Optional<QueryType> queryType,
            WarningCollector warningCollector,
            Consumer<DispatchQuery> queryQueuer);
}
