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
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import java.util.Optional;
import java.util.function.Consumer;

public interface DispatchQueryFactory
{
    DispatchQuery createDispatchQuery(
            Session session,
            String query,
            PreparedQuery preparedQuery,
            String slug,
            int retryCount,
            ResourceGroupId resourceGroup,
            Optional<QueryType> queryType,
            WarningCollector warningCollector,
            Consumer<DispatchQuery> queryQueuer);
}
