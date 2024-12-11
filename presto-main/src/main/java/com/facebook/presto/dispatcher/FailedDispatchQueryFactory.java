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
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import java.util.Optional;

/**
 * Factory interface to create FailedDispatchQuery
 *
 * This interface is required for https://github.com/prestodb/presto/issues/23455
 */
public interface FailedDispatchQueryFactory
{
    /**
     * If DispatchManager fails to create a DispatchQuery, a FailedDispatchQuery is created instead
     *
     * @param session session
     * @param query query text
     * @param resourceGroup resource group of the query
     * @param throwable failure information about the query
     * @return {@link FailedDispatchQuery}
     */
    FailedDispatchQuery createFailedDispatchQuery(Session session, String query, Optional<ResourceGroupId> resourceGroup, Throwable throwable);
}
