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
package com.facebook.presto.spi.prerequisites;

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;

import java.util.concurrent.CompletableFuture;

/**
 * An interface to plugin custom business logic that will be executed before the query is queued.
 */
public interface QueryPrerequisites
{
    /**
     * Given the query context, implementations can perform actions or ensure that all conditions
     * are ready for the query to execute. The returned <code>CompletableFuture</code> will indicate
     * when the query is ready to be queued for execution. If the returned future finishes successfully,
     * it will trigger the query to be queued and its failure will fail the query.
     */
    CompletableFuture<?> waitForPrerequisites(QueryId queryId, QueryPrerequisitesContext context, WarningCollector warningCollector);

    /**
     * Optional method for the implementations to implement if they want to be informed about the finishing
     * of queries (either successfully or unsuccessfully).
     */
    default void queryFinished(QueryId queryId)
    {
    }
}
