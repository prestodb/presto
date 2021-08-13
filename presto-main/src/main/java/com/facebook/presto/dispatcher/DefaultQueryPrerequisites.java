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

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.prerequisites.QueryPrerequisites;
import com.facebook.presto.spi.prerequisites.QueryPrerequisitesContext;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class DefaultQueryPrerequisites
        implements QueryPrerequisites
{
    private static final CompletableFuture<?> COMPLETED_FUTURE = completedFuture(null);

    @Override
    public CompletableFuture<?> waitForPrerequisites(QueryId queryId, QueryPrerequisitesContext context, WarningCollector warningCollector)
    {
        return COMPLETED_FUTURE;
    }
}
