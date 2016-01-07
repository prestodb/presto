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
package com.facebook.presto.execution;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryPerformanceFetcherProvider
        implements Provider<Optional<QueryPerformanceFetcher>>
{
    private final QueryManager queryManager;

    @Inject
    public QueryPerformanceFetcherProvider(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @Override
    public Optional<QueryPerformanceFetcher> get()
    {
        return Optional.of(queryManager::getQueryInfo);
    }
}
