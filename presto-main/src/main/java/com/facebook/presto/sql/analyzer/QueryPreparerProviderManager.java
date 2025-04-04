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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.analyzer.QueryPreparerProvider;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.UNSUPPORTED_ANALYZER_TYPE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryPreparerProviderManager
{
    private final Map<String, QueryPreparerProvider> queryPreparerProviders = new HashMap<>();

    @Inject
    public QueryPreparerProviderManager(BuiltInQueryPreparerProvider queryPreparerProvider)
    {
        addQueryPreparerProvider(queryPreparerProvider);
    }

    public void addQueryPreparerProvider(QueryPreparerProvider preparerProvider)
    {
        requireNonNull(preparerProvider, "preparerProvider is null");

        if (queryPreparerProviders.putIfAbsent(preparerProvider.getType(), preparerProvider) != null) {
            throw new IllegalArgumentException(format("Query preparer provider '%s' is already registered", preparerProvider.getType()));
        }
    }

    public QueryPreparerProvider getQueryPreparerProvider(String preparerType)
    {
        if (queryPreparerProviders.containsKey(preparerType)) {
            return queryPreparerProviders.get(preparerType);
        }

        throw new PrestoException(UNSUPPORTED_ANALYZER_TYPE, "Unsupported query preparer type: " + preparerType);
    }
}
