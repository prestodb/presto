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

import com.facebook.presto.spi.analyzer.AnalyzerProvider;
import com.facebook.presto.spi.analyzer.QueryAnalyzer;
import com.facebook.presto.spi.analyzer.QueryPreparer;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class BuiltInAnalyzerProvider
        implements AnalyzerProvider
{
    private static final String PROVIDER_NAME = "BUILTIN";
    private final BuiltInQueryPreparer queryPreparer;
    private final BuiltInQueryAnalyzer queryAnalyzer;

    @Inject
    public BuiltInAnalyzerProvider(BuiltInQueryPreparer queryPreparer, BuiltInQueryAnalyzer queryAnalyzer)
    {
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.queryAnalyzer = requireNonNull(queryAnalyzer, "queryAnalyzer is null");
    }

    @Override
    public String getType()
    {
        return PROVIDER_NAME;
    }

    @Override
    public QueryPreparer getQueryPreparer()
    {
        return queryPreparer;
    }

    @Override
    public QueryAnalyzer getQueryAnalyzer()
    {
        return queryAnalyzer;
    }
}
