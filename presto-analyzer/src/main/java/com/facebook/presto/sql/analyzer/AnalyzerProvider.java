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

import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.UNSUPPORTED_ANALYZER_TYPE;
import static java.util.Objects.requireNonNull;

/**
 * This class provides various interfaces for various functionalities in the analyzer.
 * This class can be used to get a specific analyzer implementation for a given analyzer type.
 */
public class AnalyzerProvider
{
    private final Map<AnalyzerType, QueryPreparer> queryPreparersByType;

    @Inject
    public AnalyzerProvider(Map<AnalyzerType, QueryPreparer> queryPreparersByType)
    {
        this.queryPreparersByType = requireNonNull(queryPreparersByType, "queryPreparersByType is null");
    }

    public QueryPreparer getQueryPreparer(AnalyzerType analyzerType)
    {
        requireNonNull(analyzerType, "AnalyzerType is null");
        if (queryPreparersByType.containsKey(analyzerType)) {
            return queryPreparersByType.get(analyzerType);
        }

        throw new PrestoException(UNSUPPORTED_ANALYZER_TYPE, "Unsupported analyzer type: " + analyzerType);
    }
}
