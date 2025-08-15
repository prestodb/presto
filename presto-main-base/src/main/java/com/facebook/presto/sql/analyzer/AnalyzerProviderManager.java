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
import com.facebook.presto.spi.analyzer.AnalyzerProvider;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.UNSUPPORTED_ANALYZER_TYPE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AnalyzerProviderManager
{
    private final Map<String, AnalyzerProvider> analyzerProviders = new HashMap<>();

    @Inject
    public AnalyzerProviderManager(BuiltInAnalyzerProvider analyzerProvider)
    {
        // Initializing builtin analyzer by default.
        addAnalyzerProvider(analyzerProvider);
    }

    public void addAnalyzerProvider(AnalyzerProvider analyzerProvider)
    {
        requireNonNull(analyzerProvider, "analyzerProvider is null");

        if (analyzerProviders.putIfAbsent(analyzerProvider.getType(), analyzerProvider) != null) {
            throw new IllegalArgumentException(format("Analyzer provider '%s' is already registered", analyzerProvider.getType()));
        }
    }

    public AnalyzerProvider getAnalyzerProvider(String analyzerType)
    {
        if (analyzerProviders.containsKey(analyzerType)) {
            return analyzerProviders.get(analyzerType);
        }

        throw new PrestoException(UNSUPPORTED_ANALYZER_TYPE, "Unsupported analyzer type: " + analyzerType);
    }
}
