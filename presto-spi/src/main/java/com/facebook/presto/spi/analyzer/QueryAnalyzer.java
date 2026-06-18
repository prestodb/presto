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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.spi.plan.PlanNode;

/**
 * The QueryAnalyzer interface should be implemented by respective analyzer to provide various analyzer related functionalities.
 */
public interface QueryAnalyzer
{
    /**
     * Analyzes a prepared query in the given analyzer context.
     *
     * @param analyzerContext analyzer context which stores various information used by analyzer
     * @param preparedQuery prepared query which needs to be analyzed
     * @return Query analysis object which stores various semantic analysis of the query
     */
    QueryAnalysis analyze(AnalyzerContext analyzerContext, PreparedQuery preparedQuery);

    /**
     * Create logical plan for a given query analysis.
     *
     * @param analyzerContext analyzerContext analyzer context which stores various information used by analyzer
     * @param queryAnalysis query analysis for which logical plan needs to be created
     * @return root logical plan node created from the given query analysis
     */
    PlanNode plan(AnalyzerContext analyzerContext, QueryAnalysis queryAnalysis);
}
