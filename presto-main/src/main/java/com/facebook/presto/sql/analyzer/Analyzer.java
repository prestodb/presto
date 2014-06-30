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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS;
import static com.google.common.base.Preconditions.checkNotNull;

public class Analyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final ConnectorSession session;
    private final Optional<QueryExplainer> queryExplainer;
    private final boolean experimentalSyntaxEnabled;

    public Analyzer(ConnectorSession session, Metadata metadata, SqlParser sqlParser, Optional<QueryExplainer> queryExplainer, boolean experimentalSyntaxEnabled)
    {
        this.session = checkNotNull(session, "session is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
        this.queryExplainer = checkNotNull(queryExplainer, "query explainer is null");
        this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
    }

    public Analysis analyze(Statement statement)
    {
        Analysis analysis = new Analysis();
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, session, experimentalSyntaxEnabled, queryExplainer);
        TupleDescriptor outputDescriptor = analyzer.process(statement, new AnalysisContext());
        analysis.setOutputDescriptor(outputDescriptor);
        return analysis;
    }

    static void verifyNoAggregatesOrWindowFunctions(Metadata metadata, Expression predicate, String clause)
    {
        AggregateExtractor extractor = new AggregateExtractor(metadata);
        extractor.process(predicate, null);

        WindowFunctionExtractor windowExtractor = new WindowFunctionExtractor();
        windowExtractor.process(predicate, null);

        List<FunctionCall> found = ImmutableList.copyOf(Iterables.concat(extractor.getAggregates(), windowExtractor.getWindowFunctions()));

        if (!found.isEmpty()) {
            throw new SemanticException(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, predicate, "%s clause cannot contain aggregations or window functions: %s", clause, found);
        }
    }
}
