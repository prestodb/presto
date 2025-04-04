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

import com.facebook.presto.Session;
import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.analyzer.AnalyzerContext;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.QueryAnalysis;
import com.facebook.presto.spi.analyzer.QueryAnalyzer;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.google.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class BuiltInQueryAnalyzer
        implements QueryAnalyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final Optional<QueryExplainer> queryExplainer;
    private final ExecutorService metadataExtractorExecutor;

    @Inject
    public BuiltInQueryAnalyzer(
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Optional<QueryExplainer> queryExplainer,
            @ForMetadataExtractor ExecutorService metadataExtractorExecutor)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.queryExplainer = requireNonNull(queryExplainer, "query explainer is null");
        this.metadataExtractorExecutor = requireNonNull(metadataExtractorExecutor, "metadataExtractorExecutor is null");
    }

    public static BuiltInAnalyzerContext getBuiltInAnalyzerContext(
            MetadataResolver metadataResolver,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Session session)
    {
        return new BuiltInAnalyzerContext(metadataResolver, idAllocator, variableAllocator, session);
    }

    @Override
    public QueryAnalysis analyze(AnalyzerContext analyzerContext, PreparedQuery preparedQuery)
    {
        requireNonNull(preparedQuery, "preparedQuery is null");

        checkState(analyzerContext instanceof BuiltInAnalyzerContext, "analyzerContext should be an instance of BuiltInAnalyzerContext");
        checkState(preparedQuery instanceof BuiltInQueryPreparer.BuiltInPreparedQuery, "Unsupported prepared query type: %s", preparedQuery.getClass().getSimpleName());

        BuiltInQueryPreparer.BuiltInPreparedQuery builtInPreparedQuery = (BuiltInQueryPreparer.BuiltInPreparedQuery) preparedQuery;
        Session session = ((BuiltInAnalyzerContext) analyzerContext).getSession();

        Analyzer analyzer = new Analyzer(
                session,
                metadata,
                sqlParser,
                accessControl,
                queryExplainer,
                builtInPreparedQuery.getParameters(),
                parameterExtractor(builtInPreparedQuery.getStatement(), builtInPreparedQuery.getParameters()),
                session.getWarningCollector(),
                Optional.of(metadataExtractorExecutor));

        Analysis analysis = analyzer.analyzeSemantic(((BuiltInQueryPreparer.BuiltInPreparedQuery) preparedQuery).getStatement(), false);
        return new BuiltInQueryAnalysis(analysis);
    }

    @Override
    public PlanNode plan(AnalyzerContext analyzerContext, QueryAnalysis queryAnalysis)
    {
        checkState(analyzerContext instanceof BuiltInAnalyzerContext, "analyzerContext should be an instance of BuiltInAnalyzerContext");
        LogicalPlanner logicalPlanner = new LogicalPlanner(
                ((BuiltInAnalyzerContext) analyzerContext).getSession(),
                analyzerContext.getIdAllocator(),
                metadata,
                analyzerContext.getVariableAllocator(),
                sqlParser);
        return logicalPlanner.plan(((BuiltInQueryAnalysis) queryAnalysis).getAnalysis());
    }
}
