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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.rewrite.StatementRewrite;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS;
import static java.util.Objects.requireNonNull;

public class Analyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;
    private final boolean experimentalSyntaxEnabled;
    private final List<Expression> parameters;

    public Analyzer(Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Optional<QueryExplainer> queryExplainer,
            boolean experimentalSyntaxEnabled,
            List<Expression> parameters)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.queryExplainer = requireNonNull(queryExplainer, "query explainer is null");
        this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
        this.parameters = parameters;
    }

    public Analysis analyze(Statement statement)
    {
        Statement rewrittenStatement = StatementRewrite.rewrite(session, metadata, sqlParser, queryExplainer, statement, parameters);
        Analysis analysis = new Analysis(rewrittenStatement, parameters);
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, experimentalSyntaxEnabled);
        analyzer.process(rewrittenStatement, Scope.builder().markQueryBoundary().build());
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
