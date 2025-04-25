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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.rewrite.StatementRewrite;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SystemSessionProperties.isCheckAccessControlOnUtilizedColumnsOnly;
import static com.facebook.presto.SystemSessionProperties.isCheckAccessControlWithSubfields;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractExternalFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.UtilizedColumnsAnalyzer.analyzeForUtilizedColumns;
import static com.facebook.presto.util.AnalyzerUtil.checkAccessPermissions;
import static java.util.Objects.requireNonNull;

public class Analyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;
    private final List<Expression> parameters;
    private final Map<NodeRef<Parameter>, Expression> parameterLookup;
    private final WarningCollector warningCollector;
    private final MetadataExtractor metadataExtractor;
    private final String query;

    public Analyzer(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Optional<QueryExplainer> queryExplainer,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector,
            String query)
    {
        this(session, metadata, sqlParser, accessControl, queryExplainer, parameters, parameterLookup, warningCollector, Optional.empty(), query);
    }

    public Analyzer(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Optional<QueryExplainer> queryExplainer,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector,
            Optional<ExecutorService> metadataExtractorExecutor,
            String query)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.queryExplainer = requireNonNull(queryExplainer, "query explainer is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.parameterLookup = requireNonNull(parameterLookup, "parameterLookup is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        requireNonNull(metadataExtractorExecutor, "metadataExtractorExecutor is null");
        this.metadataExtractor = new MetadataExtractor(session, metadata, metadataExtractorExecutor, sqlParser, warningCollector);
        this.query = requireNonNull(query, "query is null");
    }

    public Analysis analyze(Statement statement)
    {
        return analyze(statement, false);
    }

    // TODO: Remove this method once all calls are moved to analyzer interface, as this call is overloaded with analyze and columnCheckPermissions
    public Analysis analyze(Statement statement, boolean isDescribe)
    {
        Analysis analysis = analyzeSemantic(statement, isDescribe);
        checkAccessPermissions(analysis.getAccessControlReferences(), query);
        return analysis;
    }

    public Analysis analyzeSemantic(Statement statement, boolean isDescribe)
    {
        Statement rewrittenStatement = StatementRewrite.rewrite(session, metadata, sqlParser, queryExplainer, statement, parameters, parameterLookup, accessControl, warningCollector, query);
        Analysis analysis = new Analysis(rewrittenStatement, parameterLookup, isDescribe);

        metadataExtractor.populateMetadataHandle(session, rewrittenStatement, analysis.getMetadataHandle());
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector);
        analyzer.analyze(rewrittenStatement, Optional.empty());
        analyzeForUtilizedColumns(analysis, analysis.getStatement(), warningCollector);
        analysis.populateTableColumnAndSubfieldReferencesForAccessControl(isCheckAccessControlOnUtilizedColumnsOnly(session), isCheckAccessControlWithSubfields(session));
        return analysis;
    }

    static void verifyNoAggregateWindowOrGroupingFunctions(
            Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles,
            FunctionAndTypeResolver functionAndTypeResolver,
            Expression predicate,
            String clause)
    {
        List<FunctionCall> aggregates = extractAggregateFunctions(functionHandles, ImmutableList.of(predicate), functionAndTypeResolver);

        List<FunctionCall> windowExpressions = extractWindowFunctions(ImmutableList.of(predicate));

        List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

        List<Expression> found = ImmutableList.copyOf(Iterables.concat(
                aggregates,
                windowExpressions,
                groupingOperations));

        if (!found.isEmpty()) {
            throw new SemanticException(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, predicate, "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found);
        }
    }

    static void verifyNoExternalFunctions(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, FunctionAndTypeResolver functionAndTypeResolver, Expression predicate, String clause)
    {
        List<FunctionCall> externalFunctions = extractExternalFunctions(functionHandles, ImmutableList.of(predicate), functionAndTypeResolver);
        if (!externalFunctions.isEmpty()) {
            throw new SemanticException(NOT_SUPPORTED, predicate, "External functions in %s is not supported: %s", clause, externalFunctions);
        }
    }
}
