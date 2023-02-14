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
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.analyzer.QueryAnalysis;
import com.facebook.presto.spi.analyzer.QueryAnalyzer;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.tree.Explain;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isCheckAccessControlOnUtilizedColumnsOnly;
import static com.facebook.presto.SystemSessionProperties.isCheckAccessControlWithSubfields;
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
    private Session session;

    @Inject
    public BuiltInQueryAnalyzer(
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Optional<QueryExplainer> queryExplainer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.queryExplainer = requireNonNull(queryExplainer, "query explainer is null");
    }

    @Override
    public QueryAnalysis analyze(PreparedQuery preparedQuery)
    {
        requireNonNull(preparedQuery, "preparedQuery is null");

        // TODO: BuiltInAnalyzer should not use session once this is moved to presto-analyzer
        checkState(session != null, "session is null");
        checkState(preparedQuery instanceof BuiltInQueryPreparer.BuiltInPreparedQuery, "Unsupported prepared query type: %s", preparedQuery.getClass().getSimpleName());

        BuiltInQueryPreparer.BuiltInPreparedQuery builtInPreparedQuery = (BuiltInQueryPreparer.BuiltInPreparedQuery) preparedQuery;

        Analyzer analyzer = new Analyzer(
                session,
                metadata,
                sqlParser,
                accessControl,
                queryExplainer,
                builtInPreparedQuery.getParameters(),
                parameterExtractor(builtInPreparedQuery.getStatement(), builtInPreparedQuery.getParameters()),
                session.getWarningCollector());

        Analysis analysis = analyzer.analyzeSemantic(((BuiltInQueryPreparer.BuiltInPreparedQuery) preparedQuery).getStatement(), false);
        return new BuiltInQueryAnalysis(analysis);
    }

    @Override
    public PlanNode plan(QueryAnalysis queryAnalysis, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
    {
        Analysis analysis = ((BuiltInQueryAnalysis) queryAnalysis).getAnalysis();
        return new LogicalPlanner(session, idAllocator, metadata, variableAllocator).plan(analysis);
    }

    @Override
    public void checkAccessPermissions(QueryAnalysis queryAnalysis)
    {
        BuiltInQueryAnalysis builtInQueryAnalysis = (BuiltInQueryAnalysis) queryAnalysis;
        builtInQueryAnalysis.getAnalysis().getTableColumnAndSubfieldReferencesForAccessControl(isCheckAccessControlOnUtilizedColumnsOnly(session), isCheckAccessControlWithSubfields(session))
                .forEach((accessControlInfo, tableColumnReferences) ->
                        tableColumnReferences.forEach((tableName, columns) ->
                                accessControlInfo.getAccessControl().checkCanSelectFromColumns(
                                        session.getRequiredTransactionId(),
                                        accessControlInfo.getIdentity(),
                                        session.getAccessControlContext(),
                                        tableName,
                                        columns)));
    }

    @Override
    public boolean isExplainAnalyzeQuery(QueryAnalysis queryAnalysis)
    {
        Analysis analysis = ((BuiltInQueryAnalysis) queryAnalysis).getAnalysis();
        return analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();
    }

    @Override
    public Set<ConnectorId> extractConnectors(QueryAnalysis queryAnalysis)
    {
        Analysis analysis = ((BuiltInQueryAnalysis) queryAnalysis).getAnalysis();
        return extractConnectors(analysis);
    }

    private static Set<ConnectorId> extractConnectors(Analysis analysis)
    {
        ImmutableSet.Builder<ConnectorId> connectors = ImmutableSet.builder();

        for (TableHandle tableHandle : analysis.getTables()) {
            connectors.add(tableHandle.getConnectorId());
        }

        if (analysis.getInsert().isPresent()) {
            TableHandle target = analysis.getInsert().get().getTarget();
            connectors.add(target.getConnectorId());
        }

        return connectors.build();
    }

    // TODO: this should be removed once builtIn analyzer is not dependent on session
    public void setSession(Session session)
    {
        this.session = requireNonNull(session, "session is null");
    }
}
