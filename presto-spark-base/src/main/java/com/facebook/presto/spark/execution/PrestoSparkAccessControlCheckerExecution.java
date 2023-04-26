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
package com.facebook.presto.spark.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.facebook.presto.util.AnalyzerUtil.checkAccessPermissions;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class PrestoSparkAccessControlCheckerExecution
        implements IPrestoSparkQueryExecution
{
    private final Session session;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final QueryExplainer queryExplainer;
    private final BuiltInQueryPreparer.BuiltInPreparedQuery preparedQuery;
    private final QueryStateTimer queryStateTimer;
    private final WarningCollector warningCollector;

    public PrestoSparkAccessControlCheckerExecution(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            QueryExplainer queryExplainer,
            BuiltInQueryPreparer.BuiltInPreparedQuery preparedQuery,
            QueryStateTimer queryStateTimer,
            WarningCollector warningCollector)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        this.preparedQuery = requireNonNull(preparedQuery, "preparedQuery is null");
        this.queryStateTimer = requireNonNull(queryStateTimer, "queryStateTimer is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    @Override
    public List<List<Object>> execute()
    {
        Analyzer analyzer = new Analyzer(
                session,
                metadata,
                sqlParser,
                accessControl,
                Optional.of(queryExplainer),
                preparedQuery.getParameters(),
                parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector);

        queryStateTimer.beginSemanticAnalyzing();
        Analysis analysis = analyzer.analyzeSemantic(preparedQuery.getStatement(), false);
        queryStateTimer.beginColumnAccessPermissionChecking();
        checkAccessPermissions(analysis.getAccessControlReferences());
        queryStateTimer.endColumnAccessPermissionChecking();

        List<List<Object>> results = new ArrayList<>();
        results.add(singletonList(true));
        return results;
    }

    public List<? extends Type> getOutputTypes()
    {
        return singletonList(BooleanType.BOOLEAN);
    }
}
