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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.execution.PrestoSparkAccessControlCheckerExecution;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PrestoSparkAccessControlChecker
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final QueryExplainer queryExplainer;

    @Inject
    public PrestoSparkAccessControlChecker(
            SqlParser sqlParser,
            QueryExplainer queryExplainer,
            Metadata metadata,
            AccessControl accessControl)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    public IPrestoSparkQueryExecution createExecution(Session session, BuiltInQueryPreparer.BuiltInPreparedQuery preparedQuery, QueryStateTimer queryStateTimer, WarningCollector warningCollector)
    {
        return new PrestoSparkAccessControlCheckerExecution(session, metadata, sqlParser, accessControl, queryExplainer, preparedQuery, queryStateTimer, warningCollector);
    }
}
