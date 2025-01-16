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
package com.facebook.presto.execution;

import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.google.inject.Inject;

import static com.facebook.presto.execution.AccessControlCheckerExecution.AccessControlCheckerExecutionFactory;
import static com.facebook.presto.execution.DDLDefinitionExecution.DDLDefinitionExecutionFactory;
import static com.facebook.presto.execution.SessionDefinitionExecution.SessionDefinitionExecutionFactory;
import static com.facebook.presto.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import static com.google.common.base.Preconditions.checkState;

public class ExecutionFactoriesManager
{
    private final SqlQueryExecutionFactory sqlQueryExecutionFactory;
    private final DDLDefinitionExecutionFactory ddlDefinitionExecutionFactory;
    private final SessionDefinitionExecutionFactory sessionDefinitionExecutionFactory;
    private final AccessControlCheckerExecutionFactory accessControlCheckerExecutionFactory;

    @Inject
    public ExecutionFactoriesManager(
            SqlQueryExecutionFactory sqlQueryExecutionFactory,
            DDLDefinitionExecutionFactory ddlDefinitionExecutionFactory,
            SessionDefinitionExecutionFactory sessionDefinitionExecutionFactory,
            AccessControlCheckerExecutionFactory accessControlCheckerExecutionFactory)
    {
        this.sqlQueryExecutionFactory = sqlQueryExecutionFactory;
        this.ddlDefinitionExecutionFactory = ddlDefinitionExecutionFactory;
        this.sessionDefinitionExecutionFactory = sessionDefinitionExecutionFactory;
        this.accessControlCheckerExecutionFactory = accessControlCheckerExecutionFactory;
    }

    public QueryExecutionFactory<?> getExecutionFactory(PreparedQuery preparedQuery)
    {
        checkState(preparedQuery != null && preparedQuery.getQueryType().isPresent(), "preparedQuery is null or preparedQuery does not have queryType");
        QueryType queryType = preparedQuery.getQueryType().get();

        if (queryType == QueryType.DATA_DEFINITION) {
            return ddlDefinitionExecutionFactory;
        }

        if (queryType == QueryType.CONTROL) {
            return sessionDefinitionExecutionFactory;
        }

        if (preparedQuery.isExplainTypeValidate()) {
            return accessControlCheckerExecutionFactory;
        }

        return sqlQueryExecutionFactory;
    }
}
