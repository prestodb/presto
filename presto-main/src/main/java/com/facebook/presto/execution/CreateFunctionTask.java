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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sqlfunction.SqlInvokedRegularFunction;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CreateFunctionTask
        implements DataDefinitionTask<CreateFunction>
{
    private final SqlParser sqlParser;

    @Inject
    public CreateFunctionTask(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public String getName()
    {
        return "CREATE FUNCTION";
    }

    @Override
    public String explain(CreateFunction statement, List<Expression> parameters)
    {
        return "CREATE FUNCTION " + statement.getFunctionName();
    }

    @Override
    public ListenableFuture<?> execute(CreateFunction statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Analyzer analyzer = new Analyzer(stateMachine.getSession(), metadata, sqlParser, accessControl, Optional.empty(), parameters, stateMachine.getWarningCollector());
        metadata.getFunctionManager().createFunction(statement, statement.isReplace());
    }

    private SqlInvokedRegularFunction createFunctionToSqlInvokedRegularFunction(CreateFunction statement)
    {
        return new SqlInvokedRegularFunction()
    }
}
