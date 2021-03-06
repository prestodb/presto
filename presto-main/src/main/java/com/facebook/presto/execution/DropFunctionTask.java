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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.metadata.SessionFunctionHandle.SESSION_NAMESPACE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class DropFunctionTask
        implements DataDefinitionTask<DropFunction>
{
    private final SqlParser sqlParser;

    @Inject
    public DropFunctionTask(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public String getName()
    {
        return "DROP FUNCTION";
    }

    @Override
    public String explain(DropFunction statement, List<Expression> parameters)
    {
        return format("DROP %sFUNCTION %s", statement.isTemporary() ? "TEMPORARY " : "", statement.getFunctionName());
    }

    @Override
    public ListenableFuture<?> execute(DropFunction statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Analyzer analyzer = new Analyzer(stateMachine.getSession(), metadata, sqlParser, accessControl, Optional.empty(), parameters, stateMachine.getWarningCollector());
        analyzer.analyze(statement);
        Optional<List<TypeSignature>> parameterTypes = statement.getParameterTypes().map(types -> types.stream().map(TypeSignature::parseTypeSignature).collect(toImmutableList()));

        if (statement.isTemporary()) {
            stateMachine.removeSessionFunction(
                    new SqlFunctionId(
                            QualifiedObjectName.valueOf(SESSION_NAMESPACE, statement.getFunctionName().getSuffix()),
                            parameterTypes.orElse(emptyList())),
                    statement.isExists());
        }
        else {
            metadata.getFunctionAndTypeManager().dropFunction(
                    qualifyObjectName(statement.getFunctionName()),
                    parameterTypes,
                    statement.isExists());
        }

        return immediateFuture(null);
    }
}
