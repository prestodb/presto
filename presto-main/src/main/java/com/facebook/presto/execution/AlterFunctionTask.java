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
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class AlterFunctionTask
        implements DataDefinitionTask<AlterFunction>
{
    private final SqlParser sqlParser;

    @Inject
    public AlterFunctionTask(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public String getName()
    {
        return "ALTER FUNCTION";
    }

    @Override
    public String explain(AlterFunction statement, List<Expression> parameters)
    {
        return "ALTER FUNCTION " + statement.getFunctionName();
    }

    @Override
    public ListenableFuture<?> execute(AlterFunction statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Analyzer analyzer = new Analyzer(stateMachine.getSession(), metadata, sqlParser, accessControl, Optional.empty(), parameters, stateMachine.getWarningCollector());
        analyzer.analyze(statement);

        QualifiedObjectName functionName = qualifyObjectName(statement.getFunctionName());
        AlterRoutineCharacteristics alterRoutineCharacteristics = new AlterRoutineCharacteristics(
                statement.getCharacteristics().getNullCallClause()
                        .map(com.facebook.presto.sql.tree.RoutineCharacteristics.NullCallClause::name)
                        .map(NullCallClause::valueOf));
        metadata.getFunctionAndTypeManager().alterFunction(
                functionName,
                statement.getParameterTypes().map(types -> types.stream()
                        .map(TypeSignature::parseTypeSignature)
                        .collect(toImmutableList())),
                alterRoutineCharacteristics);
        return immediateFuture(null);
    }
}
