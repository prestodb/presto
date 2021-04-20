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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Return;
import com.facebook.presto.sql.tree.RoutineBody;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.metadata.SessionFunctionHandle.SESSION_NAMESPACE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
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
        return format("CREATE %sFUNCTION %s", statement.isTemporary() ? "TEMPORARY " : "", statement.getFunctionName());
    }

    @Override
    public ListenableFuture<?> execute(CreateFunction statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Analyzer analyzer = new Analyzer(stateMachine.getSession(), metadata, sqlParser, accessControl, Optional.empty(), parameters, stateMachine.getWarningCollector());
        Analysis analysis = analyzer.analyze(statement);
        if (analysis.getFunctionHandles().values().stream()
                .anyMatch(SqlFunctionHandle.class::isInstance)) {
            throw new PrestoException(NOT_SUPPORTED, "Invoking a dynamically registered function in SQL function body is not supported");
        }

        SqlInvokedFunction function = createSqlInvokedFunction(statement, metadata, analysis);
        if (statement.isTemporary()) {
            stateMachine.addSessionFunction(new SqlFunctionId(function.getSignature().getName(), function.getSignature().getArgumentTypes()), function);
        }
        else {
            metadata.getFunctionAndTypeManager().createFunction(function, statement.isReplace());
        }

        return immediateFuture(null);
    }

    private SqlInvokedFunction createSqlInvokedFunction(CreateFunction statement, Metadata metadata, Analysis analysis)
    {
        QualifiedObjectName functionName = statement.isTemporary() ?
                QualifiedObjectName.valueOf(SESSION_NAMESPACE, statement.getFunctionName().getSuffix()) :
                qualifyObjectName(statement.getFunctionName());
        List<Parameter> parameters = statement.getParameters().stream()
                .map(parameter -> new Parameter(parameter.getName().toString(), parseTypeSignature(parameter.getType())))
                .collect(toImmutableList());
        TypeSignature returnType = parseTypeSignature(statement.getReturnType());
        String description = statement.getComment().orElse("");
        RoutineCharacteristics routineCharacteristics = RoutineCharacteristics.builder()
                .setLanguage(new RoutineCharacteristics.Language(statement.getCharacteristics().getLanguage().getLanguage()))
                .setDeterminism(RoutineCharacteristics.Determinism.valueOf(statement.getCharacteristics().getDeterminism().name()))
                .setNullCallClause(RoutineCharacteristics.NullCallClause.valueOf(statement.getCharacteristics().getNullCallClause().name()))
                .build();
        RoutineBody body = statement.getBody();

        if (statement.getBody() instanceof Return) {
            Expression bodyExpression = ((Return) statement.getBody()).getExpression();
            Type bodyType = analysis.getType(bodyExpression);

            // Coerce expressions in body if necessary
            bodyExpression = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteExpression(Expression expression, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    Expression rewritten = treeRewriter.defaultRewrite(expression, null);

                    Type coercion = analysis.getCoercion(expression);
                    if (coercion != null) {
                        return new Cast(
                                rewritten,
                                coercion.getTypeSignature().toString(),
                                false,
                                analysis.isTypeOnlyCoercion(expression));
                    }
                    return rewritten;
                }
            }, bodyExpression, null);

            if (!bodyType.equals(metadata.getType(returnType))) {
                // Casting is safe here, since we have verified at analysis time that the actual type of the body is coercible to declared return type.
                bodyExpression = new Cast(bodyExpression, statement.getReturnType());
            }

            body = new Return(bodyExpression);
        }

        return new SqlInvokedFunction(
                functionName,
                parameters,
                returnType,
                description,
                routineCharacteristics,
                formatSql(body, Optional.empty()),
                notVersioned());
    }
}
