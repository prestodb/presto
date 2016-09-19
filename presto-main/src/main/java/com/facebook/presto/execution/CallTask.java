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

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.ParameterRewriter;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.transaction.TransactionManager;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.PROCEDURE_CALL_FAILED;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CallTask
        implements DataDefinitionTask<Call>
{
    @Override
    public String getName()
    {
        return "CALL";
    }

    @Override
    public CompletableFuture<?> execute(Call call, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        if (!stateMachine.isAutoCommit()) {
            throw new PrestoException(NOT_SUPPORTED, "Procedures cannot be called within a transaction (use autocommit mode)");
        }

        Session session = stateMachine.getSession();
        QualifiedObjectName procedureName = createQualifiedObjectName(session, call, call.getName());
        ConnectorId connectorId = metadata.getCatalogHandle(stateMachine.getSession(), procedureName.getCatalogName())
                .orElseThrow(() -> new SemanticException(MISSING_CATALOG, call, "Catalog %s does not exist", procedureName.getCatalogName()));
        Procedure procedure = metadata.getProcedureRegistry().resolve(connectorId, procedureName.asSchemaTableName());

        // map declared argument names to positions
        Map<String, Integer> positions = new HashMap<>();
        for (int i = 0; i < procedure.getArguments().size(); i++) {
            positions.put(procedure.getArguments().get(i).getName(), i);
        }

        // per specification, do not allow mixing argument types
        Predicate<CallArgument> hasName = argument -> argument.getName().isPresent();
        boolean anyNamed = call.getArguments().stream().anyMatch(hasName);
        boolean allNamed = call.getArguments().stream().allMatch(hasName);
        if (anyNamed && !allNamed) {
            throw new SemanticException(INVALID_PROCEDURE_ARGUMENTS, call, "Named and positional arguments cannot be mixed");
        }

        // get the argument names in call order
        Map<String, CallArgument> names = new LinkedHashMap<>();
        for (int i = 0; i < call.getArguments().size(); i++) {
            CallArgument argument = call.getArguments().get(i);
            if (argument.getName().isPresent()) {
                String name = argument.getName().get();
                if (names.put(name, argument) != null) {
                    throw new SemanticException(INVALID_PROCEDURE_ARGUMENTS, argument, "Duplicate procedure argument: %s", name);
                }
                if (!positions.containsKey(name)) {
                    throw new SemanticException(INVALID_PROCEDURE_ARGUMENTS, argument, "Unknown argument name: %s", name);
                }
            }
            else if (i < procedure.getArguments().size()) {
                names.put(procedure.getArguments().get(i).getName(), argument);
            }
            else {
                throw new SemanticException(INVALID_PROCEDURE_ARGUMENTS, call, "Too many arguments for procedure");
            }
        }

        // verify argument count
        if (names.size() < positions.size()) {
            throw new SemanticException(INVALID_PROCEDURE_ARGUMENTS, call, "Too few arguments for procedure");
        }

        // get argument values
        Object[] values = new Object[procedure.getArguments().size()];
        for (Entry<String, CallArgument> entry : names.entrySet()) {
            CallArgument callArgument = entry.getValue();
            int index = positions.get(entry.getKey());
            Argument argument = procedure.getArguments().get(index);

            Expression expression = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), callArgument.getValue());
            Type type = argument.getType();

            Object value = evaluateConstantExpression(expression, type, metadata, session, parameters);

            values[index] = toTypeObjectValue(session, type, value);
        }

        // validate arguments
        MethodType methodType = procedure.getMethodHandle().type();
        for (int i = 0; i < procedure.getArguments().size(); i++) {
            if ((values[i] == null) && methodType.parameterType(i).isPrimitive()) {
                String name = procedure.getArguments().get(i).getName();
                throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "Procedure argument cannot be null: " + name);
            }
        }

        // insert session argument
        List<Object> arguments = new ArrayList<>();
        Iterator<Object> valuesIterator = asList(values).iterator();
        for (Class<?> type : methodType.parameterList()) {
            if (ConnectorSession.class.isAssignableFrom(type)) {
                arguments.add(session.toConnectorSession(connectorId));
            }
            else {
                arguments.add(valuesIterator.next());
            }
        }

        try {
            procedure.getMethodHandle().invokeWithArguments(arguments);
        }
        catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(PROCEDURE_CALL_FAILED, t);
        }

        return completedFuture(null);
    }

    private static Object toTypeObjectValue(Session session, Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        writeNativeValue(type, blockBuilder, value);
        return type.getObjectValue(session.toConnectorSession(), blockBuilder, 0);
    }
}
