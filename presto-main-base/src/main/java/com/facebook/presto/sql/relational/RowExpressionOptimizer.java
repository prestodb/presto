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
package com.facebook.presto.sql.relational;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;

import java.util.function.Function;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public final class RowExpressionOptimizer
        implements ExpressionOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final CatalogSchemaName defaultNamespace;
    private final Function<RowExpression, RowExpression> normalizeRowExpression;

    public RowExpressionOptimizer(Metadata metadata)
    {
        this(requireNonNull(metadata, "metadata is null").getFunctionAndTypeManager());
    }

    public RowExpressionOptimizer(FunctionAndTypeManager functionAndTypeManager)
    {
        this.defaultNamespace = requireNonNull(functionAndTypeManager, "functionMetadataManager is null").getDefaultNamespace();
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionMetadataManager is null");
        if (!defaultNamespace.equals(JAVA_BUILTIN_NAMESPACE)) {
            this.normalizeRowExpression = rowExpression -> rowExpression.accept(new BuiltInFunctionNamespaceOverride(), null);
        }
        else {
            this.normalizeRowExpression = identity();
        }
    }

    @Override
    public RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session)
    {
        if (level.ordinal() <= OPTIMIZED.ordinal()) {
            RowExpressionInterpreter rowExpressionInterpreter = new RowExpressionInterpreter(
                    normalizeRowExpression.apply(rowExpression),
                    functionAndTypeManager,
                    session,
                    level);
            return toRowExpression(rowExpression.getSourceLocation(), rowExpressionInterpreter.optimize(), rowExpression.getType());
        }
        throw new IllegalArgumentException("Not supported optimization level: " + level);
    }

    @Override
    public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        RowExpressionInterpreter interpreter = new RowExpressionInterpreter(
                normalizeRowExpression.apply(expression),
                functionAndTypeManager,
                session,
                level);
        return toRowExpression(expression.getSourceLocation(), interpreter.optimize(variableResolver::apply), expression.getType());
    }

    /**
     * TODO: GIANT HACK
     * This class is a hack and should eventually be removed.  It is used to ensure consistent constant folding behavior when the built-in
     * function namespace has been switched (for example, to native.default. in the case of native functions).  This will no longer be needed
     * when the native sidecar is capable of providing its own expression optimizer.
     */
    private class BuiltInFunctionNamespaceOverride
            implements RowExpressionVisitor<RowExpression, Void>
    {
        @Override
        public RowExpression visitExpression(RowExpression expression, Void context)
        {
            return expression;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            if (call.getFunctionHandle().getCatalogSchemaName().equals(defaultNamespace)) {
                call = new CallExpression(
                        call.getSourceLocation(),
                        call.getDisplayName(),
                        functionAndTypeManager.lookupFunction(
                                QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, call.getDisplayName()),
                                call.getArguments().stream()
                                        .map(RowExpression::getType)
                                        .map(x -> new TypeSignatureProvider(x.getTypeSignature()))
                                        .collect(toImmutableList())),
                        call.getType(),
                        call.getArguments());
            }
            return call;
        }
    }
}
