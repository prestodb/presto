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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import jakarta.annotation.Nullable;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.common.Utils.checkState;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class RowExpressionOptimizer
        implements ExpressionOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final CatalogSchemaName defaultNamespace;

    public RowExpressionOptimizer(Metadata metadata)
    {
        this(requireNonNull(metadata, "metadata is null").getFunctionAndTypeManager());
    }

    public RowExpressionOptimizer(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.defaultNamespace = functionAndTypeManager.getDefaultNamespace();
    }

    @Override
    public RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session)
    {
        if (level.ordinal() <= OPTIMIZED.ordinal()) {
            return getRowExpression(rowExpression, level, session, null);
        }
        throw new IllegalArgumentException("Not supported optimization level: " + level);
    }

    @Override
    public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        return getRowExpression(expression, level, session, variableResolver);
    }

    private RowExpression getRowExpression(RowExpression expression, Level level, ConnectorSession session, @Nullable Function<VariableReferenceExpression, Object> variableResolver)
    {
        BuiltInNamespaceRewriter visitor = new BuiltInNamespaceRewriter();
        RowExpressionInterpreter interpreter = new RowExpressionInterpreter(
                visitor.convertToInterpreterNamespace(expression),
                functionAndTypeManager,
                session,
                level);
        return visitor.restoreOriginalNamespaces(toRowExpression(
                expression.getSourceLocation(),
                interpreter.optimize(variableResolver != null ? variableResolver::apply : null),
                expression.getType()));
    }

    /**
     * TODO: GIANT HACK
     * This class is a hack and should eventually be removed.  It is used to ensure consistent constant folding behavior when the built-in
     * function namespace has been switched (for example, to native.default. in the case of native functions).  This will no longer be needed
     * when the native sidecar is capable of providing its own expression optimizer.
     */
    private class BuiltInNamespaceRewriter
    {
        private final Map<FunctionHandle, FunctionHandle> defaultToOriginalFunctionHandles = new IdentityHashMap<>();

        public RowExpression convertToInterpreterNamespace(RowExpression expression)
        {
            if (defaultNamespace.equals(JAVA_BUILTIN_NAMESPACE)) {
                // No need to replace built-in namespaces if the default namespace is already the Java built-in namespace
                return expression;
            }
            return expression.accept(new ReplaceBuiltInNamespaces(), null);
        }

        public RowExpression restoreOriginalNamespaces(RowExpression expression)
        {
            if (defaultToOriginalFunctionHandles.isEmpty()) {
                return expression;
            }
            return expression.accept(new ReplaceOriginalNamespaces(), null);
        }

        private class ReplaceBuiltInNamespaces
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
                FunctionHandle functionHandle = call.getFunctionHandle();
                FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(functionHandle);
                if (!functionMetadata.getImplementationType().canBeEvaluatedInCoordinator()) {
                    checkState(!functionHandle.getCatalogSchemaName().equals(JAVA_BUILTIN_NAMESPACE),
                            format("FunctionHandle %s is already in the Java built-in namespace (%s), yet is marked as ineligible to be evaluated in the coordinator", functionHandle, functionHandle.getCatalogSchemaName()));

                    // Replace the namespace with the Java built-in namespace
                    FunctionHandle javaNamespaceFunctionHandle;
                    try {
                        javaNamespaceFunctionHandle = functionAndTypeManager.lookupFunction(
                                QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, call.getDisplayName()),
                                functionHandle.getArgumentTypes().stream().map(TypeSignatureProvider::new).collect(toImmutableList()));
                    }
                    catch (PrestoException e) {
                        if (e.getErrorCode().equals(FUNCTION_NOT_FOUND.toErrorCode())) {
                            return call; // If the function is not found in the Java built-in namespace, return the original call
                        }
                        throw e; // Rethrow other exceptions
                    }

                    checkState(functionAndTypeManager.getFunctionMetadata(javaNamespaceFunctionHandle).getImplementationType().canBeEvaluatedInCoordinator(),
                            format("FunctionHandle %s in the Java built-in namespace (%s) is not eligible to be evaluated in the coordinator", javaNamespaceFunctionHandle, JAVA_BUILTIN_NAMESPACE));

                    defaultToOriginalFunctionHandles.put(javaNamespaceFunctionHandle, functionHandle);
                    return new CallExpression(
                            call.getSourceLocation(),
                            call.getDisplayName(),
                            javaNamespaceFunctionHandle,
                            call.getType(),
                            call.getArguments());
                }
                return call;
            }
        }

        private class ReplaceOriginalNamespaces
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
                if (defaultToOriginalFunctionHandles.containsKey(call.getFunctionHandle())) {
                    FunctionHandle originalFunctionHandle = defaultToOriginalFunctionHandles.get(call.getFunctionHandle());
                    return new CallExpression(
                            call.getSourceLocation(),
                            call.getDisplayName(),
                            originalFunctionHandle,
                            call.getType(),
                            call.getArguments());
                }
                return call;
            }
        }
    }
}
