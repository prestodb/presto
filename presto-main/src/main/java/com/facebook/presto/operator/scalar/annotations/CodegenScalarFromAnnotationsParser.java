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

package com.facebook.presto.operator.scalar.annotations;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CodegenScalarFunction;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.operator.annotations.FunctionsParserHelper.findPublicStaticMethods;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class CodegenScalarFromAnnotationsParser
{
    private CodegenScalarFromAnnotationsParser() {}

    public static List<SqlScalarFunction> parseFunctionDefinitions(Class<?> clazz)
    {
        return findScalarsInFunctionDefinitionClass(clazz).stream()
                .map(method -> createSqlScalarFunction(method))
                .collect(toImmutableList());
    }

    private static List<Method> findScalarsInFunctionDefinitionClass(Class<?> clazz)
    {
        Set<Method> methods = findPublicStaticMethods(
                clazz,
                ImmutableSet.of(CodegenScalarFunction.class),
                ImmutableSet.of(ScalarFunction.class, ScalarOperator.class, SqlInvokedScalarFunction.class));
        for (Method method : methods) {
            checkCondition(
                    method.isAnnotationPresent(SqlType.class),
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Function-defining method [%s] is missing @SqlType",
                    method);
            checkCondition(method.getReturnType().equals(MethodHandle.class), FUNCTION_IMPLEMENTATION_ERROR, "Function-defining method [%s] must return MethodHandle", method);
        }

        return ImmutableList.copyOf(methods);
    }

    private static List<ArgumentProperty> getArgumentProperties(Method method)
    {
        return Arrays.stream(method.getParameters())
                .map(p -> {
                    checkCondition(p.getType() == Type.class, FUNCTION_IMPLEMENTATION_ERROR, "Codegen scalar function %s must have paramter [%s] of type Type", method, p.getName());
                    checkCondition(p.getAnnotationsByType(BlockPosition.class).length == 0, FUNCTION_IMPLEMENTATION_ERROR, "Block and Position format is not supported for codegen function %s", method);
                    checkCondition(p.getAnnotationsByType(IsNull.class).length == 0, FUNCTION_IMPLEMENTATION_ERROR, "Null flag format is not supported for codegen function %s", method);
                    return valueTypeArgumentProperty(p.getAnnotation(SqlNullable.class) == null ? RETURN_NULL_ON_NULL : USE_BOXED_TYPE);
                })
                .collect(toImmutableList());
    }

    private static SqlScalarFunction createSqlScalarFunction(Method method)
    {
        CodegenScalarFunction codegenScalarFunction = method.getAnnotation(CodegenScalarFunction.class);

        Signature signature = new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, codegenScalarFunction.value()),
                FunctionKind.SCALAR,
                Arrays.stream(method.getAnnotationsByType(TypeParameter.class)).map(t -> withVariadicBound(t.value(), t.boundedBy().isEmpty() ? null : t.boundedBy())).collect(toImmutableList()),
                ImmutableList.of(),
                parseTypeSignature(method.getAnnotation(SqlType.class).value()),
                Arrays.stream(method.getParameters()).map(p -> parseTypeSignature(p.getAnnotation(SqlType.class).value())).collect(toImmutableList()),
                false);

        return new SqlScalarFunction(signature)
        {
            @Override
            public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
            {
                Signature boundSignature = applyBoundVariables(signature, boundVariables, arity);
                MethodHandle handle;
                try {
                    handle = (MethodHandle) method.invoke(null, boundSignature.getArgumentTypes().stream().map(t -> functionAndTypeManager.getType(t)).toArray());
                }
                catch (Exception e) {
                    throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, format("Method %s does not return valid MethodHandle", method), e);
                }
                return new BuiltInScalarFunctionImplementation(
                        method.getAnnotation(SqlNullable.class) != null,
                        getArgumentProperties(method),
                        handle,
                        Optional.empty());
            }

            @Override
            public SqlFunctionVisibility getVisibility()
            {
                return codegenScalarFunction.visibility();
            }

            @Override
            public boolean isDeterministic()
            {
                return codegenScalarFunction.deterministic();
            }

            @Override
            public String getDescription()
            {
                return "";
            }

            @Override
            public boolean isCalledOnNullInput()
            {
                return codegenScalarFunction.calledOnNullInput();
            }
        };
    }
}
