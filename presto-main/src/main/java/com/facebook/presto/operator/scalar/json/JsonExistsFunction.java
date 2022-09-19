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
package com.facebook.presto.operator.scalar.json;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.json.ir.IrJsonPath;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.planner.JsonPathEvaluator;
import com.facebook.presto.sql.planner.JsonPathEvaluator.PathEvaluationError;
import com.facebook.presto.sql.tree.JsonExists;
import com.facebook.presto.type.JsonPath2016Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.JSON_2016;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.json.JsonInputErrorNode.JSON_ERROR;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.operator.scalar.json.ParameterUtil.getParametersArray;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;

public class JsonExistsFunction
        extends SqlScalarFunction
{
    public static final JsonExistsFunction JSON_EXISTS_FUNCTION = new JsonExistsFunction();
    public static final String JSON_EXISTS_FUNCTION_NAME = "$json_exists";
    private static final MethodHandle METHOD_HANDLE = methodHandle(
            JsonExistsFunction.class,
            "jsonExists",
            FunctionAndTypeManager.class,
            Type.class,
            SqlFunctionProperties.class,
            JsonNode.class,
            IrJsonPath.class,
            Object.class,
            long.class);
    private static final PrestoException INPUT_ARGUMENT_ERROR = new JsonInputConversionError("malformed input argument to JSON_EXISTS function");
    private static final PrestoException PATH_PARAMETER_ERROR = new JsonInputConversionError("malformed JSON path parameter to JSON_EXISTS function");

    public JsonExistsFunction()
    {
        super(new Signature(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, JSON_EXISTS_FUNCTION_NAME),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                BOOLEAN.getTypeSignature(),
                ImmutableList.of(parseTypeSignature(JSON_2016), parseTypeSignature(JsonPath2016Type.NAME), parseTypeSignature("T"), parseTypeSignature(TINYINT)),
                false));
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Determines whether a JSON value satisfies a path specification.";
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type parametersRowType = boundVariables.getTypeVariable("T");
        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(functionAndTypeManager)
                .bindTo(parametersRowType);

        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Boolean jsonExists(
            FunctionAndTypeManager functionAndTypeManager,
            Type parametersRowType,
            SqlFunctionProperties properties,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Object parametersRow,
            long errorBehavior)
    {
        if (inputExpression == null || jsonPath == null) {
            return null;
        }

        if (inputExpression.equals(JSON_ERROR)) {
            return handleError(errorBehavior, INPUT_ARGUMENT_ERROR); // ERROR ON ERROR was already handled by the input function
        }
        Object[] parameters = getParametersArray(parametersRowType, parametersRow);
        for (Object parameter : parameters) {
            if (parameter.equals(JSON_ERROR)) {
                return handleError(errorBehavior, PATH_PARAMETER_ERROR); // ERROR ON ERROR was already handled by the input function
            }
        }
        JsonPathEvaluator pathEvaluator = new JsonPathEvaluator(inputExpression, parameters, functionAndTypeManager, properties);
        List<Object> pathResult;
        try {
            pathResult = pathEvaluator.evaluate(jsonPath);
        }
        catch (PathEvaluationError e) {
            return handleError(errorBehavior, e);
        }

        return !pathResult.isEmpty();
    }

    private static Boolean handleError(long errorBehavior, PrestoException error)
    {
        switch (JsonExists.ErrorBehavior.values()[(int) errorBehavior]) {
            case FALSE:
                return false;
            case TRUE:
                return true;
            case UNKNOWN:
                return null;
            case ERROR:
                throw error;
        }
        throw new IllegalStateException("unexpected error behavior");
    }
}
