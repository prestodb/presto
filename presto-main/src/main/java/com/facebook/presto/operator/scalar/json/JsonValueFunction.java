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
import com.facebook.presto.json.ir.SqlJsonLiteralConverter;
import com.facebook.presto.json.ir.TypedValue;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.planner.JsonPathEvaluator;
import com.facebook.presto.sql.planner.JsonPathEvaluator.PathEvaluationError;
import com.facebook.presto.sql.tree.JsonValue;
import com.facebook.presto.type.JsonPath2016Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.JSON_2016;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.json.JsonInputErrorNode.JSON_ERROR;
import static com.facebook.presto.json.ir.SqlJsonLiteralConverter.getTypedValue;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.operator.scalar.json.ParameterUtil.getParametersArray;
import static com.facebook.presto.spi.StandardErrorCode.JSON_VALUE_RESULT_ERROR;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;

public class JsonValueFunction
        extends SqlScalarFunction
{
    public static final JsonValueFunction JSON_VALUE_FUNCTION = new JsonValueFunction();
    public static final String JSON_VALUE_FUNCTION_NAME = "$json_value";
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(JsonValueFunction.class, "jsonValueLong", FunctionAndTypeManager.class, Type.class, Type.class, SqlFunctionProperties.class, JsonNode.class, IrJsonPath.class, Object.class, long.class, Long.class, long.class, Long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(JsonValueFunction.class, "jsonValueDouble", FunctionAndTypeManager.class, Type.class, Type.class, SqlFunctionProperties.class, JsonNode.class, IrJsonPath.class, Object.class, long.class, Double.class, long.class, Double.class);
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(JsonValueFunction.class, "jsonValueBoolean", FunctionAndTypeManager.class, Type.class, Type.class, SqlFunctionProperties.class, JsonNode.class, IrJsonPath.class, Object.class, long.class, Boolean.class, long.class, Boolean.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(JsonValueFunction.class, "jsonValueSlice", FunctionAndTypeManager.class, Type.class, Type.class, SqlFunctionProperties.class, JsonNode.class, IrJsonPath.class, Object.class, long.class, Slice.class, long.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonValueFunction.class, "jsonValue", FunctionAndTypeManager.class, Type.class, Type.class, SqlFunctionProperties.class, JsonNode.class, IrJsonPath.class, Object.class, long.class, Object.class, long.class, Object.class);
    private static final PrestoException INPUT_ARGUMENT_ERROR = new JsonInputConversionError("malformed input argument to JSON_VALUE function");
    private static final PrestoException PATH_PARAMETER_ERROR = new JsonInputConversionError("malformed JSON path parameter to JSON_VALUE function");
    private static final PrestoException NO_ITEMS = new JsonValueResultError("JSON path found no items");
    private static final PrestoException MULTIPLE_ITEMS = new JsonValueResultError("JSON path found multiple items");
    private static final PrestoException INCONVERTIBLE_ITEM = new JsonValueResultError("JSON path found an item that cannot be converted to an SQL value");

    public JsonValueFunction()
    {
        super(new Signature(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, JSON_VALUE_FUNCTION_NAME),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("R"), typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("R"),
                ImmutableList.of(parseTypeSignature(JSON_2016),
                        parseTypeSignature(JsonPath2016Type.NAME),
                        parseTypeSignature("T"),
                        parseTypeSignature(TINYINT),
                        parseTypeSignature("R"),
                        parseTypeSignature(TINYINT),
                        parseTypeSignature("R")),
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
        return "Extracts an SQL scalar from a JSON value.";
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
        Type returnType = boundVariables.getTypeVariable("R");
        MethodHandle handle;
        if (returnType.getJavaType().equals(long.class)) {
            handle = METHOD_HANDLE_LONG;
        }
        else if (returnType.getJavaType().equals(double.class)) {
            handle = METHOD_HANDLE_DOUBLE;
        }
        else if (returnType.getJavaType().equals(boolean.class)) {
            handle = METHOD_HANDLE_BOOLEAN;
        }
        else if (returnType.getJavaType().equals(Slice.class)) {
            handle = METHOD_HANDLE_SLICE;
        }
        else {
            handle = METHOD_HANDLE;
        }
        MethodHandle methodHandle = handle
                .bindTo(functionAndTypeManager)
                .bindTo(parametersRowType)
                .bindTo(returnType);

        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(USE_BOXED_TYPE)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Long jsonValueLong(
            FunctionAndTypeManager functionAndTypeManager,
            Type parametersRowType,
            Type returnType,
            SqlFunctionProperties properties,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Object parametersRow,
            long emptyBehavior,
            Long emptyDefault,
            long errorBehavior,
            Long errorDefault)
    {
        return (Long) jsonValue(functionAndTypeManager, parametersRowType, returnType, properties, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Double jsonValueDouble(
            FunctionAndTypeManager functionAndTypeManager,
            Type parametersRowType,
            Type returnType,
            SqlFunctionProperties properties,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Object parametersRow,
            long emptyBehavior,
            Double emptyDefault,
            long errorBehavior,
            Double errorDefault)
    {
        return (Double) jsonValue(functionAndTypeManager, parametersRowType, returnType, properties, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Boolean jsonValueBoolean(
            FunctionAndTypeManager functionAndTypeManager,
            Type parametersRowType,
            Type returnType,
            SqlFunctionProperties properties,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Object parametersRow,
            long emptyBehavior,
            Boolean emptyDefault,
            long errorBehavior,
            Boolean errorDefault)
    {
        return (Boolean) jsonValue(functionAndTypeManager, parametersRowType, returnType, properties, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Slice jsonValueSlice(
            FunctionAndTypeManager functionAndTypeManager,
            Type parametersRowType,
            Type returnType,
            SqlFunctionProperties properties,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Object parametersRow,
            long emptyBehavior,
            Slice emptyDefault,
            long errorBehavior,
            Slice errorDefault)
    {
        return (Slice) jsonValue(functionAndTypeManager, parametersRowType, returnType, properties, inputExpression, jsonPath, parametersRow, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @UsedByGeneratedCode
    public static Object jsonValue(
            FunctionAndTypeManager functionAndTypeManager,
            Type parametersRowType,
            Type returnType,
            SqlFunctionProperties properties,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Object parametersRow,
            long emptyBehavior,
            Object emptyDefault,
            long errorBehavior,
            Object errorDefault)
    {
        if (inputExpression == null || jsonPath == null) {
            return null;
        }

        if (inputExpression.equals(JSON_ERROR)) {
            return handleSpecialCase(errorBehavior, errorDefault, INPUT_ARGUMENT_ERROR); // ERROR ON ERROR was already handled by the input function
        }
        Object[] parameters = getParametersArray(parametersRowType, parametersRow);
        for (Object parameter : parameters) {
            if (parameter.equals(JSON_ERROR)) {
                return handleSpecialCase(errorBehavior, errorDefault, PATH_PARAMETER_ERROR); // ERROR ON ERROR was already handled by the input function
            }
        }
        JsonPathEvaluator pathEvaluator = new JsonPathEvaluator(inputExpression, parameters, functionAndTypeManager, properties);
        List<Object> pathResult;
        try {
            pathResult = pathEvaluator.evaluate(jsonPath);
        }
        catch (PathEvaluationError e) {
            return handleSpecialCase(errorBehavior, errorDefault, e); // TODO by spec, we should cast the defaults only if they are used
        }

        if (pathResult.isEmpty()) {
            return handleSpecialCase(emptyBehavior, emptyDefault, NO_ITEMS);
        }

        if (pathResult.size() > 1) {
            return handleSpecialCase(errorBehavior, errorDefault, MULTIPLE_ITEMS);
        }

        Object item = getOnlyElement(pathResult);
        TypedValue typedValue;
        if (item instanceof JsonNode) {
            if (item.equals(NullNode.instance)) {
                return null;
            }
            Optional<TypedValue> itemValue;
            try {
                itemValue = getTypedValue((JsonNode) item);
            }
            catch (SqlJsonLiteralConverter.JsonLiteralConversionError e) {
                return handleSpecialCase(errorBehavior, errorDefault, new JsonValueResultError("JSON path found an item that cannot be converted to an SQL value", e));
            }
            if (!itemValue.isPresent()) {
                return handleSpecialCase(errorBehavior, errorDefault, INCONVERTIBLE_ITEM);
            }
            typedValue = itemValue.get();
        }
        else {
            typedValue = (TypedValue) item;
        }
        if (returnType.equals(typedValue.getType())) {
            return typedValue.getValueAsObject();
        }
        FunctionHandle coercion;
        try {
            coercion = functionAndTypeManager.lookupCast(CastType.CAST, typedValue.getType(), returnType);
        }
        catch (OperatorNotFoundException e) {
            return handleSpecialCase(errorBehavior, errorDefault, new JsonValueResultError(format(
                    "Cannot cast value of type %s to declared return type of function JSON_VALUE: %s",
                    typedValue.getType(),
                    returnType)));
        }
        try {
            return new InterpretedFunctionInvoker(functionAndTypeManager).invoke(coercion, properties, ImmutableList.of(typedValue.getValueAsObject()));
        }
        catch (RuntimeException e) {
            return handleSpecialCase(errorBehavior, errorDefault, new JsonValueResultError(format(
                    "Cannot cast value of type %s to declared return type of function JSON_VALUE: %s",
                    typedValue.getType(),
                    returnType)));
        }
    }

    private static Object handleSpecialCase(long behavior, Object defaultValue, PrestoException error)
    {
        switch (JsonValue.EmptyOrErrorBehavior.values()[(int) behavior]) {
            case NULL:
                return null;
            case ERROR:
                throw error;
            case DEFAULT:
                return defaultValue;
        }
        throw new IllegalStateException("unexpected behavior");
    }

    public static class JsonValueResultError
            extends PrestoException
    {
        public JsonValueResultError(String message)
        {
            super(JSON_VALUE_RESULT_ERROR, "cannot extract SQL scalar from JSON: " + message);
        }

        public JsonValueResultError(String message, Throwable cause)
        {
            super(JSON_VALUE_RESULT_ERROR, "cannot extract SQL scalar from JSON: " + message, cause);
        }
    }
}
