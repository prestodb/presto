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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.JsonPath;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.type.JsonPath2016Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.StandardTypes.JSON_2016;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;

public class JsonValueFunction
        extends SqlScalarFunction
{
    public static final JsonValueFunction JSON_VALUE_FUNCTION = new JsonValueFunction();
    public static final String JSON_VALUE_FUNCTION_NAME = "$json_value";
    private static final MethodHandle METHOD_HANDLE = methodHandle(
            JsonValueFunction.class,
            "jsonValue",
            FunctionAndTypeManager.class,
            Type.class,
            Type.class,
            ConnectorSession.class,
            JsonNode.class,
            JsonPath.class, // TODO replace JsonPath with IrJsonPath
            Object.class,
            long.class,
            Object.class,
            long.class,
            Object.class);

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
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type parametersRowType = boundVariables.getTypeVariable("T");
        Type returnType = boundVariables.getTypeVariable("R");
        MethodHandle methodHandle = METHOD_HANDLE
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
    public static Object jsonValue(
            FunctionAndTypeManager functionAndTypeManager,
            Type parametersRowType,
            Type returnType,
            ConnectorSession session,
            JsonNode inputExpression,
            JsonPath jsonPath,
            Object parametersRow,
            long emptyBehavior,
            Object emptyDefault,
            long errorBehavior,
            Object errorDefault)
    {
        throw new UnsupportedOperationException("JSON_VALUE function is not yet supported");
    }
}
